# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Environment setup
# MAGIC
# MAGIC Creates the Unity Catalog objects the pipeline writes to.
# MAGIC
# MAGIC **Run this once.** It is idempotent — safe to re-run at any time.
# MAGIC
# MAGIC Design notes:
# MAGIC * Tables are created with **explicit DDL**, not schema-on-write. A production
# MAGIC   table's schema is a contract; inferring it from whatever arrived today means
# MAGIC   an upstream change silently reshapes the table analysts depend on.
# MAGIC * Money is `DECIMAL`, never `DOUBLE`. Binary floating point cannot represent
# MAGIC   `0.10` exactly, and the error compounds across a `SUM`.
# MAGIC * Column comments are populated because Genie and the Databricks Assistant
# MAGIC   read them. An undocumented Gold table is not self-serve.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail")
CATALOG = dbutils.widgets.get("catalog")
print(f"Target catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog, schemas, volumes
# MAGIC
# MAGIC Two separate volumes on purpose. Auto Loader scans a directory recursively, so
# MAGIC checkpoint files living under the inbox would get picked up as input data on the
# MAGIC next run. Landing zone and pipeline state are kept apart.

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    print("Could not create a catalog. Set the 'catalog' widget to 'workspace' and re-run.")
    raise e

for schema in ["landing", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.inbox")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.checkpoints")

INBOX = f"/Volumes/{CATALOG}/landing/inbox"
dbutils.fs.mkdirs(f"{INBOX}/orders")
dbutils.fs.mkdirs(f"{INBOX}/reference")

print(f"Upload the daily order CSVs to : {INBOX}/orders")
print(f"Upload products.xlsx to        : {INBOX}/reference")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — raw and immutable
# MAGIC
# MAGIC Every column lands as `STRING`. Bronze's job is to preserve exactly what the
# MAGIC source sent, including the malformed rows. If Bronze casts, a bad value becomes
# MAGIC a null and the evidence of what actually arrived is gone.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.orders_raw (
  order_id      STRING,
  order_date    STRING,
  customer_id   STRING,
  product_id    STRING,
  quantity      STRING,
  unit_price    STRING,
  region        STRING,
  _rescued_data STRING COMMENT 'Fields present in the file but not in the declared schema',
  _source_file  STRING,
  _ingested_at  TIMESTAMP
)
COMMENT 'Raw daily order drops, untyped and unfiltered. Append only.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.products_raw (
  product_id   STRING,
  product_name STRING,
  category     STRING,
  list_price   STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP
)
COMMENT 'Raw product reference file.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — typed, conformed, deduplicated
# MAGIC
# MAGIC `NOT NULL` and `CHECK` constraints are declared on the table rather than enforced
# MAGIC in application code. A second pipeline writing to this table later cannot bypass
# MAGIC them — the guarantee belongs to the data, not to my notebook.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.orders (
  order_id           STRING  NOT NULL COMMENT 'Business key. One row per order.',
  order_date         DATE    NOT NULL COMMENT 'Business date, parsed from three source formats.',
  customer_id        STRING           COMMENT 'Nullable: some source rows omit it.',
  product_id         STRING,
  quantity           INT     NOT NULL COMMENT 'Negative = return. Zero = placed but no units.',
  unit_price         DECIMAL(12,2) NOT NULL,
  region             STRING  NOT NULL COMMENT 'Title-cased. Missing becomes Unknown.',
  line_revenue       DECIMAL(14,2) COMMENT 'quantity * unit_price. Signed, so returns net out.',
  is_return          BOOLEAN,
  is_zero_quantity   BOOLEAN,
  has_schema_drift   BOOLEAN COMMENT 'Row carried fields not in the declared schema.',
  _source_file       STRING,
  _source_batch_date DATE    COMMENT 'Drop date parsed from the filename. Ordering key for dedup.',
  _ingested_at       TIMESTAMP
)
COMMENT 'One row per order, latest version wins. Deduplicated via MERGE on order_id.'
""")

for name, check in [
    ("chk_unit_price_non_negative", "unit_price >= 0"),
    ("chk_region_present", "region IS NOT NULL AND region <> ''"),
]:
    try:
        spark.sql(f"ALTER TABLE {CATALOG}.silver.orders ADD CONSTRAINT {name} CHECK ({check})")
    except Exception:
        pass  # already exists

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.orders_quarantine (
  order_id       STRING,
  order_date_raw STRING,
  customer_id    STRING,
  product_id     STRING,
  quantity_raw   STRING,
  unit_price_raw STRING,
  region_raw     STRING,
  reject_reason  STRING COMMENT 'Why this row could not be promoted to Silver.',
  _source_file   STRING,
  _ingested_at   TIMESTAMP,
  _quarantined_at TIMESTAMP
)
COMMENT 'Rows that failed the Silver contract. Nothing is silently dropped.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.products (
  product_id   STRING NOT NULL,
  product_name STRING,
  category     STRING COMMENT 'Title-cased so Kitchen and kitchen do not split.',
  list_price   DECIMAL(12,2),
  _ingested_at TIMESTAMP
)
COMMENT 'Conformed product reference.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The refresh queue
# MAGIC
# MAGIC This is the piece that makes the pipeline correct rather than merely working.
# MAGIC
# MAGIC Day 2's file contains orders dated day 1 — one of them restating a quantity. So
# MAGIC Gold cannot be append-only: yesterday's answer changes when today's file lands.
# MAGIC Silver records which business dates each batch touched, and Gold rebuilds exactly
# MAGIC those. The queue decouples the two stages, so either can be re-run alone.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.gold_refresh_queue (
  order_date DATE NOT NULL COMMENT 'A business date whose Gold aggregate is now stale.',
  queued_at  TIMESTAMP
)
COMMENT 'Work queue: business dates awaiting Gold rebuild.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — the analyst-facing table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.daily_sales_by_category_region (
  order_date   DATE   NOT NULL COMMENT 'Business date the order was placed.',
  category     STRING NOT NULL COMMENT 'Product category. Unknown = product id absent from reference.',
  region       STRING NOT NULL,
  net_revenue  DECIMAL(18,2) COMMENT 'Sum of quantity * unit_price. Net of returns.',
  order_count  BIGINT        COMMENT 'Distinct orders.',
  units_sold   BIGINT        COMMENT 'Sum of quantity. Net of returns.',
  aov          DECIMAL(18,2) COMMENT 'net_revenue / order_count within this cell.',
  _refreshed_at TIMESTAMP
)
CLUSTER BY (order_date, region)
COMMENT 'Daily sales by product category and region. Rebuilt per affected date.'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.pipeline_run_audit (
  run_id           STRING,
  run_ts           TIMESTAMP,
  stage            STRING,
  rows_in          BIGINT,
  rows_loaded      BIGINT,
  rows_quarantined BIGINT,
  net_revenue      DECIMAL(18,2),
  notes            STRING
)
COMMENT 'Per-run reconciliation. rows_in must equal rows_loaded + rows_quarantined.'
""")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.silver"))
print("Setup complete.")
