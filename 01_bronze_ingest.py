# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingest
# MAGIC
# MAGIC Auto Loader picks up new files in the landing volume and appends them to Bronze.
# MAGIC
# MAGIC **Why Auto Loader rather than reading the directory each run:** the brief says to
# MAGIC design for a daily schedule, which makes "which files have I already processed?"
# MAGIC the central question. Auto Loader answers it from its own checkpoint, so I don't
# MAGIC hand-roll a watermark table that drifts out of sync the first time a run fails
# MAGIC halfway. Re-running this notebook processes nothing if nothing new landed.
# MAGIC
# MAGIC `trigger(availableNow=True)` runs the stream until the backlog is drained, then
# MAGIC stops. It is a batch job with streaming's bookkeeping — the right shape for a job
# MAGIC that fires once per file arrival rather than running continuously.

# COMMAND ----------

# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "retail")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F

# Epoch timestamps appear in the source data. from_unixtime resolves against the
# session timezone, so leaving this unset would decode 1704153600 as 2024-01-01
# in any timezone west of UTC — an off-by-one-day bug in the revenue numbers that
# would never surface in testing. Pinned explicitly.
spark.conf.set("spark.sql.session.timeZone", "UTC")

INBOX      = f"/Volumes/{CATALOG}/landing/inbox"
CHECKPOINT = f"/Volumes/{CATALOG}/landing/checkpoints/bronze_orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders
# MAGIC
# MAGIC The schema is declared, not inferred, and every column is `STRING`. Inference on a
# MAGIC file this dirty would type `unit_price` from whatever the first rows happen to
# MAGIC contain and then null out everything that disagrees.
# MAGIC
# MAGIC `rescuedDataColumn` catches fields that arrive but aren't in the contract. Day 1's
# MAGIC file has a row with an eighth column, `EXTRA_FLAG`. Without this, that row either
# MAGIC fails the read or loses data silently. With it, the row lands intact and the extra
# MAGIC value is preserved as evidence of upstream drift.

# COMMAND ----------

ORDER_SCHEMA = """
    order_id    STRING,
    order_date  STRING,
    customer_id STRING,
    product_id  STRING,
    quantity    STRING,
    unit_price  STRING,
    region      STRING
"""

orders_stream = (
    spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/schema")
        # With an explicit schema, Auto Loader's default evolution mode is "none":
        # extra fields are silently ignored rather than routed to _rescued_data.
        # "rescue" activates capture. Note this still doesn't catch a CSV row with
        # MORE positional values than declared columns (see O1011 in NOTES) — that
        # needs a placeholder column or VARIANT ingestion; rescue mode alone covers
        # named/typed drift, not positional overflow.
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("pathGlobFilter", "orders_*.csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(ORDER_SCHEMA)
        .load(f"{INBOX}/orders")
        .select(
            "*",
            F.col("_metadata.file_name").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
)

query = (
    orders_stream.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.bronze.orders_raw")
)
query.awaitTermination()

print("Bronze orders row count:",
      spark.table(f"{CATALOG}.bronze.orders_raw").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product reference
# MAGIC
# MAGIC The brief refers to `products.csv`; the file actually supplied is `products.xlsx`.
# MAGIC Rather than converting it by hand — a manual step that breaks the moment this runs
# MAGIC unattended — the reader handles the format that actually arrives.
# MAGIC
# MAGIC Spark has no native Excel reader, so this reads via pandas on the driver. That is
# MAGIC acceptable for a small reference file and would not be for a fact table.
# MAGIC
# MAGIC Reference data is overwritten rather than appended: this is a snapshot of current
# MAGIC truth, not an event stream. See NOTES for what changes if categories are ever
# MAGIC restated and history matters.

# COMMAND ----------

import pandas as pd

pdf = pd.read_excel(f"{INBOX}/reference/products.xlsx", dtype=str)
pdf.columns = [c.strip().lower() for c in pdf.columns]

products_df = (
    spark.createDataFrame(pdf)
        .withColumn("_source_file", F.lit("products.xlsx"))
        .withColumn("_ingested_at", F.current_timestamp())
        .select("product_id", "product_name", "category", "list_price",
                "_source_file", "_ingested_at")
)

products_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.products_raw")

display(spark.table(f"{CATALOG}.bronze.products_raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What actually landed
# MAGIC
# MAGIC Worth looking at before moving on — this is the evidence that the ingest layer
# MAGIC preserved the mess instead of hiding it.

# COMMAND ----------

display(spark.sql(f"""
    SELECT _source_file,
           COUNT(*)                                       AS rows_landed,
           COUNT(_rescued_data)                           AS rows_with_schema_drift,
           COUNT(DISTINCT order_date)                     AS distinct_date_formats_seen
    FROM {CATALOG}.bronze.orders_raw
    GROUP BY _source_file
    ORDER BY _source_file
"""))
