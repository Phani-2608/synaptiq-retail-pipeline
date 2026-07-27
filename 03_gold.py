# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold
# MAGIC
# MAGIC The analyst-facing table: net revenue, order count, units sold and AOV, by
# MAGIC business date × product category × region.
# MAGIC
# MAGIC **Why this isn't an append.** Day 2's file restates an order dated day 1, so day
# MAGIC 1's aggregate is wrong the moment day 2 lands. Appending would leave two
# MAGIC conflicting versions of 2024-01-01 in the table. This notebook reads the refresh
# MAGIC queue, recomputes only the business dates that changed, and replaces them
# MAGIC atomically with `replaceWhere`.
# MAGIC
# MAGIC At 24 rows a full recompute would obviously be fine, and simpler. `replaceWhere` is
# MAGIC here because it is the version that still works when the client's order table is
# MAGIC 50M rows a day, and the cost of writing it that way now is one line.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F
import uuid, datetime

spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID        = str(uuid.uuid4())[:8]
SILVER_ORDERS = f"{CATALOG}.silver.orders"
PRODUCTS      = f"{CATALOG}.silver.products"
QUARANTINE    = f"{CATALOG}.silver.orders_quarantine"
REFRESH_QUEUE = f"{CATALOG}.silver.gold_refresh_queue"
GOLD          = f"{CATALOG}.gold.daily_sales_by_category_region"
AUDIT         = f"{CATALOG}.gold.pipeline_run_audit"

# COMMAND ----------

dates = [r.order_date for r in
         spark.table(REFRESH_QUEUE).select("order_date").distinct().collect()]

if not dates:
    dbutils.notebook.exit("No dates queued — Gold is already current.")

date_list = ", ".join(f"DATE'{d}'" for d in sorted(dates))
print(f"Rebuilding {len(dates)} business date(s): {date_list}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The aggregate
# MAGIC
# MAGIC **`LEFT JOIN`, not inner.** Two order rows reference products (`P099`, `P011`) that
# MAGIC do not exist in the reference file. An inner join would delete them and their
# MAGIC revenue, and nothing in the output would indicate anything was missing. They land
# MAGIC in category `Unknown` instead, so the books still tie and the gap becomes a visible
# MAGIC data-quality ticket for whoever owns the product master.
# MAGIC
# MAGIC **AOV caveat.** AOV is computed within each date × category × region cell. An order
# MAGIC spanning two categories would count toward both, so these AOVs do not roll up by
# MAGIC summation. In this dataset every order has exactly one line, so the distinction is
# MAGIC currently invisible — which is precisely why it belongs in the notes rather than
# MAGIC being discovered later by an analyst.

# COMMAND ----------

agg = spark.sql(f"""
    SELECT
        o.order_date,
        COALESCE(p.category, 'Unknown')                    AS category,
        o.region,
        CAST(SUM(o.line_revenue) AS DECIMAL(18,2))         AS net_revenue,
        COUNT(DISTINCT o.order_id)                         AS order_count,
        CAST(SUM(o.quantity) AS BIGINT)                    AS units_sold,
        CAST(SUM(o.line_revenue) / NULLIF(COUNT(DISTINCT o.order_id), 0)
             AS DECIMAL(18,2))                             AS aov,
        CURRENT_TIMESTAMP()                                AS _refreshed_at
    FROM {SILVER_ORDERS} o
    LEFT JOIN {PRODUCTS} p
           ON o.product_id = p.product_id
    WHERE o.order_date IN ({date_list})
    GROUP BY o.order_date, COALESCE(p.category, 'Unknown'), o.region
""")

(agg.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"order_date IN ({date_list})")
    .saveAsTable(GOLD))

spark.sql(f"DELETE FROM {REFRESH_QUEUE} WHERE order_date IN ({date_list})")
print("Gold rebuilt; queue cleared.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation
# MAGIC
# MAGIC Every run writes one audit row. The invariant is
# MAGIC `rows_in = rows_loaded + rows_quarantined` — if it ever fails, rows went missing
# MAGIC somewhere between Bronze and Silver and the pipeline should not be trusted until
# MAGIC someone finds out where.
# MAGIC
# MAGIC Note `rows_in` counts distinct `order_id`s, not raw rows: the same order legitimately
# MAGIC appears in two files, and collapsing it is the intended behaviour, not loss.

# COMMAND ----------

stats = spark.sql(f"""
    SELECT
      (SELECT COUNT(DISTINCT order_id) FROM {CATALOG}.bronze.orders_raw) AS rows_in,
      (SELECT COUNT(*)                 FROM {SILVER_ORDERS})             AS rows_loaded,
      (SELECT COUNT(*)                 FROM {QUARANTINE})                AS rows_quarantined,
      (SELECT CAST(SUM(net_revenue) AS DECIMAL(18,2)) FROM {GOLD})       AS net_revenue,
      -- Silver is meant to hold exactly one current row per order_id. If this
      -- ever diverges the MERGE key or the dedup window logic has a bug.
      (SELECT COUNT(*) FROM {SILVER_ORDERS}) -
        (SELECT COUNT(DISTINCT order_id) FROM {SILVER_ORDERS})           AS silver_duplicate_keys,
      -- Gold is derived entirely from Silver, so the two must tie exactly for
      -- any date Gold currently covers. A gap here means the aggregate query
      -- or the affected-dates rebuild logic dropped or double-counted rows.
      (SELECT CAST(SUM(o.line_revenue) AS DECIMAL(18,2))
         FROM {SILVER_ORDERS} o
         WHERE o.order_date IN (SELECT DISTINCT order_date FROM {GOLD})) AS silver_revenue_for_gold_dates
""").first()

balanced          = stats.rows_in == stats.rows_loaded + stats.rows_quarantined
silver_deduped    = stats.silver_duplicate_keys == 0
gold_ties_silver  = stats.net_revenue == stats.silver_revenue_for_gold_dates
all_invariants_ok = balanced and silver_deduped and gold_ties_silver

spark.createDataFrame(
    [(RUN_ID, datetime.datetime.utcnow(), "gold_refresh",
      stats.rows_in, stats.rows_loaded, stats.rows_quarantined, stats.net_revenue,
      f"dates_rebuilt={len(dates)}; bronze_silver_reconciled={balanced}; "
      f"silver_deduped={silver_deduped}; gold_ties_silver={gold_ties_silver}")],
    schema="run_id string, run_ts timestamp, stage string, rows_in bigint, "
           "rows_loaded bigint, rows_quarantined bigint, net_revenue decimal(18,2), notes string"
).write.mode("append").saveAsTable(AUDIT)

print(f"rows_in={stats.rows_in}  loaded={stats.rows_loaded}  quarantined={stats.rows_quarantined}")
print(f"bronze_silver_reconciled={balanced}  silver_deduped={silver_deduped}  "
      f"gold_ties_silver={gold_ties_silver}")

assert balanced, "Reconciliation failed — rows disappeared between Bronze and Silver."
assert silver_deduped, (
    f"Silver has {stats.silver_duplicate_keys} duplicate order_id rows — "
    "the MERGE key or dedup window has a bug."
)
assert gold_ties_silver, (
    f"Gold net_revenue ({stats.net_revenue}) does not match Silver for the same "
    f"dates ({stats.silver_revenue_for_gold_dates}) — the aggregate query or the "
    "affected-dates rebuild dropped or double-counted rows."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The output table
# MAGIC
# MAGIC This is the deliverable. Copy it into NOTES.md.

# COMMAND ----------

display(spark.sql(f"""
    SELECT order_date, category, region, net_revenue, order_count, units_sold, aov
    FROM {GOLD}
    ORDER BY order_date, category, region
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency proof
# MAGIC
# MAGIC The claim this pipeline makes is that re-landing a file changes nothing. Delta's
# MAGIC transaction log lets me demonstrate that rather than assert it: re-run notebooks
# MAGIC 01–03 with the same files present, then compare the current Gold total against the
# MAGIC version before the re-run. They should be identical.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {GOLD}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the version numbers to bracket a re-run, then execute.
# MAGIC -- SELECT 'before' AS state, SUM(net_revenue) FROM retail.gold.daily_sales_by_category_region VERSION AS OF 1
# MAGIC -- UNION ALL
# MAGIC -- SELECT 'after',           SUM(net_revenue) FROM retail.gold.daily_sales_by_category_region;
