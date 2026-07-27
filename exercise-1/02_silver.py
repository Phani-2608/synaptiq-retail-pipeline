# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver
# MAGIC
# MAGIC Parse, validate, conform, deduplicate.
# MAGIC
# MAGIC This notebook holds the two decisions the exercise is really testing:
# MAGIC
# MAGIC 1. **Deduplication is a MERGE on `order_id`, not a `DISTINCT`.** Day 2's file
# MAGIC    contains `O1002` identical to day 1 (a re-send) *and* `O1003` with the quantity
# MAGIC    changed from 5 to 6 (a restatement). `DISTINCT` keeps both versions of `O1003`
# MAGIC    and double-counts it. The correct rule is last-writer-wins per business key.
# MAGIC
# MAGIC 2. **The ordering key is the date in the filename, not the ingestion timestamp.**
# MAGIC    If both files are loaded in one batch, `_ingested_at` is identical and the tie
# MAGIC    break is arbitrary. Worse, a backfill replaying old files would let stale data
# MAGIC    overwrite good data. `_source_batch_date` is derived from the filename and
# MAGIC    reflects what the source actually asserted, in order.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail")
CATALOG = dbutils.widgets.get("catalog")

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.session.timeZone", "UTC")

CHECKPOINT       = f"/Volumes/{CATALOG}/landing/checkpoints/silver_orders"
SILVER_ORDERS    = f"{CATALOG}.silver.orders"
QUARANTINE       = f"{CATALOG}.silver.orders_quarantine"
REFRESH_QUEUE    = f"{CATALOG}.silver.gold_refresh_queue"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product reference
# MAGIC
# MAGIC `initcap` collapses `electronics`/`Electronics` and `kitchen`/`Kitchen`, which would
# MAGIC otherwise split every Gold row in two. It is a heuristic, and I'd replace it with a
# MAGIC governed mapping table the moment a category appears that title-casing gets wrong
# MAGIC (an acronym, say). For six categories it is the proportionate choice — see NOTES.

# COMMAND ----------

(spark.table(f"{CATALOG}.bronze.products_raw")
    .select(
        F.trim("product_id").alias("product_id"),
        F.trim("product_name").alias("product_name"),
        F.initcap(F.trim("category")).alias("category"),
        F.expr("try_cast(list_price AS DECIMAL(12,2))").alias("list_price"),
        F.current_timestamp().alias("_ingested_at"),
    )
    .write.mode("overwrite").saveAsTable(f"{CATALOG}.silver.products"))

display(spark.table(f"{CATALOG}.silver.products"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing rules
# MAGIC
# MAGIC **Dates** arrive in three formats: ISO, `MM/dd/yyyy`, and a 10-digit Unix epoch.
# MAGIC The slash format is genuinely ambiguous — `01/02/2024` is either 2 January or
# MAGIC 1 February. It resolves to US-style `MM/dd/yyyy` because that row sits in the file
# MAGIC dropped on 2024-01-02, so day-month would place an order a month before its own
# MAGIC drop date. That inference is stated in NOTES rather than buried here, because it is
# MAGIC exactly the kind of assumption a client should get the chance to correct.
# MAGIC
# MAGIC **Prices** carry a currency symbol and thousands separators (`"$1,099.00"`). All
# MAGIC non-numeric characters are stripped before casting to `DECIMAL`.
# MAGIC
# MAGIC `try_*` functions are used throughout: a value that cannot be parsed becomes null
# MAGIC and is routed to quarantine, rather than throwing and killing the run.

# COMMAND ----------

parsed_order_date = F.coalesce(
    F.expr("try_to_date(trim(order_date), 'yyyy-MM-dd')"),
    F.expr("try_to_date(trim(order_date), 'MM/dd/yyyy')"),
    F.expr("""CASE WHEN trim(order_date) RLIKE '^[0-9]{10}$'
                   THEN CAST(from_unixtime(CAST(trim(order_date) AS BIGINT)) AS DATE) END"""),
)

parsed_quantity   = F.expr("try_cast(trim(quantity) AS INT)")
parsed_unit_price = F.expr("try_cast(regexp_replace(trim(unit_price), '[^0-9.-]', '') AS DECIMAL(12,2))")
parsed_region     = F.expr("""CASE WHEN region IS NULL OR trim(region) = '' THEN 'Unknown'
                                  ELSE initcap(trim(region)) END""")

# First failing check wins, so reject_reason is always the most fundamental problem.
reject_reason = (
    F.when(F.col("order_id").isNull() | (F.trim("order_id") == ""), "missing_order_id")
     .when(parsed_order_date.isNull(),   "unparseable_order_date")
     .when(parsed_quantity.isNull(),     "missing_or_invalid_quantity")
     .when(parsed_unit_price.isNull(),   "missing_or_invalid_unit_price")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What gets quarantined, and what does not
# MAGIC
# MAGIC Quarantine is for rows that cannot be interpreted. It is deliberately **not** used
# MAGIC for rows that are merely unusual:
# MAGIC
# MAGIC | Condition | Decision | Why |
# MAGIC |---|---|---|
# MAGIC | Unparseable date, quantity, or price | Quarantine | The row cannot be aggregated at all |
# MAGIC | `quantity = -1` | Keep, flag `is_return` | The brief asks for **net** revenue; a return nets out |
# MAGIC | `quantity = 0` | Keep, flag `is_zero_quantity` | A real order that shipped nothing; dropping it would overstate AOV |
# MAGIC | Missing `customer_id` | Keep | Not needed for this aggregate. Dropping it would delete real revenue |
# MAGIC | Extra column (`EXTRA_FLAG`) | Keep, flag `has_schema_drift` | The row is valid; the *schema* is the problem. Alert, don't discard |
# MAGIC | Unknown `product_id` | Keep, resolved to category `Unknown` in Gold | An inner join would silently delete revenue |
# MAGIC
# MAGIC The one row that fails here is `O2009`, which has no `unit_price`. It could instead
# MAGIC be imputed from `products.list_price` ($29.99). I chose not to: inventing revenue is
# MAGIC harder to detect than missing revenue. This is a business decision, not a technical
# MAGIC one, and it is the first question I'd put to the client.

# COMMAND ----------

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    parsed = batch_df.select(
        F.trim("order_id").alias("order_id"),
        parsed_order_date.alias("order_date"),
        F.when(F.trim("customer_id") == "", None).otherwise(F.trim("customer_id")).alias("customer_id"),
        F.trim("product_id").alias("product_id"),
        parsed_quantity.alias("quantity"),
        parsed_unit_price.alias("unit_price"),
        parsed_region.alias("region"),
        F.col("_rescued_data").isNotNull().alias("has_schema_drift"),
        F.col("_source_file"),
        F.to_date(F.regexp_extract("_source_file", r"(\d{4}-\d{2}-\d{2})", 1)).alias("_source_batch_date"),
        F.col("_ingested_at"),
        reject_reason.alias("reject_reason"),
        # raw values retained for the quarantine record
        F.col("order_date").alias("order_date_raw"),
        F.col("quantity").alias("quantity_raw"),
        F.col("unit_price").alias("unit_price_raw"),
        F.col("region").alias("region_raw"),
    )

    rejected = parsed.filter(F.col("reject_reason").isNotNull())
    valid    = parsed.filter(F.col("reject_reason").isNull())

    # ---- quarantine -------------------------------------------------------
    (rejected.select(
        "order_id", "order_date_raw", "customer_id", "product_id",
        "quantity_raw", "unit_price_raw", "region_raw",
        "reject_reason", "_source_file", "_ingested_at",
        F.current_timestamp().alias("_quarantined_at"))
     .write.mode("append").saveAsTable(QUARANTINE))

    # ---- collapse to one row per order_id, newest assertion wins ----------
    w = Window.partitionBy("order_id").orderBy(
        F.col("_source_batch_date").desc_nulls_last(),
        F.col("_ingested_at").desc(),
    )
    deduped = (
        valid.withColumn("_rn", F.row_number().over(w))
             .filter("_rn = 1")
             .withColumn("line_revenue",
                         (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)"))
             .withColumn("is_return", F.col("quantity") < 0)
             .withColumn("is_zero_quantity", F.col("quantity") == 0)
             .select("order_id", "order_date", "customer_id", "product_id",
                     "quantity", "unit_price", "region", "line_revenue",
                     "is_return", "is_zero_quantity", "has_schema_drift",
                     "_source_file", "_source_batch_date", "_ingested_at")
    )
    # Note: no .cache() here. Serverless compute (Free Edition) does not support
    # PERSIST TABLE / .cache(), so `deduped` gets recomputed for the merge and for
    # the affected-dates lookup below. Fine at this volume; on classic compute
    # I'd cache it, since it's read twice.

    # ---- dates whose Gold aggregate this batch invalidates ----------------
    # Captured BEFORE the merge. If a restatement moves an order to a different
    # date, the date it left is stale too — not just the date it arrived on.
    new_dates = deduped.select("order_date")
    old_dates = (spark.table(SILVER_ORDERS)
                      .join(deduped.select("order_id").distinct(), "order_id")
                      .select("order_date"))
    affected = new_dates.union(old_dates).distinct()

    # ---- idempotent upsert ------------------------------------------------
    (DeltaTable.forName(spark, SILVER_ORDERS).alias("t")
        .merge(deduped.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s._source_batch_date >= t._source_batch_date")
        .whenNotMatchedInsertAll()
        .execute())

    (affected.withColumn("queued_at", F.current_timestamp())
             .write.mode("append").saveAsTable(REFRESH_QUEUE))

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming from Bronze with `foreachBatch` means the checkpoint tracks which Bronze
# MAGIC rows have already been promoted. Re-running this notebook after a successful run
# MAGIC processes nothing. Re-running after a *failed* run reprocesses only the failed
# MAGIC batch, and because the write is a MERGE the result is identical either way.

# COMMAND ----------

query = (
    spark.readStream
        .option("ignoreDeletes", "true")
        .table(f"{CATALOG}.bronze.orders_raw")
        .writeStream
        .option("checkpointLocation", CHECKPOINT)
        .foreachBatch(process_batch)
        .trigger(availableNow=True)
        .start()
)
query.awaitTermination()

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'silver.orders'     AS tbl, COUNT(*) AS rows FROM {SILVER_ORDERS}
    UNION ALL
    SELECT 'quarantine',             COUNT(*)        FROM {QUARANTINE}
    UNION ALL
    SELECT 'dates queued for Gold',  COUNT(DISTINCT order_date) FROM {REFRESH_QUEUE}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Proof the restatement was handled
# MAGIC
# MAGIC `O1003` arrived twice — quantity 5 on day 1, quantity 6 on day 2. Silver should
# MAGIC hold exactly one row, with quantity 6, sourced from the day-2 file.

# COMMAND ----------

display(spark.sql(f"""
    SELECT order_id, order_date, quantity, unit_price, line_revenue, _source_file
    FROM {SILVER_ORDERS}
    WHERE order_id IN ('O1002', 'O1003')
"""))

# COMMAND ----------

display(spark.sql(f"SELECT reject_reason, COUNT(*) AS rows FROM {QUARANTINE} GROUP BY 1"))
