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

CHECKPOINT = f"/Volumes/{CATALOG}/landing/checkpoints/silver_orders_v2"
SILVER_ORDERS    = f"{CATALOG}.silver.orders"
QUARANTINE       = f"{CATALOG}.silver.orders_quarantine"
REFRESH_QUEUE    = f"{CATALOG}.silver.gold_refresh_queue"

# COMMAND ----------



# Truncating the targets is a full rebuild. The stream checkpoint must be
# cleared too, otherwise a re-run replays nothing from Bronze and leaves
# Silver empty (the checkpoint still believes every Bronze row is promoted).
try:
    dbutils.fs.rm(CHECKPOINT, recurse=True)
    print(f"Cleared Silver checkpoint for rebuild: {CHECKPOINT}")
except Exception as e:
    print(f"No Silver checkpoint to clear ({e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product reference
# MAGIC
# MAGIC `initcap` collapses `electronics`/`Electronics` and `kitchen`/`Kitchen`, which would
# MAGIC otherwise split every Gold row in two. It is a heuristic, and I'd replace it with a
# MAGIC governed mapping table the moment a category appears that title-casing gets wrong
# MAGIC (an acronym, say). For six categories it is the proportionate choice — see NOTES.

# COMMAND ----------

PRODUCTS = f"{CATALOG}.silver.products"

incoming_products = (
    spark.table(f"{CATALOG}.bronze.products_raw")
    .select(
        F.trim("product_id").alias("product_id"),
        F.trim("product_name").alias("product_name"),
        F.initcap(F.trim("category")).alias("category"),
        F.expr(
            "try_cast(list_price AS DECIMAL(12,2))"
        ).alias("list_price"),
        F.current_timestamp().alias("_ingested_at"),
    )
    .filter(
        F.col("product_id").isNotNull()
        & (F.col("product_id") != "")
    )
    .dropDuplicates(["product_id"])
)

existing_products = spark.table(PRODUCTS).select(
    "product_id",
    "product_name",
    "category",
    "list_price",
)

changed_product_ids = (
    incoming_products.alias("n")
    .join(
        existing_products.alias("o"),
        "product_id",
        "full_outer",
    )
    .filter(
        F.col("n.product_id").isNull()
        | F.col("o.product_id").isNull()
        | ~F.col("n.product_name").eqNullSafe(F.col("o.product_name"))
        | ~F.col("n.category").eqNullSafe(F.col("o.category"))
        | ~F.col("n.list_price").eqNullSafe(F.col("o.list_price"))
    )
    .select(
        F.coalesce(
            F.col("n.product_id"),
            F.col("o.product_id"),
        ).alias("product_id")
    )
    .distinct()
)

# Existing orders using changed products must be reaggregated because
# their Gold category may have changed.
product_affected_dates = (
    spark.table(SILVER_ORDERS)
    .join(changed_product_ids, "product_id", "inner")
    .select("order_date")
    .distinct()
    .withColumn("queued_at", F.current_timestamp())
)

(
    DeltaTable.forName(spark, REFRESH_QUEUE)
    .alias("t")
    .merge(
        product_affected_dates.alias("s"),
        "t.order_date = s.order_date",
    )
    .whenMatchedUpdate(
        set={"queued_at": "s.queued_at"}
    )
    .whenNotMatchedInsert(
        values={
            "order_date": "s.order_date",
            "queued_at": "s.queued_at",
        }
    )
    .execute()
)

(
    incoming_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable(PRODUCTS)
)

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
    F.when(
        F.col("order_id").isNull()
        | (F.trim(F.col("order_id")) == ""),
        "missing_order_id",
    )
    .when(
        parsed_order_date.isNull(),
        "unparseable_order_date",
    )
    .when(
        parsed_quantity.isNull(),
        "missing_or_invalid_quantity",
    )
    .when(
        parsed_unit_price.isNull(),
        "missing_or_invalid_unit_price",
    )
    .when(
        parsed_unit_price < 0,
        "negative_unit_price",
    )
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
        print(f"Batch {batch_id}: empty batch received.")
        return

    # ---------------------------------------------------------------
    # Parsing rules (defined locally so foreachBatch always has them)
    # ---------------------------------------------------------------
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
        F.when(
            F.col("order_id").isNull()
            | (F.trim(F.col("order_id")) == ""),
            "missing_order_id",
        )
        .when(
            parsed_order_date.isNull(),
            "unparseable_order_date",
        )
        .when(
            parsed_quantity.isNull(),
            "missing_or_invalid_quantity",
        )
        .when(
            parsed_unit_price.isNull(),
            "missing_or_invalid_unit_price",
        )
        .when(
            parsed_unit_price < 0,
            "negative_unit_price",
        )
    )
    # ---------------------------------------------------------------
    # Parse and classify incoming records
    # ---------------------------------------------------------------
    parsed = batch_df.select(
        F.trim(F.col("order_id")).alias("order_id"),
        parsed_order_date.alias("order_date"),

        F.when(
            F.trim(F.col("customer_id")) == "",
            F.lit(None),
        ).otherwise(
            F.trim(F.col("customer_id"))
        ).alias("customer_id"),

        F.trim(F.col("product_id")).alias("product_id"),
        parsed_quantity.alias("quantity"),
        parsed_unit_price.alias("unit_price"),
        parsed_region.alias("region"),

        F.col("_rescued_data")
        .isNotNull()
        .alias("has_schema_drift"),

        F.col("_source_file"),

        F.to_date(
            F.regexp_extract(
                F.col("_source_file"),
                r"(\d{4}-\d{2}-\d{2})",
                1,
            )
        ).alias("_source_batch_date"),

        F.col("_ingested_at"),
        reject_reason.alias("reject_reason"),

        # Raw values retained for quarantine and auditing.
        F.col("order_date").alias("order_date_raw"),
        F.col("quantity").alias("quantity_raw"),
        F.col("unit_price").alias("unit_price_raw"),
        F.col("region").alias("region_raw"),
    )

    rejected = parsed.filter(
        F.col("reject_reason").isNotNull()
    )

    valid = parsed.filter(
        F.col("reject_reason").isNull()
    )

    input_count = batch_df.count()
    parsed_count = parsed.count()
    rejected_count = rejected.count()
    valid_count = valid.count()

    print(
        f"Batch {batch_id}: "
        f"input={input_count}, "
        f"parsed={parsed_count}, "
        f"valid={valid_count}, "
        f"rejected={rejected_count}"
    )

    # ---------------------------------------------------------------
    # Retry-idempotent quarantine
    # ---------------------------------------------------------------
    # Skip the merge when there are no rejected records.
    if rejected_count > 0:
        quarantine_rows = (
            rejected.select(
                "order_id",
                "order_date_raw",
                "customer_id",
                "product_id",
                "quantity_raw",
                "unit_price_raw",
                "region_raw",
                "reject_reason",
                "_source_file",
                "_ingested_at",
            )
            .withColumn(
                "_quarantined_at",
                F.current_timestamp(),
            )
        )

        (
            DeltaTable.forName(spark, QUARANTINE)
            .alias("t")
            .merge(
                quarantine_rows.alias("s"),
                """
                t._source_file <=> s._source_file
                AND t.order_id <=> s.order_id
                AND t.order_date_raw <=> s.order_date_raw
                AND t.customer_id <=> s.customer_id
                AND t.product_id <=> s.product_id
                AND t.quantity_raw <=> s.quantity_raw
                AND t.unit_price_raw <=> s.unit_price_raw
                AND t.region_raw <=> s.region_raw
                AND t.reject_reason <=> s.reject_reason
                """,
            )
            .whenNotMatchedInsert(
                values={
                    "order_id": "s.order_id",
                    "order_date_raw": "s.order_date_raw",
                    "customer_id": "s.customer_id",
                    "product_id": "s.product_id",
                    "quantity_raw": "s.quantity_raw",
                    "unit_price_raw": "s.unit_price_raw",
                    "region_raw": "s.region_raw",
                    "reject_reason": "s.reject_reason",
                    "_source_file": "s._source_file",
                    "_ingested_at": "s._ingested_at",
                    "_quarantined_at": "s._quarantined_at",
                }
            )
            .execute()
        )

        print(
            f"Batch {batch_id}: "
            f"{rejected_count} rejected row(s) processed."
        )

    # ---------------------------------------------------------------
    # Deterministically select one row per order_id
    # ---------------------------------------------------------------
    record_hash = F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(
                F.col("order_id"),
                F.lit(""),
            ),
            F.coalesce(
                F.col("order_date").cast("string"),
                F.lit(""),
            ),
            F.coalesce(
                F.col("customer_id"),
                F.lit(""),
            ),
            F.coalesce(
                F.col("product_id"),
                F.lit(""),
            ),
            F.coalesce(
                F.col("quantity").cast("string"),
                F.lit(""),
            ),
            F.coalesce(
                F.col("unit_price").cast("string"),
                F.lit(""),
            ),
            F.coalesce(
                F.col("region"),
                F.lit(""),
            ),
        ),
        256,
    )

    valid_with_hash = valid.withColumn(
        "_record_hash",
        record_hash,
    )

    window_spec = (
        Window.partitionBy("order_id")
        .orderBy(
            F.col("_source_batch_date")
            .desc_nulls_last(),

            F.col("_ingested_at")
            .desc_nulls_last(),

            F.col("_source_file")
            .desc_nulls_last(),

            F.col("_record_hash")
            .desc_nulls_last(),
        )
    )

    deduped = (
        valid_with_hash
        .withColumn(
            "_rn",
            F.row_number().over(window_spec),
        )
        .filter(
            F.col("_rn") == 1
        )
        .withColumn(
            "line_revenue",
            (
                F.col("quantity")
                * F.col("unit_price")
            ).cast("decimal(14,2)"),
        )
        .withColumn(
            "is_return",
            F.col("quantity") < 0,
        )
        .withColumn(
            "is_zero_quantity",
            F.col("quantity") == 0,
        )
        .select(
            "order_id",
            "order_date",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "region",
            "line_revenue",
            "is_return",
            "is_zero_quantity",
            "has_schema_drift",
            "_source_file",
            "_source_batch_date",
            "_ingested_at",
        )
    )

    deduped_count = deduped.count()

    print(
        f"Batch {batch_id}: "
        f"deduped valid rows={deduped_count}"
    )

    if deduped_count == 0:
        print(
            f"Batch {batch_id}: "
            "no valid rows available for Silver."
        )
        return

    # ---------------------------------------------------------------
    # Identify every Gold date invalidated by this batch
    # ---------------------------------------------------------------
    # Both the previous date and incoming date are included when an
    # existing order moves to a different business date.
    new_dates = (
        deduped.select("order_date")
        .filter(
            F.col("order_date").isNotNull()
        )
    )

    old_dates = (
        spark.table(SILVER_ORDERS)
        .join(
            deduped.select("order_id").distinct(),
            on="order_id",
            how="inner",
        )
        .select("order_date")
        .filter(
            F.col("order_date").isNotNull()
        )
    )

    affected_dates = (
        new_dates
        .unionByName(old_dates)
        .distinct()
        .withColumn(
            "queued_at",
            F.current_timestamp(),
        )
    )

    affected_date_count = affected_dates.count()

    print(
        f"Batch {batch_id}: "
        f"affected Gold dates={affected_date_count}"
    )

    # ---------------------------------------------------------------
    # Persist refresh intent before changing Silver
    # ---------------------------------------------------------------
    # An extra queue entry is harmless. Missing a queue entry after
    # changing Silver could leave Gold stale.
    (
        DeltaTable.forName(spark, REFRESH_QUEUE)
        .alias("t")
        .merge(
            affected_dates.alias("s"),
            "t.order_date = s.order_date",
        )
        .whenMatchedUpdate(
            set={
                "queued_at": "s.queued_at",
            }
        )
        .whenNotMatchedInsert(
            values={
                "order_date": "s.order_date",
                "queued_at": "s.queued_at",
            }
        )
        .execute()
    )

    print(
        f"Batch {batch_id}: "
        "affected dates added to refresh queue."
    )

    # ---------------------------------------------------------------
    # Idempotent Silver upsert
    # ---------------------------------------------------------------
    (
        DeltaTable.forName(spark, SILVER_ORDERS)
        .alias("t")
        .merge(
            deduped.alias("s"),
            "t.order_id = s.order_id",
        )
        .whenMatchedUpdateAll(
            condition="""
                (
                    t._source_batch_date IS NULL
                    AND s._source_batch_date IS NOT NULL
                )
                OR s._source_batch_date > t._source_batch_date
                OR (
                    s._source_batch_date
                        <=> t._source_batch_date
                    AND (
                        t._ingested_at IS NULL
                        OR s._ingested_at > t._ingested_at
                        OR (
                            s._ingested_at
                                <=> t._ingested_at
                            AND (
                                t._source_file IS NULL
                                OR s._source_file
                                    > t._source_file
                            )
                        )
                    )
                )
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    silver_count = spark.table(
        SILVER_ORDERS
    ).count()

    queue_count = (
        spark.table(REFRESH_QUEUE)
        .select("order_date")
        .distinct()
        .count()
    )

    print(
        f"Batch {batch_id} completed successfully: "
        f"silver_rows={silver_count}, "
        f"queued_dates={queue_count}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming from Bronze with `foreachBatch` means the checkpoint tracks which Bronze
# MAGIC rows have already been promoted. Re-running this notebook after a successful run
# MAGIC processes nothing. Re-running after a *failed* run reprocesses only the failed
# MAGIC batch, and because the write is a MERGE the result is identical either way.

# COMMAND ----------

print(f"Using Silver checkpoint: {CHECKPOINT}")

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

if query.exception() is not None:
    raise query.exception()

print("Silver stream completed successfully.")
print("Last progress:", query.lastProgress)

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

display(spark.sql("SELECT COUNT(*) FROM retail.silver.orders"))
display(spark.sql("SELECT COUNT(*) FROM retail.silver.gold_refresh_queue"))

# COMMAND ----------

display(spark.sql("SELECT COUNT(*) FROM retail.silver.orders"))
display(spark.sql("SELECT order_date, COUNT(*) FROM retail.silver.orders GROUP BY order_date ORDER BY order_date"))

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            COUNT(*) AS bronze_rows,
            COUNT(DISTINCT _source_file) AS source_files,
            MIN(_ingested_at) AS first_ingested_at,
            MAX(_ingested_at) AS last_ingested_at
        FROM {CATALOG}.bronze.orders_raw
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT 'bronze.orders_raw' AS table_name, COUNT(*) AS row_count
        FROM {CATALOG}.bronze.orders_raw

        UNION ALL

        SELECT 'silver.orders', COUNT(*)
        FROM {SILVER_ORDERS}

        UNION ALL

        SELECT 'quarantine', COUNT(*)
        FROM {QUARANTINE}

        UNION ALL

        SELECT 'gold_refresh_queue', COUNT(*)
        FROM {REFRESH_QUEUE}
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            order_id,
            COUNT(*) AS bronze_versions
        FROM {CATALOG}.bronze.orders_raw
        GROUP BY order_id
        HAVING COUNT(*) > 1
        ORDER BY bronze_versions DESC, order_id
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT *
        FROM {QUARANTINE}
    """)
)

# COMMAND ----------

print("QUARANTINE table:", QUARANTINE)

quarantine_df = spark.table(QUARANTINE)

quarantine_count = quarantine_df.count()
print("Current quarantine count:", quarantine_count)

display(
    quarantine_df.select(
        "order_id",
        "order_date_raw",
        "customer_id",
        "product_id",
        "quantity_raw",
        "unit_price_raw",
        "region_raw",
        "reject_reason",
        "_source_file",
        "_quarantined_at",
    )
)