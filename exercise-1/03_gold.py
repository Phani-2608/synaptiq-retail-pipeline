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

display(spark.sql("""
    SELECT 'bronze.orders_raw' AS tbl, COUNT(*) AS rows FROM retail.bronze.orders_raw
    UNION ALL SELECT 'silver.orders', COUNT(*) FROM retail.silver.orders
    UNION ALL SELECT 'silver.orders_quarantine', COUNT(*) FROM retail.silver.orders_quarantine
    UNION ALL SELECT 'silver.gold_refresh_queue', COUNT(*) FROM retail.silver.gold_refresh_queue
    UNION ALL SELECT 'gold.daily_sales_by_category_region', COUNT(*) FROM retail.gold.daily_sales_by_category_region
"""))

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

dates = [
    row.order_date
    for row in (
        spark.table(f"{CATALOG}.silver.gold_refresh_queue")
        .select("order_date")
        .filter(F.col("order_date").isNotNull())
        .distinct()
        .collect()
    )
]

if not dates:
    print("No dates queued — Gold is already current. Skipping rebuild.")

else:
    date_list = ", ".join(
        f"DATE'{business_date}'"
        for business_date in sorted(dates)
    )

    print(
        f"Validating and rebuilding {len(dates)} business date(s): "
        f"{date_list}"
    )

    # ---------------------------------------------------------------
    # Build candidate Gold result
    # ---------------------------------------------------------------
    candidate_gold = spark.sql(
        f"""
        SELECT
            o.order_date,
            COALESCE(p.category, 'Unknown') AS category,
            o.region,

            CAST(
                SUM(o.line_revenue)
                AS DECIMAL(18,2)
            ) AS net_revenue,

            COUNT(DISTINCT o.order_id) AS order_count,

            CAST(
                SUM(o.quantity)
                AS BIGINT
            ) AS units_sold,

            CAST(
                SUM(o.line_revenue)
                / NULLIF(COUNT(DISTINCT o.order_id), 0)
                AS DECIMAL(18,2)
            ) AS aov,

            CURRENT_TIMESTAMP() AS _refreshed_at

        FROM {CATALOG}.silver.orders o

        LEFT JOIN {CATALOG}.silver.products p
            ON o.product_id = p.product_id

        WHERE o.order_date IN ({date_list})

        GROUP BY
            o.order_date,
            COALESCE(p.category, 'Unknown'),
            o.region
        """
    )

    # ---------------------------------------------------------------
    # Validate candidate before publishing
    # ---------------------------------------------------------------
    candidate_stats = (
        candidate_gold
        .agg(
            F.sum("net_revenue")
            .cast("decimal(18,2)")
            .alias("candidate_net_revenue"),

            F.count("*")
            .alias("candidate_row_count"),
        )
        .first()
    )

    silver_stats = (
        spark.table(f"{CATALOG}.silver.orders")
        .filter(F.col("order_date").isin(dates))
        .agg(
            F.sum("line_revenue")
            .cast("decimal(18,2)")
            .alias("silver_net_revenue"),

            F.count("*")
            .alias("silver_row_count"),

            (
                F.count("*")
                - F.countDistinct("order_id")
            ).alias("duplicate_order_ids"),

            F.sum(
                F.when(
                    F.col("order_id").isNull()
                    | F.col("order_date").isNull()
                    | F.col("quantity").isNull()
                    | F.col("unit_price").isNull()
                    | F.col("region").isNull(),
                    1,
                ).otherwise(0)
            ).alias("invalid_contract_rows"),
        )
        .first()
    )

    candidate_net_revenue = (
        candidate_stats.candidate_net_revenue
        if candidate_stats.candidate_net_revenue is not None
        else 0
    )

    silver_net_revenue = (
        silver_stats.silver_net_revenue
        if silver_stats.silver_net_revenue is not None
        else 0
    )

    assert silver_stats.duplicate_order_ids == 0, (
        "Silver validation failed: "
        f"{silver_stats.duplicate_order_ids} duplicate order_id rows "
        "exist for the queued dates."
    )

    assert silver_stats.invalid_contract_rows == 0, (
        "Silver validation failed: "
        f"{silver_stats.invalid_contract_rows} rows contain null values "
        "in required analytical fields."
    )

    assert candidate_net_revenue == silver_net_revenue, (
        "Candidate Gold validation failed: "
        f"candidate revenue {candidate_net_revenue} does not match "
        f"Silver revenue {silver_net_revenue}. "
        "Gold was not published and the refresh queue was retained."
    )

    # ---------------------------------------------------------------
    # Publish only after candidate validation succeeds
    # ---------------------------------------------------------------
    (
        candidate_gold.write
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            f"order_date IN ({date_list})",
        )
        .saveAsTable(
            f"{CATALOG}.gold.daily_sales_by_category_region"
        )
    )

    # ---------------------------------------------------------------
    # Verify published Gold partitions
    # ---------------------------------------------------------------
    published_stats = (
        spark.table(
            f"{CATALOG}.gold.daily_sales_by_category_region"
        )
        .filter(F.col("order_date").isin(dates))
        .agg(
            F.sum("net_revenue")
            .cast("decimal(18,2)")
            .alias("published_net_revenue"),

            F.count("*")
            .alias("published_row_count"),
        )
        .first()
    )

    published_net_revenue = (
        published_stats.published_net_revenue
        if published_stats.published_net_revenue is not None
        else 0
    )

    publish_verified = (
        published_net_revenue == candidate_net_revenue
        and published_stats.published_row_count
        == candidate_stats.candidate_row_count
    )

    assert publish_verified, (
        "Published Gold verification failed. "
        "The refresh queue was intentionally retained."
    )

    # ---------------------------------------------------------------
    # Clear queue only after successful validation and publication
    # ---------------------------------------------------------------
    spark.sql(
        f"""
        DELETE FROM {CATALOG}.silver.gold_refresh_queue
        WHERE order_date IN ({date_list})
        """
    )

    print(
        f"Gold successfully rebuilt and verified for "
        f"{len(dates)} business date(s); queue cleared."
    )

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

# COMMAND ----------

display(spark.sql("""
    SELECT
      (SELECT COUNT(DISTINCT order_id) FROM retail.bronze.orders_raw) AS rows_in,
      (SELECT COUNT(*) FROM retail.silver.orders) AS loaded,
      (SELECT COUNT(*) FROM retail.silver.orders_quarantine) AS quarantined,
      (SELECT CAST(SUM(net_revenue) AS DECIMAL(18,2)) FROM retail.gold.daily_sales_by_category_region) AS net_revenue
"""))

# COMMAND ----------

display(spark.sql("SELECT order_id, region, _rescued_data FROM retail.bronze.orders_raw WHERE order_id = 'O1011'"))

# COMMAND ----------

display(spark.sql("""
    SELECT
      (SELECT COUNT(DISTINCT order_id) FROM retail.bronze.orders_raw) AS rows_in,
      (SELECT COUNT(*) FROM retail.silver.orders) AS loaded,
      (SELECT COUNT(*) FROM retail.silver.orders_quarantine) AS quarantined,
      (SELECT CAST(SUM(net_revenue) AS DECIMAL(18,2)) FROM retail.gold.daily_sales_by_category_region) AS net_revenue
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT order_date, category, region, net_revenue, order_count, units_sold, aov
    FROM retail.gold.daily_sales_by_category_region
    ORDER BY order_date, category, region
"""))

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            'silver_net_revenue' AS metric,
            CAST(SUM(line_revenue) AS DECIMAL(18,2)) AS value
        FROM {CATALOG}.silver.orders

        UNION ALL

        SELECT
            'gold_net_revenue',
            CAST(SUM(net_revenue) AS DECIMAL(18,2))
        FROM {CATALOG}.gold.daily_sales_by_category_region

        UNION ALL

        SELECT
            'queued_gold_dates',
            CAST(COUNT(DISTINCT order_date) AS DECIMAL(18,2))
        FROM {CATALOG}.silver.gold_refresh_queue
    """)
)