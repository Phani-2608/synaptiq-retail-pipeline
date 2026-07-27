# Retail Daily Sales Pipeline — Exercise 1

**Candidate:** Phanindra Reddy Mathireddy
**Role:** Forward-Deployed Data & AI Engineer — Synaptiq

End-to-end Databricks pipeline (medallion architecture, serverless, file-arrival
trigger) processing daily order CSVs and a product reference file into an
analytics-ready Gold table: net revenue, order count, units sold, and AOV, by
business date x product category x region.

See **NOTES.md** for the full write-up: output tables, design rationale,
assumptions, data-quality decisions, three Databricks/Delta platform behaviours
found while building this, and the AI-assisted-development disclosure.

## Structure

```
00_setup.py              Creates catalog, schemas, volumes, tables (run once)
01_bronze_ingest.py       Auto Loader ingest of orders + products
02_silver.py              Parse, validate, quarantine, dedup/upsert
03_gold.py                Aggregate, affected-date rebuild, reconciliation
parsers.py                Pure-Python unit tests for the date/price/region parsing rules
databricks.yml            Databricks Asset Bundle — deployable job definition (dev/prod)
pipeline_architecture.mermaid   Architecture diagram
orders_2024-01-03.csv     Synthetic third file used to demonstrate the trigger (see NOTES §2)
NOTES.md                  Full write-up
*.html                    Rendered notebook exports with output included
```

## Quick start

1. Run `00_setup.py` once — creates the `retail` catalog, schemas, volumes, and tables.
2. Upload `orders_2024-01-01.csv` and `orders_2024-01-02.csv` (supplied in the
   assessment) plus `products.xlsx` to the volumes `00_setup.py` creates.
3. Run `01_bronze_ingest.py` → `02_silver.py` → `03_gold.py`, in order.
4. The output table and full reconciliation are in NOTES.md §1 (official two-file
   result) and §2 (trigger demonstration with a third, synthetic file).

For a scheduled deployment: `databricks bundle deploy -t dev` (or `-t prod`), using
the included `databricks.yml`. The job triggers on file arrival in
`/Volumes/<catalog>/landing/inbox/orders/` rather than on a fixed schedule — see
NOTES.md §6 for why.

## Tests

```
python parsers.py
```

Runs 14 assertions against the date/price/region parsing rules used in
`02_silver.py`, independent of a Spark session.
