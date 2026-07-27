# Retail Daily Sales Pipeline — Exercise 1

**Candidate:** Phanindra Reddy Mathireddy
**Role:** Forward-Deployed Data & AI Engineer — Synaptiq

## What this is, in plain terms

Every day, a retailer drops a spreadsheet of that day's orders into a folder. This
project turns that daily drop into a always-up-to-date sales report — broken down by
day, product type, and region — automatically, with no one having to run anything by
hand.

Three things make this more than a simple import script:

- **It never double-counts.** The same order sometimes shows up in two different
  daily files (a "did you get this?" resend). The system recognizes that and keeps
  exactly one correct copy — instead of accidentally counting that sale twice.
- **It fixes yesterday's numbers when new information arrives.** If a customer
  changes an order today, and that order was originally placed three days ago, the
  report for that earlier day updates itself to reflect the correction — nobody has
  to remember to go back and fix it manually.
- **It doesn't hide problems, it flags them.** A handful of orders reference a
  product that doesn't exist in the product list yet, or are missing a price. Rather
  than silently deleting that revenue (which would make the sales numbers *look*
  clean while quietly being wrong), the system keeps that money visible under an
  "Unknown" label and puts the truly unusable rows in a separate holding area for
  someone to review — so nothing disappears without a trace.

I tested this by actually running it — including simulating three days of real order
files, one of which corrects an earlier day's order — and confirmed the numbers come
out right, are reproducible, and update correctly on their own the moment a new file
arrives.

---

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
