# Synaptiq Retail Data and Analytics Exercises

This repository contains two technical exercises demonstrating data engineering, data-quality management, analytical judgment, and business communication using Databricks and Python.

## Exercise 1 — Retail Data Pipeline

A medallion-style Databricks pipeline covering ingestion, parsing, Bronze/Silver/Gold transformations, quarantine handling, idempotent order restatements, validation, and business-ready aggregates.

- [Open Exercise 1](./exercise-1/)
- [Read the implementation notes](./exercise-1/NOTES.md)

## Exercise 2 — Online Retail II Exploratory Analysis

A focused exploratory analysis of approximately one million retail transaction lines. The notebook examines worksheet overlap, sales seasonality, customer concentration, repeat purchasing, geography, product performance, negative transactions, and analytical limitations.

- [Open Exercise 2](./exercise-2/)
- [View the executed notebook](./exercise-2/Online_Retail_II_EDA.html)
- [View the source notebook](./exercise-2/Online_Retail_II_EDA.py)

## Repository Structure

```text
synaptiq-retail-pipeline/
├── exercise-1/
│   ├── 00_setup.py
│   ├── 01_bronze_ingest.py
│   ├── 01_bronze_ingest.html
│   ├── 02_silver.py
│   ├── 02_silver.html
│   ├── 03_gold.py
│   ├── 03_gold.html
│   ├── NOTES.md
│   ├── README.md
│   ├── databricks.yml
│   ├── parsers.py
│   ├── pipeline_architecture.mermaid
│   └── orders_2024-01-03.csv
├── exercise-2/
│   ├── Phanindra_Reddy_Mathireddy_Online_Retail_II_EDA.html
│   ├── Phanindra_Reddy_Mathireddy_Online_Retail_II_EDA.py
│   └── README.md
└── README.md
