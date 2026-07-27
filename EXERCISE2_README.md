# Online Retail II — Exploratory Data Analysis

**Candidate:** Phanindra Reddy Mathireddy
**Deliverable:** Single Databricks notebook
**Dataset:** UCI / Kaggle Online Retail II (~1M transaction lines, UK retailer,
2009-2011)

## What's here

- `Phanindra_Reddy_Mathireddy_Online_Retail_II_EDA.html` — the rendered notebook,
  with all output (tables, charts) included, viewable directly in a browser
  without a Databricks workspace.
- `Phanindra_Reddy_Mathireddy_Online_Retail_II_EDA.py` — the notebook source,
  importable directly into Databricks (Workspace → Import).

## Plain-language summary

Two years of transaction history were analyzed to understand the business: who
buys, when, from where, and where the data itself can't be trusted at face value.

A few things that shape the whole analysis:

- The two source worksheets overlap by date and contain ~22,500 identical rows —
  combining them naively would have overstated revenue by roughly £440,000. That's
  resolved before anything else.
- A small number of established customers drive most identifiable revenue.
- The headline "return rate" is misleading — about half of what looks like returns
  is actually administrative entries (fees, manual corrections), not real product
  returns.
- The dataset's final month is a partial month (cuts off December 9, 2011) and is
  explicitly never compared to a complete month.

## Structure

The notebook follows the required 8 sections: executive summary, data overview,
data quality checks, exploratory analysis, findings with confidence levels,
caveats, recommended next steps, and use of GenAI.
