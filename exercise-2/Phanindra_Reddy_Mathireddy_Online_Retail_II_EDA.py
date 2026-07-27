# Databricks notebook source
# MAGIC %md
# MAGIC # Online Retail II — Focused Exploratory Data Analysis
# MAGIC
# MAGIC **Candidate:** Phanindra Reddy Mathireddy  
# MAGIC **Candidate deliverable:** Single Databricks notebook  
# MAGIC **Dataset:** UCI / Kaggle Online Retail II  

# COMMAND ----------

# MAGIC %md
# MAGIC # Plain-English summary
# MAGIC
# MAGIC **What this is:** an analysis of two years of a UK retailer's transaction history —
# MAGIC roughly one million order lines — to understand what the business actually looks
# MAGIC like: who buys, when they buy, where they're located, and where the data itself
# MAGIC can't be trusted at face value.
# MAGIC
# MAGIC **Why it isn't as simple as "add up the sales":**
# MAGIC
# MAGIC - **The source file itself would have lied to us if used as-is.** The data comes in
# MAGIC   two overlapping spreadsheets, and about 22,500 rows appear in both — the same
# MAGIC   sale, recorded twice. Just combining the sheets naively would have overstated
# MAGIC   revenue by roughly **£440,000** before any real analysis even began. That gets
# MAGIC   caught and corrected first.
# MAGIC - **A small number of customers matter disproportionately.** The top 1% of
# MAGIC   known customers account for about a third of identifiable revenue, and
# MAGIC   customers who've bought more than once make up nearly all of it — a strong
# MAGIC   signal that keeping existing customers happy may matter more than chasing new
# MAGIC   ones.
# MAGIC - **"Returns" aren't what they first appear to be.** At a glance it looks like
# MAGIC   about 7% of sales get reversed. But roughly half of that isn't product returns
# MAGIC   at all — it's administrative entries like fees and manual corrections mixed into
# MAGIC   the same field. The real product-return rate is closer to half that headline
# MAGIC   number, and reporting the wrong figure could send the business chasing a
# MAGIC   quality problem that isn't really there.
# MAGIC - **The business is heavily UK-based, with real seasonality.** Sales consistently
# MAGIC   spike toward the end of the year, which has real implications for
# MAGIC   inventory and staffing planning — and the data cuts off mid-December in its
# MAGIC   final month, so that month must never be compared to a complete one.
# MAGIC
# MAGIC **What I did not do:** build a predictive model, or claim any of this proves *why*
# MAGIC something happens (e.g., that a loyalty program would work) — the brief asked for
# MAGIC exploratory analysis and judgment, and correlation-shaped findings are labeled as
# MAGIC exactly that, not dressed up as causal conclusions.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %pip install -q openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup and data loading
# MAGIC
# MAGIC Upload `online_retail_II.xlsx` to a Databricks Unity Catalog volume (recommended) or a workspace file location. Then update the widget path below. The analysis uses pandas because the source is an Excel workbook and the dataset is approximately one million rows—small enough for a focused, time-boxed EDA on a Databricks driver. Spark would be appropriate if the source were materially larger or already stored as Delta.

# COMMAND ----------

import math
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd

pd.set_option("display.max_columns", 30)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

# Update this default after uploading the Excel workbook.
dbutils.widgets.text(
    "data_path",
    "/Volumes/retail/landing/inbox/exercise2/online_retail_II.xlsx",
    "Online Retail II Excel path",
)
DATA_PATH = dbutils.widgets.get("data_path").strip()

if not Path(DATA_PATH).exists():
    raise FileNotFoundError(
        f"File not found: {DATA_PATH}\n"
        "Upload online_retail_II.xlsx to a Unity Catalog volume or workspace files, "
        "then update the data_path widget and rerun."
    )

DATA_PATH

# COMMAND ----------

# Read every worksheet and standardize the schema.
raw_sheets = pd.read_excel(DATA_PATH, sheet_name=None, engine="openpyxl")

rename_map = {
    "Invoice": "invoice",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "unit_price",
    "Customer ID": "customer_id",
    "Country": "country",
}

required_columns = set(rename_map)
prepared_sheets = []

for sheet_name, frame in raw_sheets.items():
    missing = required_columns - set(frame.columns)
    if missing:
        raise ValueError(f"Worksheet '{sheet_name}' is missing columns: {sorted(missing)}")

    frame = frame.rename(columns=rename_map)[list(rename_map.values())].copy()
    frame["source_sheet"] = sheet_name
    frame["invoice"] = frame["invoice"].astype("string").str.strip()
    frame["stock_code"] = frame["stock_code"].astype("string").str.strip()
    frame["description"] = frame["description"].astype("string").str.strip()
    frame["invoice_date"] = pd.to_datetime(frame["invoice_date"], errors="coerce")
    frame["quantity"] = pd.to_numeric(frame["quantity"], errors="coerce")
    frame["unit_price"] = pd.to_numeric(frame["unit_price"], errors="coerce")
    frame["customer_id"] = pd.to_numeric(frame["customer_id"], errors="coerce").astype("Int64")
    frame["country"] = frame["country"].astype("string").str.strip()
    prepared_sheets.append(frame)

sheet_summary = pd.DataFrame(
    {
        "sheet": [x["source_sheet"].iloc[0] for x in prepared_sheets],
        "rows": [len(x) for x in prepared_sheets],
        "min_date": [x["invoice_date"].min() for x in prepared_sheets],
        "max_date": [x["invoice_date"].max() for x in prepared_sheets],
    }
).sort_values("min_date")

display(sheet_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve worksheet overlap before unioning
# MAGIC
# MAGIC The two worksheets are named like adjacent years, but their date ranges overlap. I test whether the overlapping rows are exact copies. If they are, I retain the later worksheet as the source of truth for the overlap and trim the earlier worksheet. This is safer than blindly concatenating the sheets and silently inflating sales.

# COMMAND ----------

BASE_COLUMNS = [
    "invoice",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
]

ordered_sheets = sorted(prepared_sheets, key=lambda x: x["invoice_date"].min())

if len(ordered_sheets) != 2:
    raise ValueError(f"Expected two worksheets; found {len(ordered_sheets)}")

older_sheet, newer_sheet = ordered_sheets
newer_start = newer_sheet["invoice_date"].min()
older_end = older_sheet["invoice_date"].max()

overlap_exists = newer_start <= older_end

if overlap_exists:
    old_overlap = older_sheet[older_sheet["invoice_date"] >= newer_start].copy()
    new_overlap = newer_sheet[newer_sheet["invoice_date"] <= older_end].copy()

    old_hashes = Counter(pd.util.hash_pandas_object(old_overlap[BASE_COLUMNS], index=False).tolist())
    new_hashes = Counter(pd.util.hash_pandas_object(new_overlap[BASE_COLUMNS], index=False).tolist())
    exact_cross_sheet_overlap_rows = sum((old_hashes & new_hashes).values())
    overlap_is_exact_copy = (
        len(old_overlap) == len(new_overlap) == exact_cross_sheet_overlap_rows
    )

    if not overlap_is_exact_copy:
        raise ValueError(
            "The worksheet date ranges overlap but are not exact copies. "
            "Investigate source lineage before choosing a deduplication rule."
        )

    # Keep the newer worksheet for the overlap period.
    no_overlap = pd.concat(
        [older_sheet[older_sheet["invoice_date"] < newer_start], newer_sheet],
        ignore_index=True,
    )
else:
    old_overlap = newer_sheet.iloc[0:0]
    exact_cross_sheet_overlap_rows = 0
    no_overlap = pd.concat(ordered_sheets, ignore_index=True)

# Exact duplicates still exist within the retained source periods.
remaining_exact_duplicate_rows = int(no_overlap.duplicated(subset=BASE_COLUMNS).sum())
analysis_df = no_overlap.drop_duplicates(subset=BASE_COLUMNS, keep="first").copy()

# Derived fields and analysis populations.
analysis_df["line_value"] = analysis_df["quantity"] * analysis_df["unit_price"]
analysis_df["is_cancel_invoice"] = analysis_df["invoice"].str.upper().str.startswith("C", na=False)
analysis_df["is_sale_line"] = (
    (analysis_df["quantity"] > 0)
    & (analysis_df["unit_price"] > 0)
    & (~analysis_df["is_cancel_invoice"])
)
analysis_df["is_negative_line"] = (
    (analysis_df["quantity"] < 0) & (analysis_df["unit_price"] > 0)
)
analysis_df["is_product_like_code"] = analysis_df["stock_code"].str.match(r"^\d", na=False)
analysis_df["month"] = analysis_df["invoice_date"].dt.to_period("M").dt.to_timestamp()

sales = analysis_df[analysis_df["is_sale_line"]].copy()
negative_lines = analysis_df[analysis_df["is_negative_line"]].copy()
negative_lines["negative_value"] = -negative_lines["line_value"]

# Quantify the impact of the overlap and remaining exact duplicates.
overlap_gross_inflation = float(
    old_overlap.loc[
        (old_overlap["quantity"] > 0) & (old_overlap["unit_price"] > 0),
        "quantity",
    ].mul(
        old_overlap.loc[
            (old_overlap["quantity"] > 0) & (old_overlap["unit_price"] > 0),
            "unit_price",
        ]
    ).sum()
)

gross_before_exact_dedup = float(
    no_overlap.loc[
        (no_overlap["quantity"] > 0)
        & (no_overlap["unit_price"] > 0)
        & (~no_overlap["invoice"].str.upper().str.startswith("C", na=False)),
        "quantity",
    ].mul(
        no_overlap.loc[
            (no_overlap["quantity"] > 0)
            & (no_overlap["unit_price"] > 0)
            & (~no_overlap["invoice"].str.upper().str.startswith("C", na=False)),
            "unit_price",
        ]
    ).sum()
)

gross_sales = float(sales["line_value"].sum())
negative_value = float(negative_lines["negative_value"].sum())
net_transaction_value = float(
    analysis_df.loc[analysis_df["unit_price"] > 0, "line_value"].sum()
)
exact_dedup_sales_impact_pct = (
    (gross_before_exact_dedup - gross_sales) / gross_before_exact_dedup
)

cleaning_audit = pd.DataFrame(
    [
        ["Rows across both worksheets", sum(len(x) for x in prepared_sheets), "Starting point"],
        ["Exact cross-sheet overlap removed", exact_cross_sheet_overlap_rows, "Kept later worksheet"],
        ["Rows after overlap resolution", len(no_overlap), "Canonical union"],
        ["Remaining exact duplicates removed", remaining_exact_duplicate_rows, "Sensitivity impact measured"],
        ["Final analysis rows", len(analysis_df), "Used below"],
    ],
    columns=["step", "rows", "decision"],
)

display(cleaning_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Executive summary

# COMMAND ----------

monthly = (
    sales.groupby("month", as_index=False)
    .agg(
        gross_sales=("line_value", "sum"),
        orders=("invoice", "nunique"),
        identified_customers=("customer_id", "nunique"),
    )
)
monthly["average_order_value"] = monthly["gross_sales"] / monthly["orders"]

customer_summary = (
    sales.dropna(subset=["customer_id"])
    .groupby("customer_id", as_index=False)
    .agg(gross_sales=("line_value", "sum"), orders=("invoice", "nunique"))
    .sort_values("gross_sales", ascending=False)
)

customer_count = len(customer_summary)
top_1pct_count = max(1, math.ceil(customer_count * 0.01))
top_1pct_share = (
    customer_summary.head(top_1pct_count)["gross_sales"].sum()
    / customer_summary["gross_sales"].sum()
)
repeat_customer_share = float((customer_summary["orders"] >= 2).mean())
repeat_customer_revenue_share = float(
    customer_summary.loc[customer_summary["orders"] >= 2, "gross_sales"].sum()
    / customer_summary["gross_sales"].sum()
)

missing_customer_row_pct = float(analysis_df["customer_id"].isna().mean())
anonymous_sales_share = float(
    sales.loc[sales["customer_id"].isna(), "line_value"].sum() / gross_sales
)
negative_value_rate = negative_value / gross_sales
product_like_negative_value = float(
    negative_lines.loc[negative_lines["is_product_like_code"], "negative_value"].sum()
)
product_like_gross_sales = float(
    sales.loc[sales["is_product_like_code"], "line_value"].sum()
)
product_like_return_rate = product_like_negative_value / product_like_gross_sales
non_product_negative_share = 1 - (product_like_negative_value / negative_value)

country_summary = (
    sales.groupby("country", as_index=False)
    .agg(
        gross_sales=("line_value", "sum"),
        orders=("invoice", "nunique"),
        customers=("customer_id", "nunique"),
    )
    .sort_values("gross_sales", ascending=False)
)
country_summary["sales_share"] = country_summary["gross_sales"] / gross_sales
uk_share = float(
    country_summary.loc[country_summary["country"] == "United Kingdom", "sales_share"].iloc[0]
)

nov_2010 = float(monthly.loc[monthly["month"] == "2010-11-01", "gross_sales"].iloc[0])
nov_2011 = float(monthly.loc[monthly["month"] == "2011-11-01", "gross_sales"].iloc[0])

summary_html = f"""
<div style="font-family: Arial; line-height:1.45; max-width:1100px">
  <ul>
    <li><b>Data integrity is commercially material.</b> The worksheets contain
    <b>{exact_cross_sheet_overlap_rows:,}</b> identical overlapping rows. A naïve union would overstate gross sales by
    <b>£{overlap_gross_inflation:,.0f}</b>. After resolving the overlap, removing the remaining
    <b>{remaining_exact_duplicate_rows:,}</b> exact duplicates changes gross sales by only
    <b>{exact_dedup_sales_impact_pct:.2%}</b>.</li>
    <li><b>Demand is strongly seasonal.</b> November is the highest complete month in both years:
    <b>£{nov_2010/1_000_000:.2f}M</b> in 2010 and <b>£{nov_2011/1_000_000:.2f}M</b> in 2011.
    December 2011 is partial and should not be compared with full months.</li>
    <li><b>Revenue is concentrated in established customers.</b> The top 1% of identified customers generate
    <b>{top_1pct_share:.1%}</b> of identified-customer sales. Customers with at least two orders are
    <b>{repeat_customer_share:.1%}</b> of identified customers but contribute <b>{repeat_customer_revenue_share:.1%}</b>
    of identified-customer sales.</li>
    <li><b>A simple “return rate” is misleading.</b> Negative transaction value equals <b>{negative_value_rate:.1%}</b>
    of gross positive sales, but <b>{non_product_negative_share:.1%}</b> of that negative value comes from codes that do
    not look like merchandise (for example manual entries or fees). A product-like-code estimate is closer to
    <b>{product_like_return_rate:.1%}</b>, but still requires source-system validation.</li>
    <li><b>The business is geographically concentrated.</b> The United Kingdom contributes <b>{uk_share:.1%}</b>
    of gross sales. International performance may be promising, but revenue alone is insufficient to recommend expansion
    without margin, shipping cost, and acquisition data.</li>
  </ul>
</div>
"""

displayHTML(summary_html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data overview
# MAGIC
# MAGIC **Apparent grain:** one invoice–stock-code line item. An invoice can contain multiple product lines. The dataset does not provide a stable line-item identifier, so identical rows are treated as probable duplicates rather than provably duplicated source records.
# MAGIC
# MAGIC **Metric definitions used below:**
# MAGIC - **Gross sales:** positive quantity × positive unit price, excluding invoices beginning with `C`.
# MAGIC - **Negative transaction value:** absolute value of negative-quantity lines with positive prices.
# MAGIC - **Net transaction value:** signed quantity × price for positive-price rows.
# MAGIC - **Identified customer:** a row with non-null `customer_id`.

# COMMAND ----------

overview = pd.DataFrame(
    {
        "metric": [
            "Analysis date range",
            "Rows after cleaning",
            "Distinct invoices (all statuses)",
            "Gross-sale invoices",
            "Distinct stock codes",
            "Identified customers",
            "Countries",
            "Gross sales",
            "Negative transaction value",
            "Net transaction value",
        ],
        "value": [
            f"{analysis_df['invoice_date'].min():%Y-%m-%d} to {analysis_df['invoice_date'].max():%Y-%m-%d}",
            f"{len(analysis_df):,}",
            f"{analysis_df['invoice'].nunique():,}",
            f"{sales['invoice'].nunique():,}",
            f"{sales['stock_code'].nunique():,}",
            f"{sales['customer_id'].nunique():,}",
            f"{sales['country'].nunique():,}",
            f"£{gross_sales:,.0f}",
            f"£{negative_value:,.0f}",
            f"£{net_transaction_value:,.0f}",
        ],
    }
)

display(overview)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data quality checks

# COMMAND ----------

quality_checks = pd.DataFrame(
    [
        [
            "Cross-sheet date overlap",
            exact_cross_sheet_overlap_rows,
            exact_cross_sheet_overlap_rows / sum(len(x) for x in prepared_sheets),
            f"Removed older-sheet overlap; avoids £{overlap_gross_inflation:,.0f} gross overstatement",
        ],
        [
            "Remaining exact duplicate rows",
            remaining_exact_duplicate_rows,
            remaining_exact_duplicate_rows / len(no_overlap),
            f"Removed; gross-sales sensitivity is {exact_dedup_sales_impact_pct:.2%}",
        ],
        [
            "Missing customer ID",
            int(analysis_df["customer_id"].isna().sum()),
            missing_customer_row_pct,
            f"Retained for sales analysis; excluded from customer-level metrics ({anonymous_sales_share:.1%} of gross sales)",
        ],
        [
            "Missing description",
            int(analysis_df["description"].isna().sum()),
            float(analysis_df["description"].isna().mean()),
            "Retained; avoid relying on description alone as a product key",
        ],
        [
            "Zero unit price",
            int((analysis_df["unit_price"] == 0).sum()),
            float((analysis_df["unit_price"] == 0).mean()),
            "Excluded from value metrics; may represent samples, corrections, or missing prices",
        ],
        [
            "Negative unit price",
            int((analysis_df["unit_price"] < 0).sum()),
            float((analysis_df["unit_price"] < 0).mean()),
            "Excluded from sales/return metrics; records are administrative adjustments",
        ],
        [
            "Negative quantity",
            int((analysis_df["quantity"] < 0).sum()),
            float((analysis_df["quantity"] < 0).mean()),
            "Analyzed separately; not assumed to be pure product returns",
        ],
        [
            "Negative quantity without C-prefixed invoice",
            int(((analysis_df["quantity"] < 0) & (~analysis_df["is_cancel_invoice"])).sum()),
            float(((analysis_df["quantity"] < 0) & (~analysis_df["is_cancel_invoice"])).mean()),
            "Flagged: cancellation conventions are inconsistent",
        ],
    ],
    columns=["issue", "row_count", "row_share", "treatment / implication"],
)

quality_checks["row_share"] = quality_checks["row_share"].map(lambda x: f"{x:.2%}")
display(quality_checks)

# COMMAND ----------

# Review the most extreme line values instead of automatically deleting them.
extreme_lines = (
    analysis_df.assign(abs_line_value=analysis_df["line_value"].abs())
    .sort_values("abs_line_value", ascending=False)
    [[
        "invoice",
        "invoice_date",
        "stock_code",
        "description",
        "quantity",
        "unit_price",
        "line_value",
        "customer_id",
        "country",
    ]]
    .head(15)
)

display(extreme_lines)

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation:** the largest absolute values include both high-volume product lines and administrative/manual codes. I do not remove them solely because they are statistical outliers; their business meaning must be resolved first. This prevents a common EDA mistake—using an arbitrary z-score or IQR rule on heavy-tailed transaction data and deleting valid wholesale orders.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exploratory analysis
# MAGIC
# MAGIC ### 4.1 Sales and time trends

# COMMAND ----------

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(monthly["month"], monthly["gross_sales"], marker="o")
ax.set_title("Monthly gross sales")
ax.set_xlabel("")
ax.set_ylabel("Gross sales")
ax.yaxis.set_major_formatter(mtick.StrMethodFormatter("£{x:,.0f}"))
ax.grid(axis="y", alpha=0.25)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

monthly_display = monthly.copy()
monthly_display["gross_sales"] = monthly_display["gross_sales"].map(lambda x: f"£{x:,.0f}")
monthly_display["average_order_value"] = monthly_display["average_order_value"].map(lambda x: f"£{x:,.0f}")
display(monthly_display)

# COMMAND ----------

# Compare the September–November sales concentration in the two complete Jan–Nov periods.
seasonality = []
for year in [2010, 2011]:
    year_months = monthly[
        (monthly["month"].dt.year == year) & (monthly["month"].dt.month <= 11)
    ]
    sep_nov = year_months[year_months["month"].dt.month.isin([9, 10, 11])]["gross_sales"].sum()
    jan_nov = year_months["gross_sales"].sum()
    seasonality.append(
        {
            "year": year,
            "Jan-Nov gross sales": jan_nov,
            "Sep-Nov gross sales": sep_nov,
            "Sep-Nov share": sep_nov / jan_nov,
        }
    )

seasonality = pd.DataFrame(seasonality)
seasonality["Jan-Nov gross sales"] = seasonality["Jan-Nov gross sales"].map(lambda x: f"£{x:,.0f}")
seasonality["Sep-Nov gross sales"] = seasonality["Sep-Nov gross sales"].map(lambda x: f"£{x:,.0f}")
seasonality["Sep-Nov share"] = seasonality["Sep-Nov share"].map(lambda x: f"{x:.1%}")
display(seasonality)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Customer concentration and repeat purchasing
# MAGIC
# MAGIC Customer analysis uses only rows with a known customer ID. Anonymous transactions remain in sales totals but cannot be attributed to a customer.

# COMMAND ----------

concentration_levels = [0.01, 0.05, 0.10, 0.20]
concentration_rows = []

for level in concentration_levels:
    n_customers = max(1, math.ceil(customer_count * level))
    revenue_share = (
        customer_summary.head(n_customers)["gross_sales"].sum()
        / customer_summary["gross_sales"].sum()
    )
    concentration_rows.append(
        {
            "customer segment": f"Top {level:.0%}",
            "customers": n_customers,
            "identified-customer sales share": revenue_share,
        }
    )

concentration = pd.DataFrame(concentration_rows)

fig, ax = plt.subplots(figsize=(8, 4.5))
ax.bar(concentration["customer segment"], concentration["identified-customer sales share"])
ax.set_title("Revenue concentration among identified customers")
ax.set_ylabel("Share of identified-customer gross sales")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax.grid(axis="y", alpha=0.25)
plt.tight_layout()
plt.show()

concentration_display = concentration.copy()
concentration_display["identified-customer sales share"] = concentration_display[
    "identified-customer sales share"
].map(lambda x: f"{x:.1%}")
display(concentration_display)

# COMMAND ----------

repeat_table = pd.DataFrame(
    [
        ["One order", int((customer_summary["orders"] == 1).sum()), float((customer_summary["orders"] == 1).mean()),
         float(customer_summary.loc[customer_summary["orders"] == 1, "gross_sales"].sum() / customer_summary["gross_sales"].sum())],
        ["Two or more orders", int((customer_summary["orders"] >= 2).sum()), repeat_customer_share,
         repeat_customer_revenue_share],
    ],
    columns=["segment", "customers", "customer_share", "identified_revenue_share"],
)
repeat_table["customer_share"] = repeat_table["customer_share"].map(lambda x: f"{x:.1%}")
repeat_table["identified_revenue_share"] = repeat_table["identified_revenue_share"].map(lambda x: f"{x:.1%}")
display(repeat_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Countries

# COMMAND ----------

top_countries = country_summary.head(10).copy()

fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(top_countries["country"][::-1], top_countries["gross_sales"][::-1])
ax.set_title("Top countries by gross sales")
ax.set_xlabel("Gross sales")
ax.xaxis.set_major_formatter(mtick.StrMethodFormatter("£{x:,.0f}"))
ax.grid(axis="x", alpha=0.25)
plt.tight_layout()
plt.show()

country_display = top_countries.copy()
country_display["gross_sales"] = country_display["gross_sales"].map(lambda x: f"£{x:,.0f}")
country_display["sales_share"] = country_display["sales_share"].map(lambda x: f"{x:.1%}")
display(country_display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Products and negative transactions
# MAGIC
# MAGIC For product ranking, I use codes beginning with a digit as a transparent heuristic for merchandise-like SKUs. This excludes obvious administrative codes such as `M`, `AMAZONFEE`, and `BANK CHARGES`, but it is not a substitute for a product master.

# COMMAND ----------

# Choose the most frequent description per stock code, then rank product-like codes by sales.
description_lookup = (
    sales.dropna(subset=["description"])
    .groupby(["stock_code", "description"], as_index=False)
    .size()
    .sort_values(["stock_code", "size"], ascending=[True, False])
    .drop_duplicates("stock_code")
    .set_index("stock_code")["description"]
)

product_summary = (
    sales[sales["is_product_like_code"]]
    .groupby("stock_code", as_index=False)
    .agg(
        gross_sales=("line_value", "sum"),
        units=("quantity", "sum"),
        orders=("invoice", "nunique"),
    )
    .sort_values("gross_sales", ascending=False)
)
product_summary["description"] = product_summary["stock_code"].map(description_lookup)

top_products = product_summary.head(10)[
    ["stock_code", "description", "gross_sales", "units", "orders"]
].copy()
top_products["gross_sales"] = top_products["gross_sales"].map(lambda x: f"£{x:,.0f}")
display(top_products)

# COMMAND ----------

negative_code_summary = (
    negative_lines.assign(
        code_group=np.where(
            negative_lines["is_product_like_code"],
            "Product-like stock code",
            "Non-product-like / administrative code",
        )
    )
    .groupby("code_group", as_index=False)
    .agg(negative_value=("negative_value", "sum"), rows=("invoice", "size"))
)
negative_code_summary["negative_value_share"] = (
    negative_code_summary["negative_value"] / negative_code_summary["negative_value"].sum()
)

negative_display = negative_code_summary.copy()
negative_display["negative_value"] = negative_display["negative_value"].map(lambda x: f"£{x:,.0f}")
negative_display["negative_value_share"] = negative_display["negative_value_share"].map(lambda x: f"{x:.1%}")
display(negative_display)

largest_negative_lines = negative_lines.sort_values("negative_value", ascending=False)[
    [
        "invoice",
        "invoice_date",
        "stock_code",
        "description",
        "quantity",
        "unit_price",
        "negative_value",
        "customer_id",
        "country",
    ]
].head(12)
display(largest_negative_lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Findings and hypotheses

# COMMAND ----------

# Recalculate a few values used in the narrative.
seasonality_raw = []
for year in [2010, 2011]:
    year_months = monthly[(monthly["month"].dt.year == year) & (monthly["month"].dt.month <= 11)]
    sep_nov = year_months[year_months["month"].dt.month.isin([9, 10, 11])]["gross_sales"].sum()
    seasonality_raw.append(sep_nov / year_months["gross_sales"].sum())

findings = pd.DataFrame(
    [
        {
            "finding / hypothesis": "The source workbook can materially double count sales unless worksheet overlap is handled.",
            "evidence": f"{exact_cross_sheet_overlap_rows:,} identical rows overlap and would add £{overlap_gross_inflation:,.0f} to gross sales.",
            "why it matters": "Every downstream KPI, ranking, and trend would be biased.",
            "confidence": "High",
        },
        {
            "finding / hypothesis": "Demand ramps sharply before the holiday period, creating a recurring Q4 planning need.",
            "evidence": f"Sep-Nov contributes {seasonality_raw[0]:.1%} of Jan-Nov 2010 sales and {seasonality_raw[1]:.1%} in 2011; November is the peak complete month in both years.",
            "why it matters": "Inventory, fulfillment capacity, and retention campaigns should be prepared before September.",
            "confidence": "High for seasonality; medium for the operational recommendation",
        },
        {
            "finding / hypothesis": "A relatively small group of established customers drives most attributable revenue.",
            "evidence": f"Top 1% generate {top_1pct_share:.1%}; customers with 2+ orders generate {repeat_customer_revenue_share:.1%} of identified-customer sales.",
            "why it matters": "Retention and key-account protection may have higher near-term value than undifferentiated acquisition.",
            "confidence": "High for concentration; medium for the retention hypothesis",
        },
        {
            "finding / hypothesis": "The raw negative-value ratio overstates product returns because administrative transactions are mixed in.",
            "evidence": f"Negative value is {negative_value_rate:.1%} of gross sales, but {non_product_negative_share:.1%} comes from non-product-like codes; product-like estimate is {product_like_return_rate:.1%}.",
            "why it matters": "A misleading return KPI could trigger the wrong product-quality or operations response.",
            "confidence": "Medium because product classification is heuristic",
        },
        {
            "finding / hypothesis": "International revenue is meaningful but the business remains UK-dependent.",
            "evidence": f"The UK contributes {uk_share:.1%} of gross sales.",
            "why it matters": "Country diversification could reduce concentration, but must be evaluated with margin and logistics data.",
            "confidence": "High for revenue mix; low-to-medium for expansion attractiveness",
        },
    ]
)

display(findings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Caveats
# MAGIC
# MAGIC 1. **No line-item key:** exact-row deduplication is a reasoned assumption. Identical lines could theoretically be legitimate repeated entries. The small gross-sales sensitivity after overlap resolution reduces—but does not eliminate—this concern.
# MAGIC 2. **Customer IDs are incomplete:** approximately one-quarter of rows lack a customer ID, and anonymous sales cannot be used in retention or concentration analysis.
# MAGIC 3. **Returns are not cleanly modeled:** negative quantities, cancellation invoice prefixes, fees, manual entries, and adjustments are mixed together. A true product return rate requires an explicit transaction-type field and a link to the original sale.
# MAGIC 4. **December 2011 is partial:** the data ends on December 9, 2011, so it must not be compared with complete months.
# MAGIC 5. **Revenue is not profit:** the dataset has no cost of goods, shipping cost, discount, promotion, acquisition cost, inventory, or margin data.
# MAGIC 6. **Observational analysis:** customer concentration and repeat behavior are descriptive. They do not prove that a retention intervention would cause incremental revenue.
# MAGIC 7. **Product-code heuristic:** codes beginning with a digit are treated as product-like only for diagnostic purposes; a governed product master is needed for production reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Recommended next steps
# MAGIC
# MAGIC 1. **Fix source lineage and metric definitions:** obtain a unique line-item ID, a transaction-type field, and a documented rule for overlapping extracts.
# MAGIC 2. **Build a product/adjustment taxonomy:** separate merchandise, shipping, discounts, fees, write-offs, and manual corrections before publishing product or return KPIs.
# MAGIC 3. **Reconcile returns to original purchases:** calculate return rate by product, customer, reason, and days since purchase; investigate the largest negative transactions first.
# MAGIC 4. **Create customer cohorts:** measure repeat rate and revenue retention by first-purchase month, country, and acquisition source rather than using only full-period aggregates.
# MAGIC 5. **Prepare for Q4:** combine product demand, inventory, lead times, and fulfillment capacity to test whether September–November peaks create stockout or service-level risk.
# MAGIC 6. **Evaluate international markets on contribution margin:** add shipping, duties, returns, and customer acquisition cost before recommending country expansion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Use of GenAI
# MAGIC
# MAGIC I used GenAI to accelerate notebook structure, suggest validation checks, refactor repetitive pandas code, and challenge whether apparent “returns” were actually customer product returns. I accepted suggestions only after reproducing the calculations directly in the notebook.
# MAGIC
# MAGIC I modified or rejected GenAI output when it:
# MAGIC - proposed blindly concatenating the two worksheets;
# MAGIC - treated every negative quantity as a normal product return;
# MAGIC - compared partial December 2011 with complete months;
# MAGIC - recommended removing large wholesale transactions solely because they were statistical outliers; or
# MAGIC - implied causal business recommendations from descriptive patterns.
# MAGIC
# MAGIC I validated the final work by checking worksheet date ranges, row counts at each cleaning step, duplicate impact on gross sales, sign conventions, invoice-prefix consistency, metric denominators, and the largest absolute-value records. All written figures are generated from notebook variables to reduce transcription errors.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Final decision log
# MAGIC
# MAGIC - **Kept the notebook focused:** four core analytical views plus data-quality diagnostics.
# MAGIC - **Used pandas inside Databricks:** pragmatic for an Excel source of this size and the 2–3 hour constraint.
# MAGIC - **Avoided a model:** the brief asks for EDA and analytical judgment, not prediction.
# MAGIC - **Prioritized reproducibility:** all executive-summary numbers are calculated from the same cleaned dataframe used in the charts.
