<div align="center">

# 🏗️ Week 7 — Delta Lake Data Engineering Pipeline

### Production-Grade ETL with PySpark, Delta Lake & MERGE Operations

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python&logoColor=white)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-00ADD8?logo=delta&logoColor=white)](https://delta.io)
[![Databricks](https://img.shields.io/badge/Databricks-Compatible-FF3621?logo=databricks&logoColor=white)](https://databricks.com)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

*A comprehensive data engineering project demonstrating real-world Delta Lake operations on retail analytics data — built to production standards.*

---

</div>

## 📋 Table of Contents

| # | Section | Description |
|---|---------|-------------|
| 1 | [Project Overview](#-project-overview) | What this project does |
| 2 | [Objectives](#-objectives) | Learning and business goals |
| 3 | [Architecture](#-architecture) | System design & data flow |
| 4 | [Dataset](#-dataset) | Source data description |
| 5 | [Technology Stack](#-technology-stack) | Tools and frameworks |
| 6 | [Folder Structure](#-folder-structure) | Repository layout |
| 7 | [Key Features](#-key-features) | Pipeline capabilities |
| 8 | [How to Run](#-how-to-run) | Setup & execution steps |
| 9 | [Pipeline Workflow](#-pipeline-workflow) | End-to-end data flow |
| 10 | [MERGE Deep Dive](#-delta-lake-merge-deep-dive) | Core UPSERT logic |
| 11 | [Visualizations](#-visualizations) | Business analytics charts |
| 12 | [Learning Outcomes](#-learning-outcomes) | Skills demonstrated |
| 13 | [Future Scope](#-future-scope) | Roadmap & enhancements |

---

## 🎯 Project Overview

This project implements an **end-to-end data engineering pipeline** that processes the Superstore retail dataset through a complete ETL lifecycle using **Apache Spark (PySpark)** and **Delta Lake**. The pipeline demonstrates production-grade patterns used at companies like **Databricks, Microsoft, and Amazon** for building reliable, scalable data platforms.

### Why This Matters

In modern data platforms, raw CSV/JSON ingestion is insufficient. Production systems require:
- **ACID transactions** to prevent data corruption during concurrent writes
- **Schema enforcement** to reject malformed records at ingestion time
- **MERGE/UPSERT** capabilities for incremental data loads (CDC patterns)
- **Time travel** for audit compliance and debugging data issues

This project demonstrates all of the above using the open-source Delta Lake format.

---

## 🎯 Objectives

| Objective | Status |
|-----------|--------|
| Build a complete ETL pipeline with PySpark | ✅ |
| Implement Delta Lake table management | ✅ |
| Demonstrate MERGE INTO (UPSERT) operations | ✅ |
| Perform exploratory data analysis (EDA) | ✅ |
| Engineer business-relevant features | ✅ |
| Generate production-quality visualizations | ✅ |
| Export cleaned datasets in multiple formats | ✅ |
| Document with professional-grade reporting | ✅ |

---

## 🏛️ Architecture

```text
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Raw CSV     │────▶│  PySpark     │────▶│  Delta Lake  │────▶│  Analytics   │
│  Ingestion   │     │  Processing  │     │  Storage     │     │  & Reports   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │                     │
  Superstore.csv     • Schema Inference    • ACID Writes         • Visualizations
  (10,800 rows)      • Data Cleaning       • MERGE/UPSERT       • Business KPIs
                     • Feature Eng.        • Transaction Log    • CSV/Parquet
                     • Transformations     • Time Travel        • Reports
```

### Data Flow

```
Raw CSV ─► Ingest ─► Validate ─► Clean ─► Transform ─► Delta Write ─► MERGE ─► Export
                                                              │
                                                        _delta_log/
                                                     (Transaction Log)
```

---

## 📊 Dataset

| Property | Value |
|----------|-------|
| **Source** | [Kaggle — Superstore Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) |
| **Records** | 10,800 rows |
| **Features** | 21 columns |
| **Domain** | US Retail / E-Commerce |
| **Time Range** | 2014–2017 |
| **Geography** | United States (all 50 states) |

### Key Columns

| Column | Type | Business Significance |
|--------|------|----------------------|
| `Order ID` | String | Unique transaction identifier |
| `Order Date` | Date | Temporal analysis & trends |
| `Ship Mode` | String | Fulfillment SLA tracking |
| `Segment` | String | Customer segmentation (B2C/B2B) |
| `Category` | String | Product taxonomy (3 categories) |
| `Sales` | Double | Revenue metric |
| `Profit` | Double | Profitability KPI |
| `Discount` | Double | Pricing strategy analysis |
| `Quantity` | Integer | Volume metric |

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Processing** | Apache Spark (PySpark) 3.5.1 | Distributed data processing |
| **Storage** | Delta Lake 3.2.0 | ACID-compliant lakehouse storage |
| **Analysis** | Pandas, NumPy | In-memory analytics |
| **Visualization** | Matplotlib, Seaborn | Business intelligence charts |
| **Environment** | Jupyter Notebook | Interactive development |
| **Language** | Python 3.9+ | Pipeline orchestration |

---

## 📁 Folder Structure

```
Week-7/
├── 📓 Week7_DeltaLake_Pipeline.ipynb    # Main notebook (full pipeline)
├── 🐍 week7_pipeline.py                  # Executable Python script
├── 📋 README.md                          # This file
├── 📦 requirements.txt                   # Python dependencies
├── 📊 original_superstore.csv            # Raw dataset (10,800 rows)
├── 📊 cleaned_superstore.csv             # Cleaned + engineered dataset
├── 📊 updated_superstore.csv             # Post-transformation dataset
├── 📊 merged_output.csv                  # Post-MERGE output
└── 📁 screenshots/                       # Visualization outputs
    ├── 01_sales_by_region.png
    ├── 02_sales_by_category.png
    ├── 03_profit_distribution.png
    ├── 04_top_subcategories.png
    ├── 05_discount_vs_profit.png
    ├── 06_monthly_sales_trend.png
    ├── 07_ship_mode_dist.png
    └── 08_top_customers.png
```

---

## ✨ Key Features

### 🔹 Data Ingestion & Validation
- Schema-inferred CSV ingestion via `spark.read.csv()`
- Null detection, duplicate identification, data type validation

### 🔹 Production Data Cleaning
- Duplicate removal, null handling, column standardization
- Date parsing, type casting, invalid record filtering
- Before vs. After validation at every step

### 🔹 Feature Engineering
- **9 derived business columns**: profit_margin, revenue_category, discount_bucket, shipping_delay, order_month, order_year, is_weekend_order, is_high_value, business_segment
- Each feature justified with business rationale

### 🔹 Delta Lake Operations
- Write data as Delta tables with ACID guarantees
- **MERGE INTO** with `WHEN MATCHED UPDATE` and `WHEN NOT MATCHED INSERT`
- Transaction log inspection via `DeltaTable.history()`

### 🔹 Business Analytics
- 8 professional visualizations covering sales, profit, customers, regions, categories, trends, and discount analysis

---

## 🚀 How to Run

### Prerequisites
- Python 3.9+
- Java 8 or 11 (required by Spark)

### Setup

```bash
# 1. Clone the repository
git clone <repo-url>
cd Week-6

# 2. Install dependencies
pip install -r requirements.txt

# 3. Option A: Run the Python script
python week7_pipeline.py

# 4. Option B: Open the Jupyter notebook
jupyter notebook Week7_DeltaLake_Pipeline.ipynb
```

### Environment Variables (Optional)
```bash
export JAVA_HOME=/path/to/java
export SPARK_HOME=/path/to/spark
```

---

## 🔄 Pipeline Workflow

```
Stage 1 │ Initialize SparkSession with Delta Lake extensions
        │
Stage 2 │ Ingest original_superstore.csv (10,800 × 21)
        │
Stage 3 │ Exploratory Data Analysis
        │   ├── Schema inspection
        │   ├── Null & duplicate detection
        │   ├── Statistical summaries
        │   └── Business distribution analysis
        │
Stage 4 │ Data Cleaning Pipeline
        │   ├── Remove duplicates
        │   ├── Handle nulls
        │   ├── Rename to snake_case
        │   ├── Standardize strings
        │   ├── Parse dates
        │   └── Filter invalid records
        │
Stage 5 │ Feature Engineering (9 new columns)
        │
Stage 6 │ Delta Lake Write (ACID transaction)
        │
Stage 7 │ MERGE Demonstration (UPSERT)
        │   ├── Base table: 500 rows
        │   ├── Incoming: 50 updates + 50 inserts
        │   └── Result: ~550 rows (verified)
        │
Stage 8 │ Visualization Generation (8 charts)
        │
Stage 9 │ Export (CSV, Delta, Parquet)
```

---

## 🔀 Delta Lake MERGE Deep Dive

### The Problem
Traditional data lakes (CSV/Parquet) do not support **UPDATE** or **DELETE** operations. Every change requires a full table rewrite. This is:
- ❌ Expensive (rewriting TBs of data for a single record change)
- ❌ Error-prone (partial writes corrupt data)
- ❌ Not ACID-compliant

### The Solution: Delta Lake MERGE
```python
delta_table.alias("target").merge(
    incoming_df.alias("source"),
    "target.row_id = source.row_id"
).whenMatchedUpdate(set={
    "sales": "source.sales",
    "profit": "source.profit"
}).whenNotMatchedInsertAll().execute()
```

### How It Works
| Clause | Action | Use Case |
|--------|--------|----------|
| `WHEN MATCHED` | UPDATE | Correct existing records |
| `WHEN NOT MATCHED` | INSERT | Add new records |
| `WHEN NOT MATCHED BY SOURCE` | DELETE | Remove stale records |

### Production Use Cases
- **CDC (Change Data Capture)**: Sync operational DB changes to the lakehouse
- **SCD Type 2**: Track historical dimension changes
- **Deduplication**: Prevent duplicate records in append-heavy workloads
- **Incremental ETL**: Only process changed/new records

---

## 📈 Visualizations

| Chart | Insight |
|-------|---------|
| Sales by Region | West region leads with highest revenue |
| Sales by Category | Technology generates the most revenue |
| Profit Distribution | Most orders cluster around small profit margins |
| Top Sub-Categories | Phones, Chairs, and Storage dominate sales |
| Discount vs Profit | Heavy discounts (>40%) correlate with losses |
| Monthly Sales Trend | Strong Q4 seasonality (holiday sales) |
| Ship Mode Distribution | Standard Class is the most common (60%+) |
| Top Customers | Top 10 customers contribute disproportionate revenue |

---

## 📚 Learning Outcomes

- ✅ Built production ETL pipelines with PySpark
- ✅ Understood Delta Lake vs CSV vs Parquet tradeoffs
- ✅ Implemented MERGE/UPSERT for incremental data loads
- ✅ Applied schema enforcement and ACID transactions
- ✅ Created business-relevant derived features
- ✅ Generated actionable analytics from retail data
- ✅ Followed data engineering best practices (logging, validation, documentation)

---

## 🔮 Future Scope

| Enhancement | Description |
|-------------|-------------|
| **Structured Streaming** | Real-time ingestion with `foreachBatch` + MERGE |
| **Databricks Deployment** | Migrate to Databricks Workflows for orchestration |
| **Data Quality Framework** | Add Great Expectations for automated testing |
| **Medallion Architecture** | Bronze → Silver → Gold layered lakehouse |
| **Orchestration** | Apache Airflow DAGs for scheduling |
| **Monitoring** | Datadog/Grafana dashboards for pipeline health |

---

## 📜 References

| Resource | Link |
|----------|------|
| Delta Lake MERGE Documentation | [Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/delta/merge) |
| Superstore Dataset | [Kaggle](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) |
| PySpark Documentation | [Apache Spark](https://spark.apache.org/docs/latest/api/python/) |
| Delta Lake Official | [delta.io](https://delta.io) |

---

<div align="center">

**Built with ❤️ for the Celebal Technologies Data Engineering Internship**

*Week 7 — Delta Lake & Advanced Data Engineering*

*Himanshu Batra - CEI 2026 - Data Engineer Intern - DIT University*
</div>
