# 🚗 RideSharing Driver Performance & Revenue Analytics Platform

[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze--Silver--Gold)-green.svg)]()
[![Tests](https://img.shields.io/badge/PyTest-20%2F20%20Passed-brightgreen.svg)]()
[![License](https://img.shields.io/badge/License-MIT-purple.svg)]()

An enterprise-grade, end-to-end Big Data Engineering pipeline built with **PySpark** using the **Medallion Architecture (Bronze -> Silver -> Gold)**. The platform ingests multi-source ridesharing transactional datasets, executes automated data quality validations, performs complex window function transformations, applies Spark SQL query optimizations, and outputs executive-ready business analytics and publication-quality visualizations.

---

## 📐 Pipeline Architecture (Medallion Pattern)

```
                            ┌────────────────────────────────────────┐
                            │    Raw Data Sources (CSV Files)         │
                            │  • drivers.csv                         │
                            │  • trips.csv                           │
                            │  • trip_logs.csv                       │
                            └───────────────────┬────────────────────┘
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 🥉 BRONZE LAYER (Ingestion & Normalization)                                            │
│ • Schema Enforcement & Strict Type Casting                                             │
│ • Clean Column Naming (Snake_case) & Null Truncation                                   │
│ • Data Ingestion into Parquet Storage Format (`data/bronze/`)                          │
└───────────────────────────────────────────────┬────────────────────────────────────────┘
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 🥈 SILVER LAYER (Cleaning, Data Quality & Feature Enrichment)                          │
│ • Automated Data Quality Framework (29 Checks: Nulls, Duplicates, Range, Ref Integrity)│
│ • Trip Duration & Delay Calculation (`end_time` - `start_time`)                        │
│ • Broadcast Join: `trips` + `drivers` + `trip_logs`                                    │
│ • Driver Rating Tier & Fare/KM Feature Engineering                                     │
│ • Data Quality Event Logging & Quarantine Management                                   │
└───────────────────────────────────────────────┬────────────────────────────────────────┘
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 🥇 GOLD LAYER (Executive Analytics & Business KPIs)                                    │
│ • 11 Analytical Parquet Tables (`data/gold/`)                                          │
│ • PySpark Window Functions: `RANK`, `DENSE_RANK`, `ROW_NUMBER`, `NTILE`, `LAG`, `LEAD` │
│ • Driver Efficiency Scoring & Segmentation (`Champion`, `Performer`, `At-Risk`)       │
│ • Rolling 3-Day Revenue Trends & Peak Hour Heatmap Analysis                            │
└───────────────────────────────────────────────┬────────────────────────────────────────┘
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 📊 REPORTING & VISUALIZATION ENGINE                                                    │
│ • 10 Dark-Themed Matplotlib / Seaborn Visualizations (`reports/charts/`)              │
│ • Automated Executive Markdown Business Report (`reports/business_report.md`)         │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🌟 Key Features & Technical Highlights

1. **End-to-End Medallion Lakehouse Architecture**:
   - **Bronze Layer**: Raw CSV data ingested into optimized Parquet tables with standardized schemas.
   - **Silver Layer**: Data cleaning, handling missing values, broadcasting driver lookup metadata, and generating enriched trip metrics.
   - **Gold Layer**: 11 business-facing analytical tables aggregating driver performance, city revenue, trip cancellations, delay distributions, driver segmentation, and executive KPIs.

2. **Automated Data Quality (DQ) Gateways**:
   - 29 production checks including **Null Percentage Monitoring**, **Primary/Foreign Key Uniqueness**, **Numeric Range Validation**, **Referential Integrity**, and **Status Consistency Verification**.

3. **Advanced PySpark Window Functions**:
   - `ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()` for global and city-level driver efficiency leaderboards.
   - `NTILE(4)` for driver quartile performance breakdown.
   - `LAG()` & `LEAD()` for day-over-day revenue delta and percentage growth analysis.
   - Rolling window frames (`rowsBetween(-2, 0)`) for 3-day moving average revenue calculations.

4. **PySpark Performance Optimizations Showcase**:
   - **Adaptive Query Execution (AQE)** enabled for dynamic shuffle partition coalescing.
   - **Broadcast Hash Join** strategy employed on dimension lookup table (`drivers`), eliminating expensive network shuffles.
   - **Kryo Serialization** configured for memory-efficient task execution.

5. **Cross-Platform & Windows Compatibility**:
   - Built-in dynamic `HADOOP_HOME` environment setup utility handling `winutils.exe` automatically for Windows development environments.

6. **Comprehensive Automated Test Suite**:
   - 20 PyTest unit tests validating data quality functions, transformations, null checks, window function math, and edge cases.

---

## 📁 Repository Structure

```text
RideSharing-Analytics/
│
├── config/
│   └── pipeline_config.yaml     # Single source of truth configuration
│
├── data/
│   ├── raw/                     # Original CSV datasets (drivers, trips, trip_logs)
│   ├── bronze/                  # Ingested Parquet tables
│   ├── silver/                  # Cleaned & Enriched Parquet datasets
│   └── gold/                    # 11 Business KPI Parquet tables
│
├── hadoop_home/
│   └── bin/                     # Windows native libraries (winutils.exe, hadoop.dll)
│
├── logs/
│   └── pipeline.log             # Structured runtime execution log
│
├── notebooks/
│   └── pipeline_notebook.py     # Interactive notebook demonstration pipeline
│
├── reports/
│   ├── business_report.md       # Auto-generated executive summary report
│   └── charts/                  # 10 Publication-ready dark-mode charts (.png)
│
├── sql/
│   └── analytics_queries.sql    # 10 PySpark SQL reference queries
│
├── src/
│   ├── analytics/
│   │   └── gold_analytics.py    # Gold analytics engine (Window functions & KPIs)
│   ├── ingestion/
│   │   └── bronze_ingestion.py  # Bronze ingestion engine
│   ├── transformation/
│   │   ├── silver_transformation.py # Silver enrichment engine
│   │   └── optimization_showcase.py # PySpark optimization showcase module
│   ├── utils/
│   │   ├── config_loader.py    # Config YAML parser
│   │   ├── logger.py           # Standardized logger
│   │   ├── report_generator.py # Matplotlib/Seaborn visualization engine
│   │   └── spark_session.py    # SparkSession factory & Windows env setup
│   └── validation/
│       └── data_validator.py   # Data Quality check engine
│
├── tests/
│   └── test_pipeline.py         # PyTest test suite (20 test cases)
│
├── requirements.txt             # Python dependencies
├── run_pipeline.py              # Master pipeline orchestrator entry point
└── README.md                    # System documentation
```

---

## 🚀 Getting Started

### 1. Prerequisites & Dependencies
Ensure Python 3.9+ and Java 8/11/17 (JDK) are installed on your machine.

Install required Python packages:
```bash
pip install -r requirements.txt
```

### 2. Running the Full Data Pipeline
Execute the end-to-end orchestrator from raw CSV ingestion to final Gold KPIs and report generation:

```bash
python run_pipeline.py
```

### 3. Stage-by-Stage Execution
Run specific layers independently using CLI options:

```bash
# Ingest raw CSVs to Bronze Parquet
python run_pipeline.py --stage bronze

# Perform Silver enrichment and Data Quality checks
python run_pipeline.py --stage silver

# Execute Gold analytics and window computations
python run_pipeline.py --stage gold

# Run standalone Data Quality validations
python run_pipeline.py --stage validate
```

### 4. Running the Unit Test Suite
Execute the 20 PyTest unit tests:

```bash
python -m pytest tests/
```

---

## 📊 Business Analytics & Executive Insights

### Key Metrics Generated
- **Total Revenue**: ₹41,120.00
- **Total Trips**: 150 (Completed: 61 | Cancelled: 89)
- **Overall Completion Rate**: 40.7% (Cancellation Rate: 59.3%)
- **Average Fare per Trip**: ₹674.10
- **Average Revenue per KM**: ₹44.53/km
- **Average Trip Duration**: 33.7 mins
- **Average Trip Delay**: 25.1 mins

### Strategic Recommendations
1. **Cancellation Rate Mitigation**: Current cancellation rate (~59%) exceeds industry benchmarks (20–30%). Implementing driver commitment scoring and confirmation nudges is recommended.
2. **Peak Demand Incentivization**: Align driver supply during Morning Rush (07:00–10:00) and Evening Rush (17:00–20:00) with dynamic per-trip incentives.
3. **Driver Coaching Program**: 28% of drivers fall into the *At-Risk* segment (`driver_efficiency_score` < 0.45). Focused intervention on completion rates will directly boost top-line revenue.

---

## 📜 License & Acknowledgments
Built for the **RideSharing Driver Performance & Revenue Analytics Platform** initiative. Developed with PySpark, Python, and Open Source Data Engineering standards.
