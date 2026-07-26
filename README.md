# 🚀 Celebal Excellence Internship (CEI) 2026

Welcome to my **Celebal Excellence Internship (CEI) 2026** repository.

This repository documents my learning journey throughout the internship at **Celebal Technologies**, where I complete weekly assignments focused on Python, SQL, Azure Cloud, Data Engineering, and Data Analytics. Each week's work demonstrates practical implementation of concepts using real-world datasets and industry-oriented problem statements.

---

# 👨‍💻 Profile

| **Name** | Himanshu Batra |
|-----------|----------------|
| **Program** | Celebal Excellence Internship (CEI) 2026 |
| **Domain** | Data Engineering |
| **University** | DIT University, Dehradun |

---

# 📁 Repository Overview

```
CEI-2026/
│
├── Week-1/
├── Week-2/
├── Week-3/
├── Week-4/
├── Week-5/
├── Week-6/
└── README.md
```

Each weekly folder contains notebooks, SQL scripts, Azure resources, reports, datasets, screenshots, and supporting documentation for the corresponding assignment.

---

# 📌 Internship Progress

| Week | Assignment | Technologies | Status |
|------|------------|--------------|--------|
| Week 1 | Data Exploration & Cleaning using Pandas | Python, Pandas | ✅ Completed |
| Week 2 | SQL-Based Sales Data Analysis | SQL, SQLite | ✅ Completed |
| Week 3 | Advanced SQL (Subqueries, CTEs & Window Functions) | SQL, SQLite | ✅ Completed |
| Week 4 | Azure Cloud Fundamentals & Azure Data Factory Pipeline | Azure, Azure Blob Storage, Azure Data Factory, IAM | ✅ Completed |
| Week 5 | Data Cleaning, Transformation & Aggregation using PySpark | PySpark, Apache Spark, Google Colab | ✅ Completed |
| Week 6 | Apache Spark & PySpark — Retail Analytics Pipeline | PySpark, Apache Spark, Spark SQL | ✅ Completed |

---

# 📖 Weekly Summary

---

## 📊 Week 1 — Data Exploration & Cleaning with Pandas

### Objective

Perform data cleaning and exploratory analysis on a shopping dataset using Python and Pandas.

### Highlights

- Imported and explored CSV datasets
- Examined dataset structure and data types
- Handled missing values
- Removed duplicate records
- Applied filtering and transformations
- Created derived features
- Exported cleaned dataset

### Deliverables

- `analysis_shopping.ipynb`
- `cleaned_shopping_dataset.csv`

**Tools & Libraries**

- Python
- Pandas
- NumPy
- Jupyter Notebook

---

## 🗄️ Week 2 — SQL Sales Data Analysis

### Objective

Analyze retail sales data by applying SQL queries to generate business insights.

### Highlights

- Imported Superstore dataset into SQLite
- Performed filtering using `WHERE`
- Applied aggregation using `GROUP BY`
- Generated sales reports
- Identified top-performing customers and products
- Analyzed regional and monthly sales
- Performed duplicate detection and validation

### Deliverables

- `superstore_analysis.ipynb`
- `sql_analysis.sql`

**Tools & Technologies**

- SQL
- SQLite
- Google Colab
- Pandas

---

## ⚡ Week 3 — Advanced SQL Analytics

### Objective

Apply advanced SQL concepts to solve customer-centric business problems.

### Highlights

- Created normalized customer, order, and product tables
- Used Subqueries for analytical reporting
- Implemented Common Table Expressions (CTEs)
- Applied Window Functions (`RANK()`, `ROW_NUMBER()`)
- Ranked customers by revenue
- Identified top and low-performing customers
- Calculated highest order values
- Combined JOINs, CTEs, and analytical functions

### Deliverables

- `week3_advanced_sql.ipynb`
- `superstore_advanced_queries.sql`

**Tools & Technologies**

- SQL
- SQLite
- Google Colab
- Pandas

---

# ☁️ Week 4 — Azure Cloud Fundamentals & Azure Data Factory Pipeline

### Objective

Learn Azure cloud fundamentals by designing and implementing an end-to-end Azure Data Factory pipeline to transfer data between Azure Blob Storage containers while validating metadata.

### Highlights

- Created an Azure Resource Group
- Provisioned an Azure Storage Account
- Created Blob Storage containers for source and destination
- Uploaded CSV dataset into Azure Blob Storage
- Configured Azure Blob Storage Linked Service
- Created Source and Destination datasets
- Built an Azure Data Factory pipeline
- Implemented **Get Metadata** activity to validate source file properties
- Configured **Copy Data** activity for data movement
- Assigned IAM roles (Reader & Contributor) to the Azure Data Factory Managed Identity
- Validated and published the pipeline
- Successfully executed the pipeline
- Verified copied data in destination Blob container
- Confirmed successful metadata validation

### Deliverables

- Azure Resource Group
- Azure Storage Account
- Blob Storage Containers
- Azure Data Factory
- Linked Service Configuration
- Source & Destination Datasets
- Get Metadata Activity
- Copy Data Pipeline
- IAM Role Assignment
- Pipeline Validation & Publishing
- Pipeline Execution Report
- Mini Project Documentation

### Technologies Used

- Microsoft Azure
- Azure Resource Manager
- Azure Blob Storage
- Azure Data Factory (ADF)
- Azure IAM
- Copy Data Activity
- Get Metadata Activity

---

# 🔥 Week 5 — Data Cleaning, Transformation & Aggregation using PySpark

### Objective

Perform large-scale data cleaning, transformation, and aggregation using PySpark, covering both foundational Spark theory and hands-on DataFrame engineering on a single, realistic Superstore-style dataset.

### Highlights

- Explained the limitations of traditional MapReduce compared to Spark's in-memory computing model
- Built one consistent, realistic PySpark DataFrame reused across the entire assignment instead of separate toy datasets per question
- Removed duplicate rows using `dropDuplicates()` on composite keys (`user_id`, `transaction_date`)
- Filtered and aggregated data using `filter()`, `groupBy()`, and `agg()`
- Handled missing values using `.na.drop()` and `.na.fill()`
- Explained DataFrame immutability and its effect on data cleaning workflows
- Cast a string column to `TimestampType` and renamed it using `withColumnRenamed()`
- Explained the Shuffle process and the distinction between wide and narrow transformations
- Computed multiple aggregate statistics (min, max, average) in a single `.agg()` call
- Discussed the risks of using `inferSchema=True` on messy or inconsistent source data
- Built a complete end-to-end pipeline: deduplication → null handling → grouped revenue aggregation

### Deliverables

- `Week5_CEI_DataEngineering.ipynb`
- `week5_pyspark.py`
- `requirements.txt`

**Tools & Technologies**

- PySpark
- Apache Spark
- Google Colab
- Python

---

# 🔥 Week 6 — Apache Spark & PySpark: Retail Analytics Pipeline
 
### Objective
 
Develop a comprehensive PySpark workflow using a realistic retail transactions dataset, covering Spark session initialization, schema-aware CSV ingestion, exploratory data analysis, DataFrame transformations, conditional filtering, data type casting, derived column creation, and CSV/Parquet read-write operations. The project also demonstrates key Apache Spark architecture concepts and is presented as a complete internship deliverable with detailed documentation, a technical report, and GitHub-ready artifacts.
 
### Key Tasks
 
- Initialized a local `SparkSession` and loaded a 50-row retail dataset with `header=true`
  and `inferSchema=true`
- Explored schema and data quality (`printSchema()`, `show()`, null-value checks on
  `Customer_ID`)
- Explained Spark's Driver / Cluster Manager / Executor architecture and Client vs.
  Cluster deployment modes
- Explained Lazy Evaluation and how the DAG/lineage graph enables fault tolerance
- Selected and filtered columns (`product_id`, `price` where `category == 'Electronics'`)
- Renamed a column and cast `price` from `String` to `Double`
- Applied compound `AND` (`status == 'Completed' AND amount > 1000`) and `OR`
  (`region == 'North' OR priority == 'High'`) filters
- Added a calculated column (`final_price = base_price * 1.18`)
- Compared CSV (row-based) vs. Parquet (columnar) storage and explained Predicate Pushdown
- Read Parquet, filtered out null `user_id` rows, and wrote the cleaned result to CSV
- Inspected the physical execution plan with `explain()` and reasoned about `.show()` vs.
  `.collect()` on large datasets
- Wrote CSV and Parquet outputs and documented every step in a full internship report

### Technologies Used
 
- PySpark (DataFrame API)
- Apache Spark (local mode)
- Jupyter Notebook
- Python, Pandas (dataset generation & verification)
- CSV & Parquet

### Deliverables
 
- Jupyter Notebook (`Week6_PySpark(1).ipynb`)
- Internship Report (`week6_assignment_report(1).pdf`)
- Sample Dataset (`sample_retail_dataset.csv`)
- Requirements(`requirements.txt`)
---

# 💡 Technical Skills Strengthened

Throughout the internship, I have gained hands-on experience in:

- Python Programming
- SQL Query Development
- Data Cleaning & Transformation
- Relational Database Concepts
- Data Engineering Fundamentals
- Exploratory Data Analysis
- Azure Cloud Fundamentals
- Azure Resource Management
- Azure Blob Storage
- Azure Data Factory (ADF)
- ETL Pipeline Development
- Metadata Validation
- IAM Role Management
- Cloud Data Integration
- Distributed Data Processing with PySpark
- Spark DataFrame Transformations & Aggregations

---

# 🎯 Internship Vision

The primary objective of this internship is to bridge the gap between academic concepts and real-world data engineering practices by solving practical business problems using modern cloud technologies, SQL, Python, and Azure Data Services.

Each assignment contributes towards building strong practical skills in Data Engineering, Cloud Computing, and Analytics.

---

# 📢 Future Updates

This repository will continue to evolve as new assignments, cloud projects, and learning milestones are completed during the Celebal Excellence Internship (CEI) 2026.

---

<div align="center">

### ⭐ Thanks for visiting my repository!

*If you find this repository useful, feel free to explore the weekly assignments and follow my learning journey.*

</div>
