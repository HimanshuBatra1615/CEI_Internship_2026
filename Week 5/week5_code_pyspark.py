# 1. Environment Setup
pip install pyspark==4.0.0


# 1.2 Import Libraries
import random
from datetime import datetime, timedelta

import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

print("PySpark version:", pyspark.__version__)


# 1.3 Create Spark Session
spark = (
    SparkSession.builder
    .appName("CEI-Week5-DataEngineering-Superstore")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark


# 2. Dataset
random.seed(42)

REGIONS = ["West", "East", "Central", "South"]
CATEGORIES = ["Furniture", "Office Supplies", "Technology"]
CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
]
STATUS_VALUES = ["Shipped", "Delivered", "Pending", "Cancelled", None]
SUBSCRIPTION_TIERS = ["Premium", "Standard", "Basic"]

COLUMNS = [
    "order_id", "user_id", "transaction_date", "region", "city",
    "product_category", "sale_amount", "status", "age", "subscription",
    "raw_timestamp", "username", "email", "store_id", "price",
]


def generate_superstore_records(n_records: int = 1200):
    """Builds a realistic, extended Superstore-style transaction record set."""
    records = []
    for i in range(1, n_records + 1):
        user_id = f"U{random.randint(1000, 1199)}"
        order_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 720))
        region = random.choice(REGIONS)
        city = random.choice(CITIES)
        category = random.choice(CATEGORIES)
        sale_amount = round(random.uniform(15, 2500), 2)
        status = random.choice(STATUS_VALUES)
        age = random.randint(16, 65)
        subscription = random.choice(SUBSCRIPTION_TIERS)
        raw_timestamp = order_date.strftime("%Y-%m-%d %H:%M:%S")
        username = f"user_{user_id.lower()}" if random.random() > 0.03 else ""
        email = f"{user_id.lower()}@example.com" if random.random() > 0.06 else None
        store_id = f"ST-{random.randint(1, 20):03d}"
        price = sale_amount if random.random() > 0.05 else None

        records.append((
            f"ORD-{i:05d}", user_id, order_date.strftime("%Y-%m-%d"), region, city,
            category, sale_amount, status, age, subscription, raw_timestamp,
            username, email, store_id, price,
        ))
    return records


records = generate_superstore_records(1200)
pdf = pd.DataFrame(records, columns=COLUMNS)

# Deliberately re-insert a sample of rows to create duplicate (user_id, transaction_date)
# pairs, so the deduplication logic in Q3 / Q15 has something real to remove.
duplicate_sample = pdf.sample(60, random_state=7)
pdf = pd.concat([pdf, duplicate_sample], ignore_index=True)

df = spark.createDataFrame(pdf)
df.cache()

print("Total records loaded:", df.count())


# 2.1 Dataset Preview
df.show(10, truncate=False)


# 2.2 Dataset Schema
df.printSchema()


# 3. Questions Q1 – Q15

# Q1. Limitations of Traditional MapReduce
# ============================================================================

# ============================================================================
# Q2. In-Memory Computing in Spark
# ============================================================================

# Q3. Removing Duplicate Rows
df_dedup = df.dropDuplicates(["user_id", "transaction_date"])

print("Row count before deduplication:", df.count())
print("Row count after deduplication:", df_dedup.count())
df_dedup.select("order_id", "user_id", "transaction_date").show(10)


# Q4. Average Sales by Category in the West Region
df_west_avg = (
    df.filter(F.col("region") == "West")
      .groupBy("product_category")
      .agg(F.round(F.avg("sale_amount"), 2).alias("avg_sale_amount"))
      .orderBy(F.col("avg_sale_amount").desc())
)

df_west_avg.show()


# Q5. `.na.drop()` vs `.na.fill()`
df_status_filled = df.na.fill({"status": "Unknown"})

print("Null status count before fill:", df.filter(F.col("status").isNull()).count())
print("Null status count after fill:", df_status_filled.filter(F.col("status").isNull()).count())
df_status_filled.select("order_id", "status").show(10)


# Q6. City Record Counts Above a Threshold
df_city_counts = (
    df.groupBy("city")
      .agg(F.count("*").alias("record_count"))
      .filter(F.col("record_count") > 100)
      .orderBy(F.col("record_count").desc())
)

df_city_counts.show()


# ============================================================================
# Q7. DataFrame Immutability and Data Cleaning
# ============================================================================

# Q8. Filtering Premium Subscribers Aged 18–30
df_young_premium = df.filter(
    (F.col("age").between(18, 30)) & (F.col("subscription") == "Premium")
)

print("Matching records:", df_young_premium.count())
df_young_premium.select("user_id", "age", "subscription", "city").show(10)


# ============================================================================
# Q9. Handling Nulls Before Aggregation
# ============================================================================

# Q10. Casting `raw_timestamp` to `TimestampType`
df_event_time = (
    df.withColumn("raw_timestamp", F.col("raw_timestamp").cast(TimestampType()))
      .withColumnRenamed("raw_timestamp", "event_time")
)

df_event_time.select("order_id", "event_time").printSchema()
df_event_time.select("order_id", "event_time").show(5, truncate=False)


# ============================================================================
# Q11. Shuffle and Wide Transformations
# ============================================================================

# Q12. Removing Rows with Null Email or Empty Username
df_clean_contacts = df.filter(
    F.col("email").isNotNull() & (F.col("username") != "")
)

print("Rows before cleaning:", df.count())
print("Rows after removing null email / empty username:", df_clean_contacts.count())
df_clean_contacts.select("order_id", "username", "email").show(10)


# Q13. Multiple Aggregations with `.agg()`
df_price_stats = df.agg(
    F.min("price").alias("min_price"),
    F.max("price").alias("max_price"),
    F.round(F.avg("price"), 2).alias("avg_price"),
)

df_price_stats.show()


# ============================================================================
# Q14. Risks of `inferSchema=True`
# ============================================================================

# Q15. End-to-End Processing Pipeline
df_pipeline = (
    df.dropDuplicates(["user_id", "transaction_date"])
      .na.fill({"price": 0})
      .groupBy("store_id")
      .agg(F.round(F.sum("price"), 2).alias("total_revenue"))
      .orderBy(F.col("total_revenue").desc())
)

df_pipeline.show(20)

# Stop the Spark session when the script finishes
spark.stop()
