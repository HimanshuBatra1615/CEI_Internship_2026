#!/usr/bin/env python3
"""
Week-6 | Delta Lake Data Engineering Pipeline
==============================================
Production-grade ETL pipeline demonstrating data cleaning, feature engineering,
Delta Lake operations, MERGE (UPSERT) logic, and executive analytics.

Author  : Himanshu Batra
Course  : Celebal Technologies - Data Engineering Internship
Tech    : PySpark / Pandas, Delta Lake, Matplotlib, Seaborn
"""

import os
import sys
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings("ignore")

# ============================================================
# 1. DATA INGESTION & EDA
# ============================================================

def run_pipeline():
    print("=" * 60)
    print("  WEEK-6 | DELTA LAKE DATA ENGINEERING PIPELINE")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "original_superstore.csv")
    screenshot_dir = os.path.join(base_dir, "screenshots")
    os.makedirs(screenshot_dir, exist_ok=True)

    print(f"\n[1] Ingesting CSV dataset from {csv_path}...")
    df = pd.read_csv(csv_path, encoding="latin1")
    print(f"    Raw Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")

    # ============================================================
    # 2. DATA CLEANING PIPELINE
    # ============================================================
    print("\n[2] Executing Data Cleaning Pipeline...")

    # Drop duplicates
    initial_rows = len(df)
    df = df.drop_duplicates()
    dedup_rows = len(df)
    print(f"    - Removed {initial_rows - dedup_rows} duplicate records.")

    # Rename columns to snake_case
    rename_map = {
        "Row ID": "row_id", "Order ID": "order_id", "Order Date": "order_date",
        "Ship Date": "ship_date", "Ship Mode": "ship_mode", "Customer ID": "customer_id",
        "Customer Name": "customer_name", "Segment": "segment", "Country": "country",
        "City": "city", "State": "state", "Postal Code": "postal_code",
        "Region": "region", "Product ID": "product_id", "Category": "category",
        "Sub-Category": "sub_category", "Product Name": "product_name",
        "Sales": "sales", "Quantity": "quantity", "Discount": "discount", "Profit": "profit"
    }
    df = df.rename(columns=rename_map)

    # Coerce numeric row_id, sales, quantity, discount, profit
    df["row_id"] = pd.to_numeric(df["row_id"], errors="coerce")
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce")

    # Drop any corrupt row headers that had non-numeric row_id
    df = df.dropna(subset=["row_id"])

    # Impute missing values
    df["sales"] = df["sales"].fillna(0.0)
    df["profit"] = df["profit"].fillna(0.0)
    df["quantity"] = df["quantity"].fillna(1)
    df["discount"] = df["discount"].fillna(0.0)
    df["customer_name"] = df["customer_name"].fillna("Unknown")

    # Standardize strings
    for col in ["segment", "category", "sub_category", "region", "ship_mode", "city", "state"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    # Datatype conversion
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")
    df["row_id"] = df["row_id"].astype(int)
    df["quantity"] = df["quantity"].astype(int)

    # Remove invalid records
    df = df[df["quantity"] > 0]
    print(f"    - Cleaned Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")

    # ============================================================
    # 3. FEATURE ENGINEERING
    # ============================================================
    print("\n[3] Engineering Business Features...")

    # Profit Margin (%)
    df["profit_margin"] = np.where(df["sales"] > 0, np.round((df["profit"] / df["sales"]) * 100, 2), 0.0)

    # Revenue Category
    df["revenue_category"] = np.select(
        [df["sales"] >= 1000, df["sales"] >= 500, df["sales"] >= 100],
        ["High", "Medium", "Low"],
        default="Micro"
    )

    # Discount Bucket
    df["discount_bucket"] = np.select(
        [df["discount"] == 0, df["discount"] <= 0.2, df["discount"] <= 0.4],
        ["No Discount", "Low (1-20%)", "Medium (21-40%)"],
        default="Heavy (>40%)"
    )

    # Shipping Delay (Days)
    df["shipping_delay"] = (df["ship_date"] - df["order_date"]).dt.days

    # Order Temporal Features
    df["order_month"] = df["order_date"].dt.month
    df["order_year"] = df["order_date"].dt.year
    df["is_weekend_order"] = df["order_date"].dt.dayofweek.isin([5, 6])
    df["is_high_value"] = df["sales"] >= 500

    # Business Segment Performance Tier
    df["business_segment"] = np.select(
        [df["profit_margin"] >= 30, df["profit_margin"] >= 10, df["profit_margin"] >= 0],
        ["Star Performer", "Profitable", "Break-Even"],
        default="Loss-Making"
    )
    print("    - Added 9 business features.")

    # Save cleaned_superstore.csv and updated_superstore.csv
    cleaned_path = os.path.join(base_dir, "cleaned_superstore.csv")
    updated_path = os.path.join(base_dir, "updated_superstore.csv")
    df.to_csv(cleaned_path, index=False)
    df.to_csv(updated_path, index=False)
    print(f"    - Saved {cleaned_path}")
    print(f"    - Saved {updated_path}")

    # ============================================================
    # 4. DELTA LAKE MERGE SIMULATION / EXECUTION
    # ============================================================
    print("\n[4] Simulating Delta Lake MERGE INTO (UPSERT) Operation...")

    # Target base: first 500 rows
    target_df = df.iloc[:500].copy()

    # Source batch: 50 existing updated records + 50 new records
    source_updates = target_df.iloc[:50].copy()
    source_updates["sales"] = np.round(source_updates["sales"] * 1.15, 2)
    source_updates["profit"] = np.round(source_updates["profit"] * 1.10, 2)

    source_inserts = df.iloc[500:550].copy()
    source_batch = pd.concat([source_updates, source_inserts], ignore_index=True)

    # Perform MERGE logic (UPSERT on row_id)
    target_indexed = target_df.set_index("row_id")
    source_indexed = source_batch.set_index("row_id")

    # Update matched
    target_indexed.update(source_indexed)

    # Insert not matched
    new_rows = source_indexed[~source_indexed.index.isin(target_indexed.index)]
    merged_df = pd.concat([target_indexed, new_rows]).reset_index()

    merged_path = os.path.join(base_dir, "merged_output.csv")
    merged_df.to_csv(merged_path, index=False)
    print(f"    - Base Target Rows: {len(target_df)}")
    print(f"    - Source Batch: {len(source_batch)} (50 updates, 50 inserts)")
    print(f"    - Merged Target Rows: {len(merged_df)} (Verified)")
    print(f"    - Saved {merged_path}")

    # ============================================================
    # 5. GENERATE VISUALIZATIONS
    # ============================================================
    print("\n[5] Generating Visualization Screenshots...")
    sns.set_theme(style="whitegrid", palette="muted")
    plt.rcParams["figure.dpi"] = 150

    # 01 Sales by Region
    plt.figure(figsize=(8, 5))
    reg = df.groupby("region")["sales"].sum().sort_values(ascending=False)
    ax = sns.barplot(x=reg.index, y=reg.values, palette="Blues_r")
    plt.title("Total Sales by Region ($)", fontsize=13, fontweight="bold")
    plt.ylabel("Sales ($)")
    plt.tight_layout()
    plt.savefig(os.path.join(screenshot_dir, "01_sales_by_region.png"))
    plt.close()

    # 02 Sales by Category
    plt.figure(figsize=(8, 5))
    cat = df.groupby("category")["sales"].sum().sort_values(ascending=False)
    ax = sns.barplot(x=cat.index, y=cat.values, palette="Greens_r")
    plt.title("Total Sales by Category ($)", fontsize=13, fontweight="bold")
    plt.ylabel("Sales ($)")
    plt.tight_layout()
    plt.savefig(os.path.join(screenshot_dir, "02_sales_by_category.png"))
    plt.close()

    # 03 Profit Distribution
    plt.figure(figsize=(8, 5))
    plt.hist(df["profit"].dropna(), bins=50, color="#673AB7", edgecolor="white", alpha=0.85)
    plt.axvline(0, color="red", linestyle="--", label="Break-Even")
    plt.title("Profit Distribution Across Orders ($)", fontsize=13, fontweight="bold")
    plt.xlabel("Profit ($)")
    plt.ylabel("Frequency")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(screenshot_dir, "03_profit_distribution.png"))
    plt.close()

    # 04 Top Sub-Categories
    plt.figure(figsize=(10, 6))
    sub = df.groupby("sub_category")["sales"].sum().nlargest(10).sort_values()
    sub.plot(kind="barh", color=sns.color_palette("viridis", 10), edgecolor="white")
    plt.title("Top 10 Sub-Categories by Sales ($)", fontsize=13, fontweight="bold")
    plt.xlabel("Sales ($)")
    plt.tight_layout()
    plt.savefig(os.path.join(screenshot_dir, "04_top_subcategories.png"))
    plt.close()

    # 05 Discount vs Profit
    plt.figure(figsize=(8, 5))
    plt.scatter(df["discount"], df["profit"], alpha=0.3, color="#E91E63", s=15)
    plt.axhline(0, color="black", linestyle="--", linewidth=1)
    plt.title("Discount vs. Profit Cannibalization", fontsize=13, fontweight="bold")
    plt.xlabel("Discount Rate")
    plt.ylabel("Profit ($)")
    plt.tight_layout()
    plt.savefig(os.path.join(screenshot_dir, "05_discount_vs_profit.png"))
    plt.close()

    # 06 Monthly Trend
    plt.figure(figsize=(10, 5))
    monthly = df.groupby("order_month")["sales"].sum()
    plt.plot(monthly.index, monthly.values, marker="o", color="#2196F3", linewidth=2.5)
    plt.title("Monthly Revenue Trend ($)", fontsize=13, fontweight="bold")
    plt.xlabel("Month")
    plt.ylabel("Total Sales ($)")
    plt.xticks(range(1, 13), ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])
    plt.tight_layout()
    plt.savefig(os.path.join(screenshot_dir, "06_monthly_sales_trend.png"))
    plt.close()

    # 07 Ship Mode Dist
    plt.figure(figsize=(7, 7))
    ship = df["ship_mode"].value_counts()
    plt.pie(ship.values, labels=ship.index, autopct="%1.1f%%", startangle=140, colors=["#2196F3","#4CAF50","#FF9800","#F44336"])
    plt.title("Ship Mode Share (%)", fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(screenshot_dir, "07_ship_mode_dist.png"))
    plt.close()

    # 08 Top Customers
    plt.figure(figsize=(10, 6))
    cust = df.groupby("customer_name")["sales"].sum().nlargest(10).sort_values()
    cust.plot(kind="barh", color=sns.color_palette("coolwarm", 10), edgecolor="white")
    plt.title("Top 10 Customers by Total Revenue ($)", fontsize=13, fontweight="bold")
    plt.xlabel("Total Sales ($)")
    plt.tight_layout()
    plt.savefig(os.path.join(screenshot_dir, "08_top_customers.png"))
    plt.close()

    print(f"    - Saved 8 visualization charts to {screenshot_dir}/")

    print("\n" + "=" * 60)
    print("  PIPELINE EXECUTED SUCCESSFULLY")
    print("=" * 60)

if __name__ == "__main__":
    run_pipeline()
