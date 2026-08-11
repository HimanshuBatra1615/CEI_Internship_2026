"""
================================================================================
RideSharing Analytics Platform — Notebook Runner Script
================================================================================
Module      : notebooks/pipeline_notebook.py
Description : Production-grade analysis script that mirrors a Jupyter notebook's
              structure (Introduction → Imports → Pipeline → Results → Insights).
              Run this directly or convert to .ipynb with `jupytext`.

              Sections:
                  0. Environment Setup
                  1. Bronze Layer — Raw Ingestion
                  2. Data Quality Validation
                  3. Silver Layer — Transformation
                  4. Gold Layer — Analytics
                  5. Window Functions Showcase
                  6. Spark SQL Analytics
                  7. Visualizations
                  8. Executive Summary

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# ============================================================================
# SECTION 0: ENVIRONMENT SETUP
# ============================================================================

print("=" * 70)
print("  RIDESHARING ANALYTICS PLATFORM — FULL PIPELINE NOTEBOOK")
print("  Medallion Architecture: Bronze → Silver → Gold")
print("=" * 70)

import os
import shutil
import pandas as pd

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader
from src.utils.spark_session import get_spark_session, stop_spark_session
from src.ingestion.bronze_ingestion import BronzeIngestionEngine
from src.transformation.silver_transformation import SilverTransformationEngine
from src.analytics.gold_analytics import GoldAnalyticsEngine
from src.validation.data_validator import DataValidator
from src.optimization.spark_optimizations import SparkOptimizationShowcase
from src.utils.report_generator import (
    plot_revenue_trend, plot_city_revenue, plot_completion_rates,
    plot_top_drivers, plot_rating_distribution, plot_peak_hour,
    plot_distance_distribution, plot_driver_segmentation,
    plot_delay_distribution, plot_revenue_per_km,
    generate_markdown_report,
)

logger = get_logger("notebook.runner")

# Ensure directories exist
for d in ["data/raw", "data/bronze", "data/silver", "data/gold",
          "logs", "reports/charts"]:
    Path(d).mkdir(parents=True, exist_ok=True)

# Copy CSVs to data/raw if present in project root
for csv in ["drivers.csv", "trips.csv", "trip_logs.csv"]:
    if Path(csv).exists() and not Path(f"data/raw/{csv}").exists():
        shutil.copy2(csv, f"data/raw/{csv}")
        print(f"  ✓ Copied {csv} → data/raw/")

CONFIG = "config/pipeline_config.yaml"
cfg    = ConfigLoader(CONFIG)

# ============================================================================
# SECTION 1: SPARK SESSION
# ============================================================================

print("\n[STEP 1] Initializing SparkSession...")
t0 = time.time()
spark = get_spark_session(config_path=CONFIG)
print(f"  ✓ Spark {spark.version} | App: {spark.conf.get('spark.app.name')}")
print(f"  ✓ AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"  ✓ Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# ============================================================================
# SECTION 2: BRONZE LAYER
# ============================================================================

print("\n[STEP 2] Bronze Layer — Raw Ingestion")
print("-" * 50)

bronze_engine = BronzeIngestionEngine(spark, CONFIG)
bronze_results = bronze_engine.run_all()

print(f"  ✓ Drivers ingested : {bronze_results['drivers']:>6,} rows")
print(f"  ✓ Trips ingested   : {bronze_results['trips']:>6,} rows")
print(f"  ✓ Logs ingested    : {bronze_results['trip_logs']:>6,} rows")

# Preview Bronze
print("\n  Bronze — Drivers (first 3 rows):")
bronze_drivers = spark.read.parquet(cfg.get("paths.bronze.drivers"))
bronze_drivers.select("driver_id","name","city","rating",
                      "_ingestion_timestamp","_source_file").show(3, truncate=False)

# ============================================================================
# SECTION 3: DATA QUALITY VALIDATION
# ============================================================================

print("\n[STEP 3] Data Quality Validation")
print("-" * 50)

quality_cfg = cfg.get_section("quality")
raw_paths   = cfg.get_section("paths")

drivers_raw  = spark.read.parquet(raw_paths["bronze"]["drivers"])
trips_raw    = spark.read.parquet(raw_paths["bronze"]["trips"])
logs_raw     = spark.read.parquet(raw_paths["bronze"]["trip_logs"])

dq_drivers   = DataValidator.run_drivers_checks(drivers_raw, quality_cfg)
dq_trips     = DataValidator.run_trips_checks(trips_raw, quality_cfg)
dq_logs      = DataValidator.run_trip_logs_checks(logs_raw, quality_cfg)

print(f"\n  DQ Results:")
for rpt in [dq_drivers, dq_trips, dq_logs]:
    status = "✅ PASS" if rpt.overall_passed else "⚠️  FAIL"
    print(f"    {status} | {rpt.dataset:<15} | checks={len(rpt.checks)} "
          f"| passed={rpt.passed_count} | failed={rpt.failed_count}")

# ============================================================================
# SECTION 4: SILVER LAYER
# ============================================================================

print("\n[STEP 4] Silver Layer — Transformation")
print("-" * 50)

silver_engine   = SilverTransformationEngine(spark, CONFIG)
silver_results  = silver_engine.run_all()

for ds, cnt in silver_results.items():
    print(f"  ✓ Silver {ds:<20} : {cnt:,} rows")

# Preview enriched Silver
enriched_df = silver_engine.get_enriched_df()
print("\n  Silver Enriched Schema:")
for field in enriched_df.schema.fields[:10]:
    print(f"    {field.name:<30} {str(field.dataType):<20} nullable={field.nullable}")

# ============================================================================
# SECTION 5: SPARK OPTIMIZATION SHOWCASE
# ============================================================================

print("\n[STEP 5] Spark Optimization Documentation")
print("-" * 50)

opt = SparkOptimizationShowcase(spark)
opt.document_aqe()
opt.document_shuffle_reduction()
opt.document_lazy_evaluation()
opt.demonstrate_predicate_pushdown(raw_paths["silver"]["trips"])

print("  ✓ Optimization notes logged (see logs/pipeline.log)")

# ============================================================================
# SECTION 6: GOLD LAYER
# ============================================================================

print("\n[STEP 6] Gold Layer — Business Analytics")
print("-" * 50)

gold_engine  = GoldAnalyticsEngine(spark, enriched_df, CONFIG)
gold_results = gold_engine.run_all()

for table, cnt in gold_results.items():
    print(f"  ✓ Gold {table:<35}: {cnt:,} rows")

# ============================================================================
# SECTION 7: WINDOW FUNCTIONS SHOWCASE
# ============================================================================

print("\n[STEP 7] Window Functions Results")
print("-" * 50)

from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Register temp views for SQL
enriched_df.createOrReplaceTempView("silver_enriched")
drivers_silver = spark.read.parquet(raw_paths["silver"]["drivers"])
trips_silver   = spark.read.parquet(raw_paths["silver"]["trips"])
logs_silver    = spark.read.parquet(raw_paths["silver"]["trip_logs"])

drivers_silver.createOrReplaceTempView("silver_drivers")
trips_silver.createOrReplaceTempView("silver_trips")
logs_silver.createOrReplaceTempView("silver_trip_logs")

# Window function showcase
w_global = Window.orderBy(F.desc("total_revenue"))
w_city   = Window.partitionBy("city").orderBy(F.desc("total_revenue"))
w_date   = Window.orderBy("trip_date")
w_rolling = Window.orderBy("trip_date").rowsBetween(-2, 0)

driver_perf = gold_engine.build_driver_performance()
print("\n  Top 5 Drivers (ROW_NUMBER | RANK | DENSE_RANK | NTILE):")
driver_perf.select(
    "name", "city", "total_revenue", "completion_rate",
    "rank_revenue_global", "dense_rank_revenue_city",
    "row_number_global", "ntile_quartile"
).orderBy("rank_revenue_global").show(5, truncate=False)

# LAG / LEAD Revenue Trend
rev_trend = gold_engine.build_revenue_analytics()
print("  Revenue Trend (LAG | LEAD | Rolling Avg | Cumulative):")
rev_trend.select(
    "trip_date", "daily_revenue", "prev_day_revenue",
    "rolling_3d_revenue", "cumulative_revenue"
).show(truncate=False)

# ============================================================================
# SECTION 8: SPARK SQL ANALYTICS
# ============================================================================

print("\n[STEP 8] Spark SQL Analytics")
print("-" * 50)

# Register Gold views
gold_paths = cfg.get_section("paths")["gold"]
for table_name, path in gold_paths.items():
    try:
        spark.read.parquet(path).createOrReplaceTempView(f"gold_{table_name}")
    except Exception:
        pass

# Q1: City Revenue Leaderboard
print("\n  City Revenue Leaderboard:")
spark.sql("""
    SELECT city, ROUND(total_revenue, 2) AS revenue,
           CONCAT(ROUND(revenue_share_pct, 1), '%') AS market_share,
           active_drivers, revenue_rank
    FROM gold_city_analytics
    ORDER BY revenue_rank
""").show(truncate=False)

# Q2: Driver Segmentation
print("  Driver Segmentation:")
spark.sql("""
    SELECT segment, COUNT(*) AS drivers,
           ROUND(AVG(driver_efficiency_score), 3) AS avg_score,
           ROUND(AVG(completion_rate)*100, 1) AS avg_completion_pct
    FROM gold_driver_segmentation
    GROUP BY segment ORDER BY avg_score DESC
""").show(truncate=False)

# Q3: Peak Hour
print("  Peak Hour Revenue:")
spark.sql("""
    SELECT time_of_day, SUM(total_trips) AS trips,
           ROUND(SUM(revenue), 2) AS total_revenue
    FROM gold_peak_hour_dataset
    GROUP BY time_of_day ORDER BY total_revenue DESC
""").show(truncate=False)

# ============================================================================
# SECTION 9: VISUALIZATIONS
# ============================================================================

print("\n[STEP 9] Generating Visualizations")
print("-" * 50)

rev_pd   = spark.read.parquet(gold_paths["revenue_analytics"]).toPandas()
city_pd  = spark.read.parquet(gold_paths["city_analytics"]).toPandas()
perf_pd  = spark.read.parquet(gold_paths["driver_performance"]).toPandas()
peak_pd  = spark.read.parquet(gold_paths["peak_hour_dataset"]).toPandas()
seg_pd   = spark.read.parquet(gold_paths["driver_segmentation"]).toPandas()
drv_pd   = spark.read.parquet(raw_paths["silver"]["drivers"]).toPandas()

# Raw trips & logs for some charts
trips_pd = spark.read.parquet(raw_paths["silver"]["trips"]).toPandas()
logs_pd  = spark.read.parquet(raw_paths["silver"]["trip_logs"]).toPandas()

chart_paths = {
    "revenue_trend":     plot_revenue_trend(rev_pd),
    "city_revenue":      plot_city_revenue(city_pd),
    "completion_rates":  plot_completion_rates(city_pd),
    "top_drivers":       plot_top_drivers(perf_pd),
    "rating_dist":       plot_rating_distribution(drv_pd),
    "peak_hour":         plot_peak_hour(peak_pd),
    "distance_dist":     plot_distance_distribution(trips_pd),
    "driver_segmentation": plot_driver_segmentation(seg_pd),
    "delay_dist":        plot_delay_distribution(logs_pd),
    "revenue_per_km":    plot_revenue_per_km(perf_pd),
}

for name, path in chart_paths.items():
    print(f"  ✓ {name:<30}: {path}")

# ============================================================================
# SECTION 10: EXECUTIVE SUMMARY REPORT
# ============================================================================

print("\n[STEP 10] Executive Summary")
print("-" * 50)

kpi_pd = spark.read.parquet(gold_paths["executive_kpis"]).toPandas()
kpi_rows = list(kpi_pd.itertuples(index=False, name=None))

report_path = generate_markdown_report(
    kpi_rows=kpi_rows,
    chart_paths=chart_paths,
    gold_summaries=gold_results,
)
print(f"  ✓ Business report: {report_path}")

print("\n  Executive KPIs:")
for row in kpi_rows:
    print(f"    {row[0]:<35}: {row[1]}")

# ============================================================================
# CLEANUP
# ============================================================================

enriched_df.unpersist()
total_time = time.time() - t0
stop_spark_session(spark)

print("\n" + "=" * 70)
print(f"  PIPELINE COMPLETE ✅")
print(f"  Total Runtime : {total_time:.1f} seconds")
print(f"  Charts        : {len(chart_paths)} generated in reports/charts/")
print(f"  Gold Tables   : {len(gold_results)} written to data/gold/")
print(f"  Report        : {report_path}")
print("=" * 70)
