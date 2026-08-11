"""
================================================================================
RideSharing Analytics Platform — Gold Layer Analytics Engine
================================================================================
Module      : src/analytics/gold_analytics.py
Description : Reads the Silver enriched dataset and generates all Gold-layer
              business tables using window functions, aggregations, and KPI
              calculations. Every table is analytics-ready and executive-facing.

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end, log_optimization_note
from src.utils.config_loader import ConfigLoader

logger = get_logger("analytics.gold")


class GoldAnalyticsEngine:
    """
    Generates all Gold-layer business tables from the Silver enriched dataset.
    All window functions, KPIs, rankings, and trend tables are built here.
    """

    def __init__(self, spark: SparkSession, enriched_df: DataFrame,
                 config_path: str = "config/pipeline_config.yaml") -> None:
        self.spark = spark
        self.df = enriched_df
        self.cfg = ConfigLoader(config_path)
        self.paths = self.cfg.get_section("paths")

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    def run_all(self) -> dict[str, int]:
        """Execute all Gold layer computations and write Parquet outputs."""
        logger.info("╔══════════════════════════════════════════╗")
        logger.info("║      GOLD LAYER ANALYTICS STARTED        ║")
        logger.info("╚══════════════════════════════════════════╝")

        results = {}
        tables = [
            ("driver_performance",     self.build_driver_performance),
            ("revenue_analytics",      self.build_revenue_analytics),
            ("city_analytics",         self.build_city_analytics),
            ("cancellation_analytics", self.build_cancellation_analytics),
            ("delay_analytics",        self.build_delay_analytics),
            ("location_analytics",     self.build_location_analytics),
            ("driver_ranking",         self.build_driver_ranking),
            ("revenue_trend",          self.build_revenue_trend),
            ("executive_kpis",         self.build_executive_kpis),
            ("driver_segmentation",    self.build_driver_segmentation),
            ("peak_hour_dataset",      self.build_peak_hour_dataset),
        ]

        for name, fn in tables:
            log_pipeline_start(logger, "Gold", name)
            gold_df = fn()
            out_path = self.paths["gold"].get(name, f"data/gold/{name}")
            gold_df.coalesce(1).write.mode("overwrite").parquet(out_path)
            count = gold_df.count()
            results[name] = count
            log_pipeline_end(logger, "Gold", name, count)

        logger.info("Gold layer complete: %s", results)
        return results

    # ------------------------------------------------------------------
    # 1. Driver Performance
    # ------------------------------------------------------------------

    def build_driver_performance(self) -> DataFrame:
        """
        Per-driver aggregated performance metrics.
        Window functions: RANK, DENSE_RANK, NTILE, ROW_NUMBER over revenue and rating.
        """
        completed = self.df.filter(F.col("is_completed") == 1)

        base = (
            self.df.groupBy("driver_id", "name", "city", "rating", "rating_tier")
            .agg(
                F.count("trip_id").alias("total_trips"),
                F.sum("is_completed").cast("int").alias("completed_trips"),
                F.sum(F.when(F.col("is_completed") == 0, 1).otherwise(0)).alias("cancelled_trips"),
                F.sum(F.when(F.col("is_completed") == 1, F.col("fare_amount")).otherwise(0)).alias("total_revenue"),
                F.avg(F.when(F.col("is_completed") == 1, F.col("fare_amount"))).alias("avg_fare"),
                F.sum(F.when(F.col("is_completed") == 1, F.col("distance_km"))).alias("total_distance_km"),
                F.avg(F.when(F.col("is_completed") == 1, F.col("distance_km"))).alias("avg_distance_km"),
                F.avg(F.when(F.col("is_completed") == 1, F.col("trip_duration_min"))).alias("avg_trip_duration_min"),
                F.avg(F.col("delay_minutes")).alias("avg_delay_minutes"),
                F.sum(F.col("delay_minutes")).alias("total_delay_minutes"),
            )
        )

        # Derived KPIs
        base = base.withColumn(
            "completion_rate",
            F.round(F.col("completed_trips") / F.col("total_trips"), 4)
        ).withColumn(
            "cancellation_rate",
            F.round(F.col("cancelled_trips") / F.col("total_trips"), 4)
        ).withColumn(
            "revenue_per_km",
            F.when(F.col("total_distance_km") > 0,
                   F.round(F.col("total_revenue") / F.col("total_distance_km"), 4))
             .otherwise(None)
        )

        # Driver Consistency Score = 1 - (rating_stddev / avg_rating)
        # Since each driver has a single rating row, we use rating directly
        # In a multi-trip-rating model this would use stddev over trip ratings
        base = base.withColumn(
            "driver_efficiency_score",
            F.round(
                (0.40 * F.col("completion_rate")) +
                (0.30 * (F.col("rating") / 5.0)) +
                (0.20 * F.least(F.col("revenue_per_km") / 20.0, F.lit(1.0))) +
                (0.10 * F.greatest(F.lit(0.0), F.lit(1.0) - F.col("avg_delay_minutes") / 20.0)),
                4
            )
        )

        # Window functions
        w_city_rev   = Window.partitionBy("city").orderBy(F.desc("total_revenue"))
        w_global_rev = Window.orderBy(F.desc("total_revenue"))
        w_global_eff = Window.orderBy(F.desc("driver_efficiency_score"))

        return (
            base
            .withColumn("rank_revenue_global",     F.rank().over(w_global_rev))
            .withColumn("dense_rank_revenue_city", F.dense_rank().over(w_city_rev))
            .withColumn("row_number_global",       F.row_number().over(w_global_rev))
            .withColumn("ntile_quartile",          F.ntile(4).over(w_global_eff))
            .withColumn("rank_efficiency",         F.rank().over(w_global_eff))
        )

    # ------------------------------------------------------------------
    # 2. Revenue Analytics
    # ------------------------------------------------------------------

    def build_revenue_analytics(self) -> DataFrame:
        """Revenue metrics with LAG/LEAD window functions for trend analysis."""
        completed = self.df.filter(F.col("is_completed") == 1)

        daily = (
            completed.groupBy("trip_date")
            .agg(
                F.count("trip_id").alias("completed_trips"),
                F.sum("fare_amount").alias("daily_revenue"),
                F.avg("fare_amount").alias("avg_fare"),
                F.avg("distance_km").alias("avg_distance_km"),
            )
            .orderBy("trip_date")
        )

        w_date = Window.orderBy("trip_date")
        w_rolling = Window.orderBy("trip_date").rowsBetween(-2, 0)  # 3-day rolling

        return (
            daily
            .withColumn("prev_day_revenue",   F.lag("daily_revenue",  1).over(w_date))
            .withColumn("next_day_revenue",   F.lead("daily_revenue", 1).over(w_date))
            .withColumn("revenue_delta",      F.col("daily_revenue") - F.col("prev_day_revenue"))
            .withColumn("revenue_delta_pct",
                        F.round((F.col("revenue_delta") / F.col("prev_day_revenue")) * 100, 2))
            .withColumn("rolling_3d_revenue", F.round(F.avg("daily_revenue").over(w_rolling), 2))
            .withColumn("cumulative_revenue", F.round(F.sum("daily_revenue").over(w_date), 2))
            .withColumn("first_day_revenue",  F.first("daily_revenue").over(w_date))
            .withColumn("last_day_revenue",   F.last("daily_revenue").over(w_date))
        )

    # ------------------------------------------------------------------
    # 3. City Analytics
    # ------------------------------------------------------------------

    def build_city_analytics(self) -> DataFrame:
        """City-level performance with ranking and share metrics."""
        city_agg = (
            self.df.groupBy("city")
            .agg(
                F.count("trip_id").alias("total_trips"),
                F.sum("is_completed").cast("long").alias("completed_trips"),
                F.sum(F.when(F.col("is_completed") == 0, 1).otherwise(0)).alias("cancelled_trips"),
                F.sum(F.when(F.col("is_completed") == 1, F.col("fare_amount")).otherwise(0)).alias("total_revenue"),
                F.avg(F.when(F.col("is_completed") == 1, F.col("fare_amount"))).alias("avg_fare"),
                F.countDistinct("driver_id").alias("active_drivers"),
                F.avg("rating").alias("avg_driver_rating"),
                F.avg("delay_minutes").alias("avg_delay_minutes"),
            )
        )

        total_rev_row = self.df.filter(F.col("is_completed") == 1).agg(
            F.sum("fare_amount").alias("grand_total_revenue")
        ).collect()[0]
        grand_total = total_rev_row["grand_total_revenue"] or 1.0

        w_rev = Window.orderBy(F.desc("total_revenue"))

        return (
            city_agg
            .withColumn("completion_rate",     F.round(F.col("completed_trips") / F.col("total_trips"), 4))
            .withColumn("cancellation_rate",   F.round(F.col("cancelled_trips") / F.col("total_trips"), 4))
            .withColumn("revenue_share_pct",   F.round((F.col("total_revenue") / grand_total) * 100, 2))
            .withColumn("trips_per_driver",    F.round(F.col("total_trips") / F.col("active_drivers"), 2))
            .withColumn("revenue_rank",        F.rank().over(w_rev))
        )

    # ------------------------------------------------------------------
    # 4. Cancellation Analytics
    # ------------------------------------------------------------------

    def build_cancellation_analytics(self) -> DataFrame:
        """Cancellation patterns by city, location, and driver."""
        return (
            self.df.groupBy("city", "pickup_location")
            .agg(
                F.count("trip_id").alias("total_trips"),
                F.sum(F.when(F.col("is_completed") == 0, 1).otherwise(0)).alias("cancelled_trips"),
                F.sum("is_completed").cast("long").alias("completed_trips"),
            )
            .withColumn("cancellation_rate", F.round(F.col("cancelled_trips") / F.col("total_trips"), 4))
            .withColumn("completion_rate",   F.round(F.col("completed_trips") / F.col("total_trips"), 4))
            .orderBy(F.desc("cancellation_rate"))
        )

    # ------------------------------------------------------------------
    # 5. Delay Analytics
    # ------------------------------------------------------------------

    def build_delay_analytics(self) -> DataFrame:
        """Delay distribution analysis per driver and city."""
        completed = self.df.filter(F.col("is_completed") == 1)

        w_city = Window.partitionBy("city").orderBy(F.desc("avg_delay_minutes"))

        return (
            completed.groupBy("driver_id", "name", "city")
            .agg(
                F.count("trip_id").alias("completed_trips"),
                F.avg("delay_minutes").alias("avg_delay_minutes"),
                F.max("delay_minutes").alias("max_delay_minutes"),
                F.min("delay_minutes").alias("min_delay_minutes"),
                F.sum("delay_minutes").alias("total_delay_minutes"),
            )
            .withColumn("avg_delay_score",
                        F.round(F.lit(1.0) - F.col("avg_delay_minutes") / 20.0, 4))
            .withColumn("delay_rank_in_city", F.rank().over(w_city))
        )

    # ------------------------------------------------------------------
    # 6. Location Analytics
    # ------------------------------------------------------------------

    def build_location_analytics(self) -> DataFrame:
        """Pickup and drop-off hotspot analysis."""
        pickup = (
            self.df.groupBy("pickup_location")
            .agg(
                F.count("trip_id").alias("total_pickups"),
                F.sum("is_completed").cast("long").alias("completed_pickups"),
                F.avg(F.when(F.col("is_completed") == 1, F.col("fare_amount"))).alias("avg_fare_at_pickup"),
            )
            .withColumnRenamed("pickup_location", "location")
            .withColumn("location_type", F.lit("pickup"))
        )

        drop = (
            self.df.groupBy("drop_location")
            .agg(
                F.count("trip_id").alias("total_pickups"),
                F.sum("is_completed").cast("long").alias("completed_pickups"),
                F.avg(F.when(F.col("is_completed") == 1, F.col("fare_amount"))).alias("avg_fare_at_pickup"),
            )
            .withColumnRenamed("drop_location", "location")
            .withColumn("location_type", F.lit("drop"))
        )

        return pickup.unionByName(drop).orderBy(F.desc("total_pickups"))

    # ------------------------------------------------------------------
    # 7. Driver Ranking (Top/Bottom N using window functions)
    # ------------------------------------------------------------------

    def build_driver_ranking(self) -> DataFrame:
        """
        Comprehensive driver ranking using ROW_NUMBER, RANK, DENSE_RANK,
        NTILE, FIRST_VALUE, LAST_VALUE across multiple dimensions.
        """
        perf = self.build_driver_performance()

        w_global = Window.orderBy(F.desc("driver_efficiency_score"))
        w_city   = Window.partitionBy("city").orderBy(F.desc("driver_efficiency_score"))
        w_rev    = Window.orderBy(F.desc("total_revenue"))

        return (
            perf
            .withColumn("global_rank_efficiency",   F.row_number().over(w_global))
            .withColumn("city_rank_efficiency",     F.row_number().over(w_city))
            .withColumn("global_rank_revenue",      F.row_number().over(w_rev))
            .withColumn("performance_quartile",     F.ntile(4).over(w_global))
            .withColumn("top_driver_in_city",       F.first("name").over(w_city))
            .withColumn("is_top_10",
                        F.when(F.row_number().over(w_global) <= 10, 1).otherwise(0))
            .withColumn("is_bottom_10",
                        F.when(F.row_number().over(
                            Window.orderBy(F.asc("driver_efficiency_score"))) <= 10, 1).otherwise(0))
            .select(
                "driver_id", "name", "city", "rating", "rating_tier",
                "total_trips", "completed_trips", "total_revenue",
                "completion_rate", "driver_efficiency_score",
                "revenue_per_km", "avg_delay_minutes",
                "global_rank_efficiency", "city_rank_efficiency",
                "global_rank_revenue", "performance_quartile",
                "top_driver_in_city", "is_top_10", "is_bottom_10",
            )
        )

    # ------------------------------------------------------------------
    # 8. Revenue Trend
    # ------------------------------------------------------------------

    def build_revenue_trend(self) -> DataFrame:
        """Daily, rolling-3d, and cumulative revenue trend table."""
        return self.build_revenue_analytics()

    # ------------------------------------------------------------------
    # 9. Executive KPIs
    # ------------------------------------------------------------------

    def build_executive_kpis(self) -> DataFrame:
        """
        Single-row executive summary dashboard table.
        Designed for a BI tool tile/card display.
        """
        completed = self.df.filter(F.col("is_completed") == 1)

        total_trips    = self.df.count()
        completed_cnt  = completed.count()
        cancelled_cnt  = total_trips - completed_cnt

        agg = completed.agg(
            F.sum("fare_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("distance_km").alias("avg_distance_km"),
            F.avg("trip_duration_min").alias("avg_trip_duration_min"),
            F.avg("delay_minutes").alias("avg_delay_minutes"),
            F.avg("revenue_per_km").alias("avg_revenue_per_km"),
        ).collect()[0]

        driver_agg = self.df.groupBy("driver_id").agg(
            F.avg("rating").alias("r")
        ).agg(F.avg("r").alias("avg_driver_rating")).collect()[0]

        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
        schema = StructType([
            StructField("kpi_name",   StringType(), True),
            StructField("kpi_value",  StringType(), True),
            StructField("kpi_unit",   StringType(), True),
            StructField("kpi_category", StringType(), True),
        ])

        kpi_rows = [
            ("Total Trips",             str(total_trips),                             "count",   "Volume"),
            ("Completed Trips",         str(completed_cnt),                           "count",   "Volume"),
            ("Cancelled Trips",         str(cancelled_cnt),                           "count",   "Volume"),
            ("Completion Rate",         f"{completed_cnt/total_trips*100:.1f}%",      "%",       "Quality"),
            ("Cancellation Rate",       f"{cancelled_cnt/total_trips*100:.1f}%",      "%",       "Quality"),
            ("Total Revenue",           f"₹{agg['total_revenue']:,.2f}",              "INR",     "Revenue"),
            ("Avg Fare per Trip",       f"₹{agg['avg_fare']:,.2f}",                  "INR",     "Revenue"),
            ("Avg Revenue per KM",      f"₹{agg['avg_revenue_per_km']:,.2f}",         "INR/km",  "Revenue"),
            ("Avg Distance per Trip",   f"{agg['avg_distance_km']:,.2f} km",          "km",      "Operations"),
            ("Avg Trip Duration",       f"{agg['avg_trip_duration_min']:,.1f} min",   "min",     "Operations"),
            ("Avg Delay",               f"{agg['avg_delay_minutes']:,.1f} min",       "min",     "Operations"),
            ("Avg Driver Rating",       f"{driver_agg['avg_driver_rating']:,.2f}",    "/5.0",    "Quality"),
        ]

        return self.spark.createDataFrame(kpi_rows, schema=schema)

    # ------------------------------------------------------------------
    # 10. Driver Segmentation
    # ------------------------------------------------------------------

    def build_driver_segmentation(self) -> DataFrame:
        """Cluster drivers into performance tiers for targeted interventions."""
        perf = self.build_driver_performance()

        return perf.withColumn(
            "segment",
            F.when(F.col("driver_efficiency_score") >= 0.75, "Champion")
             .when(F.col("driver_efficiency_score") >= 0.60, "Performer")
             .when(F.col("driver_efficiency_score") >= 0.45, "Developing")
             .otherwise("At-Risk")
        ).withColumn(
            "intervention_flag",
            F.when(F.col("segment") == "At-Risk", 1).otherwise(0)
        )

    # ------------------------------------------------------------------
    # 11. Peak Hour Dataset
    # ------------------------------------------------------------------

    def build_peak_hour_dataset(self) -> DataFrame:
        """Demand analytics by hour and time-of-day slot."""
        return (
            self.df.groupBy("trip_hour", "time_of_day")
            .agg(
                F.count("trip_id").alias("total_trips"),
                F.sum("is_completed").cast("long").alias("completed_trips"),
                F.sum(F.when(F.col("is_completed") == 1, F.col("fare_amount")).otherwise(0)).alias("revenue"),
                F.avg(F.when(F.col("is_completed") == 1, F.col("fare_amount"))).alias("avg_fare"),
                F.avg("delay_minutes").alias("avg_delay_minutes"),
            )
            .withColumn("completion_rate",
                        F.round(F.col("completed_trips") / F.col("total_trips"), 4))
            .orderBy("trip_hour")
        )
