"""
================================================================================
RideSharing Analytics Platform — Master Pipeline Orchestrator
================================================================================
Module      : run_pipeline.py
Description : Single entry point for the entire end-to-end pipeline.
              Orchestrates Bronze → Silver → Gold execution with full
              error handling, timing, and reporting.

Usage:
    python run_pipeline.py                        # Full pipeline
    python run_pipeline.py --stage bronze         # Bronze only
    python run_pipeline.py --stage silver         # Silver only
    python run_pipeline.py --stage gold           # Gold only
    python run_pipeline.py --stage validate       # Validation only

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

# Ensure src is importable
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader
from src.utils.spark_session import get_spark_session, stop_spark_session
from src.utils.report_generator import generate_all_reports
from src.ingestion.bronze_ingestion import BronzeIngestionEngine
from src.transformation.silver_transformation import SilverTransformationEngine
from src.analytics.gold_analytics import GoldAnalyticsEngine
from src.validation.data_validator import DataValidator
from src.optimization.spark_optimizations import SparkOptimizationShowcase

logger = get_logger("pipeline.orchestrator")

CONFIG_PATH = "config/pipeline_config.yaml"


def _copy_raw_data() -> None:
    """
    Copy source CSVs from project root to data/raw/ if not already present.
    In production, this step would be replaced by an S3/ADLS ingestion trigger.
    """
    import shutil
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    for csv_name in ["drivers.csv", "trips.csv", "trip_logs.csv"]:
        src = Path(csv_name)
        dst = raw_dir / csv_name
        if src.exists() and not dst.exists():
            shutil.copy2(src, dst)
            logger.info("Copied %s → %s", src, dst)
        elif dst.exists():
            logger.info("Raw file already present: %s", dst)
        else:
            logger.warning("Source file not found: %s", src)


def run_bronze(engine: BronzeIngestionEngine) -> dict:
    """Execute Bronze layer and return row counts."""
    logger.info("▶  Starting Bronze Layer")
    t0 = time.time()
    result = engine.run_all()
    elapsed = time.time() - t0
    logger.info("✅ Bronze complete in %.2fs | %s", elapsed, result)
    return result


def run_silver(spark, config_path: str) -> tuple:
    """Execute Silver layer and return engine + enriched df."""
    logger.info("▶  Starting Silver Layer")
    t0 = time.time()
    engine = SilverTransformationEngine(spark, config_path)
    result = engine.run_all()
    elapsed = time.time() - t0
    logger.info("✅ Silver complete in %.2fs | %s", elapsed, result)
    return engine, result


def run_validation(spark, config_path: str) -> None:
    """Run data quality checks on Silver datasets."""
    logger.info("▶  Starting Validation Layer")
    cfg = ConfigLoader(config_path)
    paths = cfg.get_section("paths")
    quality_cfg = cfg.get_section("quality")

    drivers_df  = spark.read.parquet(paths["silver"]["drivers"])
    trips_df    = spark.read.parquet(paths["silver"]["trips"])
    logs_df     = spark.read.parquet(paths["silver"]["trip_logs"])

    reports = [
        DataValidator.run_drivers_checks(drivers_df, quality_cfg),
        DataValidator.run_trips_checks(trips_df, quality_cfg),
        DataValidator.run_trip_logs_checks(logs_df, quality_cfg),
    ]

    # Cross-dataset checks
    trips_clean = trips_df.filter(~trips_df["trip_status"].isNull())
    ref_check = DataValidator.check_referential_integrity(
        trips_df, drivers_df, "driver_id", "driver_id", "trips→drivers"
    )
    cancel_check = DataValidator.check_cancellation_consistency(
        trips_df, logs_df, "trips+logs"
    )

    logger.info("Referential integrity: %s", "PASS" if ref_check.passed else "FAIL")
    logger.info("Cancellation consistency: %s", "PASS" if cancel_check.passed else "FAIL")

    for r in reports:
        logger.info("DQ Summary | %s | passed=%d | failed=%d",
                    r.dataset, r.passed_count, r.failed_count)


def run_gold(spark, enriched_df, config_path: str) -> dict:
    """Execute Gold layer and return row counts."""
    logger.info("▶  Starting Gold Layer")
    t0 = time.time()
    engine = GoldAnalyticsEngine(spark, enriched_df, config_path)
    result = engine.run_all()
    elapsed = time.time() - t0
    logger.info("✅ Gold complete in %.2fs | %s", elapsed, result)
    return result


def run_full_pipeline() -> None:
    """Execute the complete Bronze → Silver → Gold pipeline."""

    start_time = time.time()
    run_ts = datetime.now(timezone.utc).isoformat()

    logger.info("╔═══════════════════════════════════════════════════════════╗")
    logger.info("║   RIDESHARING ANALYTICS PLATFORM — FULL PIPELINE START   ║")
    logger.info("║   Run Timestamp : %-40s ║", run_ts)
    logger.info("╚═══════════════════════════════════════════════════════════╝")

    spark = None
    try:
        # 0. Environment setup
        _copy_raw_data()
        Path("logs").mkdir(exist_ok=True)
        for layer in ["data/bronze", "data/silver", "data/gold"]:
            Path(layer).mkdir(parents=True, exist_ok=True)

        # 1. Spark Session
        spark = get_spark_session(config_path=CONFIG_PATH)

        # 3. Bronze Layer
        bronze_engine = BronzeIngestionEngine(spark, CONFIG_PATH)
        bronze_results = run_bronze(bronze_engine)

        # 4. Silver Layer
        silver_engine, silver_results = run_silver(spark, CONFIG_PATH)

        # 2. Optimization documentation (runs after Silver dataset is generated)
        opt_showcase = SparkOptimizationShowcase(spark)
        opt_showcase.run_all_documentation("data/silver/trips")

        # 5. Validation
        run_validation(spark, CONFIG_PATH)

        # 6. Gold Layer (uses cached enriched_df from Silver)
        enriched_df = silver_engine.get_enriched_df()
        gold_results = run_gold(spark, enriched_df, CONFIG_PATH)

        # 7. Release cache
        enriched_df.unpersist()
        logger.info("Enriched df cache released.")

        # 8. Report & Chart Generation
        logger.info("▶  Generating Visualizations & Markdown Report")
        report_file = generate_all_reports(spark)
        logger.info("Report created: %s", report_file)

        total_time = time.time() - start_time
        logger.info("╔═══════════════════════════════════════════════════════════╗")
        logger.info("║         PIPELINE COMPLETE — SUCCESS                       ║")
        logger.info("║   Total Runtime : %-40.2fs ║", total_time)
        logger.info("║   Bronze Rows   : drivers=%d trips=%d logs=%d             ║",
                    bronze_results.get("drivers", 0),
                    bronze_results.get("trips", 0),
                    bronze_results.get("trip_logs", 0))
        logger.info("║   Gold Tables   : %d tables generated                     ║",
                    len(gold_results))
        logger.info("╚═══════════════════════════════════════════════════════════╝")

    except FileNotFoundError as exc:
        logger.error("PIPELINE FAILED — Missing file: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("PIPELINE FAILED — Unexpected error:\n%s", traceback.format_exc())
        sys.exit(1)
    finally:
        if spark:
            stop_spark_session(spark)


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RideSharing Analytics Platform — Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                  # Full pipeline
  python run_pipeline.py --stage bronze   # Bronze only
  python run_pipeline.py --stage silver   # Silver only
  python run_pipeline.py --stage gold     # Gold only
  python run_pipeline.py --stage validate # Validation only
        """
    )
    parser.add_argument(
        "--stage",
        choices=["full", "bronze", "silver", "gold", "validate"],
        default="full",
        help="Pipeline stage to execute (default: full)",
    )
    parser.add_argument(
        "--config",
        default=CONFIG_PATH,
        help=f"Path to pipeline config YAML (default: {CONFIG_PATH})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.stage == "full":
        run_full_pipeline()
    else:
        # Stage-specific execution
        _copy_raw_data()
        Path("logs").mkdir(exist_ok=True)
        spark = get_spark_session(config_path=args.config)
        try:
            if args.stage == "bronze":
                run_bronze(BronzeIngestionEngine(spark, args.config))
            elif args.stage == "silver":
                run_silver(spark, args.config)
            elif args.stage == "validate":
                # Silver must exist for validation
                run_validation(spark, args.config)
            elif args.stage == "gold":
                # Silver enriched must exist
                silver_engine = SilverTransformationEngine(spark, args.config)
                enriched_df = silver_engine.get_enriched_df()
                run_gold(spark, enriched_df, args.config)
        finally:
            stop_spark_session(spark)
