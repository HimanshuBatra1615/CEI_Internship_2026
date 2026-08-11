"""
================================================================================
RideSharing Analytics Platform — Spark Session Factory
================================================================================
Module      : src/utils/spark_session.py
Description : Centralized Spark session factory. All pipeline stages obtain
              their SparkSession through this module. This enforces:
                  - Consistent configuration across all stages
                  - Single session reuse (Spark is a singleton per JVM)
                  - Config-driven tuning without code changes

Design Decisions:
    - SparkSession.builder.getOrCreate() is intentional — Spark enforces
      a single active session per JVM context. Multiple calls return the
      existing session, making this safe to call from any module.
    - AQE (Adaptive Query Execution) is enabled: Spark 3+ auto-optimizes
      shuffle partitions and join strategies at runtime.
    - Broadcast threshold is set to 10MB: the drivers table (~150 rows)
      qualifies for broadcast, eliminating shuffle in join operations.

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession

from src.utils.logger import get_logger, log_optimization_note
from src.utils.config_loader import ConfigLoader

logger = get_logger("utils.spark_session")


def _setup_windows_environment() -> None:
    """Ensure PySpark and Hadoop native binaries are configured correctly on Windows."""
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if os.name == "nt":
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        hadoop_dir = os.path.join(base_dir, "hadoop_home")
        bin_dir = os.path.join(hadoop_dir, "bin")
        if os.path.exists(bin_dir):
            os.environ["HADOOP_HOME"] = hadoop_dir
            if bin_dir not in os.environ.get("PATH", ""):
                os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


def get_spark_session(
    app_name: Optional[str] = None,
    config_path: str = "config/pipeline_config.yaml",
) -> SparkSession:
    """
    Build or retrieve the active SparkSession.

    The session is fully config-driven: every Spark parameter is sourced
    from pipeline_config.yaml, making environment promotion (dev → prod)
    a pure configuration change.

    Args:
        app_name:    Override application name. Uses config value if None.
        config_path: Path to the YAML pipeline configuration file.

    Returns:
        An active, configured SparkSession.
    """
    _setup_windows_environment()

    cfg = ConfigLoader(config_path)
    spark_cfg = cfg.get("spark", {})
    resolved_app_name = app_name or spark_cfg.get("app_name", "RideSharing-Analytics")

    log_optimization_note(
        logger,
        "AdaptiveQueryExecution",
        "Enabled — Spark auto-selects optimal join strategy and coalesces shuffle partitions at runtime.",
    )
    log_optimization_note(
        logger,
        "BroadcastJoin",
        "Threshold=10MB. drivers table (150 rows) will be broadcast-joined, eliminating shuffle.",
    )
    log_optimization_note(
        logger,
        "KryoSerializer",
        "Faster serialization than default Java serializer for large shuffle stages.",
    )

    builder = SparkSession.builder.appName(resolved_app_name)

    # Apply every Spark property from config
    for key, value in spark_cfg.get("configs", {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Suppress verbose Spark logs — keep terminal readable during development
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "SparkSession initialized | App=%s | Spark=%s | Python=%s",
        resolved_app_name,
        spark.version,
        os.sys.version.split()[0],
    )

    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Gracefully stop the SparkSession.

    Always called at the end of a pipeline run to release JVM resources.
    In production, this would be managed by the cluster's lifecycle hooks.
    """
    logger.info("Stopping SparkSession: %s", spark.conf.get("spark.app.name"))
    spark.stop()
