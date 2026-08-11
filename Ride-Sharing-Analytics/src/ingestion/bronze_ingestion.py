"""
================================================================================
RideSharing Analytics Platform — Bronze Layer Ingestion Engine
================================================================================
Module      : src/ingestion/bronze_ingestion.py
Description : Reads raw CSV files and writes them to the Bronze layer as Parquet
              with metadata columns appended. No business transformations.
              Bronze is the immutable record of what was received.

Design Decisions:
    - Bronze = raw data as-is. If the source sends garbage, Bronze stores it.
      The Silver layer handles quality. This separation allows root-cause analysis
      of data issues without losing the original ingested record.
    - Explicit schema prevents Spark from scanning entire file for type inference
      (which would double I/O on large datasets).
    - Metadata columns (_ingestion_timestamp, _source_file, _pipeline_version)
      are critical for data lineage — a non-negotiable requirement in production.
    - Parquet output (vs. CSV) enables column pruning and predicate pushdown in
      all downstream layers, making Silver/Gold reads 3-10x faster.
    - coalesce(1) used here intentionally: for small datasets, single-file output
      avoids creating hundreds of tiny Parquet part files. At Uber scale, this
      would be removed — Spark's default parallelism would handle file sizing.

Optimization Notes:
    - CACHE: Not applied at Bronze — raw data is read once, transformed minimally,
      and written. Caching ephemeral data wastes memory.
    - BROADCAST: Not applicable in Bronze (no joins).
    - PARTITION: Not applied — Bronze stores full raw snapshots. Partitioning
      by ingest date would be added in production for incremental loads.

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.utils.logger import (
    get_logger,
    log_pipeline_start,
    log_pipeline_end,
    log_optimization_note,
)
from src.utils.schema_definitions import (
    DRIVERS_RAW_SCHEMA,
    TRIPS_RAW_SCHEMA,
    TRIP_LOGS_RAW_SCHEMA,
    PIPELINE_VERSION,
)
from src.utils.config_loader import ConfigLoader

logger = get_logger("ingestion.bronze")


class BronzeIngestionEngine:
    """
    Orchestrates the ingestion of raw CSV files into the Bronze Parquet layer.

    Each dataset is handled by a dedicated private method, making the class
    easily extensible as new data sources are onboarded.

    Attributes:
        spark:  Active SparkSession.
        cfg:    Loaded pipeline configuration.
        paths:  Resolved path configuration section.
    """

    def __init__(self, spark: SparkSession, config_path: str = "config/pipeline_config.yaml") -> None:
        self.spark = spark
        self.cfg = ConfigLoader(config_path)
        self.paths = self.cfg.get_section("paths")

        log_optimization_note(
            logger,
            "ExplicitSchema",
            "All CSV reads use predefined StructType schemas. "
            "Eliminates inferSchema double-pass I/O overhead.",
        )

    # ------------------------------------------------------------------
    # Public Interface
    # ------------------------------------------------------------------

    def run_all(self) -> dict[str, int]:
        """
        Execute Bronze ingestion for all three datasets.

        Returns:
            Dict mapping dataset name to ingested row count.
        """
        logger.info("╔══════════════════════════════════════════╗")
        logger.info("║       BRONZE LAYER INGESTION STARTED     ║")
        logger.info("╚══════════════════════════════════════════╝")

        results = {
            "drivers":   self.ingest_drivers(),
            "trips":     self.ingest_trips(),
            "trip_logs": self.ingest_trip_logs(),
        }

        logger.info(
            "Bronze ingestion complete | drivers=%d | trips=%d | trip_logs=%d",
            results["drivers"], results["trips"], results["trip_logs"],
        )
        return results

    def ingest_drivers(self) -> int:
        """Ingest drivers.csv → Bronze Parquet."""
        return self._ingest_dataset(
            raw_path=self.paths["raw"]["drivers"],
            output_path=self.paths["bronze"]["drivers"],
            schema=DRIVERS_RAW_SCHEMA,
            dataset_name="drivers",
        )

    def ingest_trips(self) -> int:
        """Ingest trips.csv → Bronze Parquet."""
        return self._ingest_dataset(
            raw_path=self.paths["raw"]["trips"],
            output_path=self.paths["bronze"]["trips"],
            schema=TRIPS_RAW_SCHEMA,
            dataset_name="trips",
        )

    def ingest_trip_logs(self) -> int:
        """Ingest trip_logs.csv → Bronze Parquet."""
        return self._ingest_dataset(
            raw_path=self.paths["raw"]["trip_logs"],
            output_path=self.paths["bronze"]["trip_logs"],
            schema=TRIP_LOGS_RAW_SCHEMA,
            dataset_name="trip_logs",
        )

    # ------------------------------------------------------------------
    # Private Implementation
    # ------------------------------------------------------------------

    def _ingest_dataset(
        self,
        raw_path: str,
        output_path: str,
        schema,
        dataset_name: str,
    ) -> int:
        """
        Generic CSV-to-Bronze ingestion routine.

        Steps:
            1. Read CSV with explicit schema (no type inference).
            2. Append lineage metadata columns.
            3. Validate minimum row count.
            4. Write Parquet with overwrite semantics (idempotent).

        Args:
            raw_path:     Path to source CSV file.
            output_path:  Destination Bronze Parquet path.
            schema:       Expected PySpark StructType.
            dataset_name: Human-readable name used in log messages.

        Returns:
            Row count of the ingested dataset.

        Raises:
            FileNotFoundError: If the source CSV does not exist.
            ValueError:        If ingested row count is below the configured minimum.
        """
        log_pipeline_start(logger, stage="Bronze", dataset=dataset_name)

        # ----------------------------------------------------------
        # 1. Validate source file existence before Spark I/O
        # ----------------------------------------------------------
        source_path = Path(raw_path)
        if not source_path.exists():
            raise FileNotFoundError(
                f"Source file not found: {source_path.resolve()}\n"
                f"Expected raw file at: data/raw/{source_path.name}"
            )

        source_filename = source_path.name
        ingestion_ts = datetime.now(timezone.utc).isoformat()

        logger.info("Reading CSV: %s", source_path.resolve())

        # ----------------------------------------------------------
        # 2. Read with explicit schema — no inferSchema double scan
        # ----------------------------------------------------------
        df: DataFrame = (
            self.spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")        # Don't fail on bad records
            .option("nullValue", "")             # Empty strings → null
            .option("emptyValue", "")
            .schema(schema)
            .csv(str(source_path))
        )

        # ----------------------------------------------------------
        # 3. Append Bronze lineage metadata
        # ----------------------------------------------------------
        df_with_meta = (
            df
            .withColumn("_ingestion_timestamp", F.lit(ingestion_ts))
            .withColumn("_source_file",         F.lit(source_filename))
            .withColumn("_pipeline_version",    F.lit(PIPELINE_VERSION))
        )

        # ----------------------------------------------------------
        # 4. Minimum row count validation (eager — triggers action)
        # ----------------------------------------------------------
        min_rows = self.cfg.get("quality.min_row_count", 10)
        row_count = df_with_meta.count()

        if row_count < min_rows:
            raise ValueError(
                f"Bronze validation failed for '{dataset_name}': "
                f"ingested {row_count} rows, minimum expected {min_rows}."
            )
        logger.info("Row count validated: %d rows (min=%d)", row_count, min_rows)

        # ----------------------------------------------------------
        # 5. Write Parquet — overwrite for idempotent reruns
        #    coalesce(1) prevents small-file explosion for this dataset size
        # ----------------------------------------------------------
        log_optimization_note(
            logger,
            "coalesce(1)",
            f"Dataset '{dataset_name}' is small (~{row_count} rows). Single output file "
            "avoids small-file problem. Remove coalesce in production for large datasets.",
        )

        (
            df_with_meta
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(output_path)
        )

        logger.info("Bronze Parquet written: %s", output_path)
        log_pipeline_end(logger, stage="Bronze", dataset=dataset_name, row_count=row_count)

        return row_count
