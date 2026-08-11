"""
================================================================================
RideSharing Analytics Platform — Schema Definitions
================================================================================
Module      : src/utils/schema_definitions.py
Description : PySpark StructType schemas for all three datasets.
              Schemas are declared explicitly rather than inferred to:
                  1. Catch schema drift at ingestion time (fail fast).
                  2. Eliminate the double-scan that inferSchema performs.
                  3. Serve as the authoritative data contract between teams.

Design Decisions:
    - All string columns use StringType (not VarcharType) — PySpark does not
      support Varchar length enforcement natively; validation is handled
      in the validation layer instead.
    - nullable=True for ALL raw columns because Bronze layer must ingest
      dirty data without failing. Silver layer enforces non-nullability.
    - TimestampType is NOT used in raw schemas — raw CSVs store timestamps
      as strings. Silver layer parses them with explicit format strings to
      avoid silent mis-parsing.

Author      : RideSharing Platform Engineering
================================================================================
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Raw / Bronze Schemas — Accept everything, validate nothing yet
# ---------------------------------------------------------------------------

DRIVERS_RAW_SCHEMA = StructType([
    StructField("driver_id", IntegerType(),  nullable=True),
    StructField("name",      StringType(),   nullable=True),
    StructField("city",      StringType(),   nullable=True),
    StructField("rating",    DoubleType(),   nullable=True),
])

TRIPS_RAW_SCHEMA = StructType([
    StructField("trip_id",         IntegerType(), nullable=True),
    StructField("driver_id",       IntegerType(), nullable=True),
    StructField("pickup_location", StringType(),  nullable=True),
    StructField("drop_location",   StringType(),  nullable=True),
    StructField("distance_km",     DoubleType(),  nullable=True),
    StructField("fare_amount",     DoubleType(),  nullable=True),
    StructField("trip_status",     StringType(),  nullable=True),
])

TRIP_LOGS_RAW_SCHEMA = StructType([
    StructField("log_id",            IntegerType(), nullable=True),
    StructField("trip_id",           IntegerType(), nullable=True),
    StructField("start_time",        StringType(),  nullable=True),  # Raw string
    StructField("end_time",          StringType(),  nullable=True),  # Raw string, nullable
    StructField("delay_minutes",     IntegerType(), nullable=True),
    StructField("cancellation_flag", IntegerType(), nullable=True),
])

# ---------------------------------------------------------------------------
# Silver Schemas — Post-cleaning, type-safe, nullable constraints applied
# ---------------------------------------------------------------------------

DRIVERS_SILVER_SCHEMA = StructType([
    StructField("driver_id", IntegerType(),  nullable=False),
    StructField("name",      StringType(),   nullable=False),
    StructField("city",      StringType(),   nullable=False),
    StructField("rating",    DoubleType(),   nullable=False),
])

TRIPS_SILVER_SCHEMA = StructType([
    StructField("trip_id",         IntegerType(), nullable=False),
    StructField("driver_id",       IntegerType(), nullable=False),
    StructField("pickup_location", StringType(),  nullable=False),
    StructField("drop_location",   StringType(),  nullable=False),
    StructField("distance_km",     DoubleType(),  nullable=False),
    StructField("fare_amount",     DoubleType(),  nullable=False),
    StructField("trip_status",     StringType(),  nullable=False),
    StructField("is_completed",    IntegerType(), nullable=False),   # Derived flag
    StructField("is_same_location",IntegerType(), nullable=False),   # DQ flag
])

TRIP_LOGS_SILVER_SCHEMA = StructType([
    StructField("log_id",            IntegerType(), nullable=False),
    StructField("trip_id",           IntegerType(), nullable=False),
    StructField("start_time",        StringType(),  nullable=False),
    StructField("end_time",          StringType(),  nullable=True),   # Null for cancelled
    StructField("delay_minutes",     IntegerType(), nullable=False),
    StructField("cancellation_flag", IntegerType(), nullable=False),
    StructField("trip_duration_min", DoubleType(),  nullable=True),   # Null for cancelled
])

# ---------------------------------------------------------------------------
# Metadata columns added at Bronze ingestion time
# ---------------------------------------------------------------------------
BRONZE_METADATA_FIELDS = [
    StructField("_ingestion_timestamp", StringType(), nullable=False),
    StructField("_source_file",         StringType(), nullable=False),
    StructField("_pipeline_version",    StringType(), nullable=False),
]

PIPELINE_VERSION = "1.0.0"
