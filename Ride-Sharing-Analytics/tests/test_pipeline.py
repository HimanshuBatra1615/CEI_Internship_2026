"""
================================================================================
RideSharing Analytics Platform — Test Suite
================================================================================
Module      : tests/test_pipeline.py
Description : Unit and integration tests for transformation logic, validation
              checks, and business rules. Uses PySpark local mode.

              Tests cover:
                  - Schema validation
                  - Null handling
                  - Deduplication
                  - Business rule correctness
                  - KPI calculation accuracy
                  - Edge cases

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

from src.validation.data_validator import DataValidator


from src.utils.spark_session import get_spark_session


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Create a minimal test SparkSession. Shared across all tests in session."""
    return get_spark_session(app_name="RideSharing-Test")


@pytest.fixture
def sample_drivers_df(spark):
    """Clean drivers fixture — all valid records."""
    data = [
        (1, "Rahul_1",   "Delhi",     4.6),
        (2, "Priya_2",   "Mumbai",    3.7),
        (3, "Amit_3",    "Pune",      4.2),
        (4, "Sneha_4",   "Bangalore", 5.0),
        (5, "Vikas_5",   "Hyderabad", 3.5),
    ]
    schema = StructType([
        StructField("driver_id", IntegerType(), True),
        StructField("name",      StringType(),  True),
        StructField("city",      StringType(),  True),
        StructField("rating",    DoubleType(),  True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_trips_df(spark):
    """Trips fixture with mix of Completed and Cancelled."""
    data = [
        (1, 1, "Airport",   "Mall",     15.0, 200.0, "Completed"),
        (2, 2, "Mall",      "IT Park",  10.0, 0.0,   "Cancelled"),
        (3, 3, "IT Park",   "Airport",  20.0, 300.0, "Completed"),
        (4, 4, "IT Park",   "IT Park",   8.0, 0.0,   "Cancelled"),  # Same location
        (5, 5, "Airport",   "Mall",     12.0, 150.0, "Completed"),
    ]
    schema = StructType([
        StructField("trip_id",         IntegerType(), True),
        StructField("driver_id",       IntegerType(), True),
        StructField("pickup_location", StringType(),  True),
        StructField("drop_location",   StringType(),  True),
        StructField("distance_km",     DoubleType(),  True),
        StructField("fare_amount",     DoubleType(),  True),
        StructField("trip_status",     StringType(),  True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_logs_df(spark):
    """Trip logs fixture — 1:1 with trips."""
    data = [
        (1, 1, "2025-01-01 08:00:00", "2025-01-01 08:45:00", 10, 0),
        (2, 2, "2025-01-01 09:00:00", None,                  0,  1),
        (3, 3, "2025-01-01 10:00:00", "2025-01-01 10:50:00", 15, 0),
        (4, 4, "2025-01-01 11:00:00", None,                  0,  1),
        (5, 5, "2025-01-01 12:00:00", "2025-01-01 12:40:00", 5,  0),
    ]
    schema = StructType([
        StructField("log_id",            IntegerType(), True),
        StructField("trip_id",           IntegerType(), True),
        StructField("start_time",        StringType(),  True),
        StructField("end_time",          StringType(),  True),
        StructField("delay_minutes",     IntegerType(), True),
        StructField("cancellation_flag", IntegerType(), True),
    ])
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------
# Schema & Null Tests
# ---------------------------------------------------------------------------

class TestNullChecks:

    def test_no_nulls_in_clean_dataset(self, spark, sample_drivers_df):
        """Clean drivers dataset should pass null check for all columns."""
        result = DataValidator.check_null_percentage(
            sample_drivers_df, "driver_id", "drivers", max_null_pct=0.05
        )
        assert result.passed, f"Expected PASS but got: {result.details}"

    def test_null_detection_triggers_failure(self, spark):
        """A column with 50% nulls should fail when threshold is 5%."""
        data = [(1, "A"), (2, None), (3, "C"), (4, None)]
        df = spark.createDataFrame(data, ["id", "name"])
        result = DataValidator.check_null_percentage(df, "name", "test", max_null_pct=0.05)
        assert not result.passed
        assert result.metric == pytest.approx(0.50, abs=0.01)

    def test_null_pct_metric_accuracy(self, spark):
        """Null percentage metric must be mathematically correct."""
        data = [(i, "val" if i % 4 != 0 else None) for i in range(1, 21)]
        df = spark.createDataFrame(data, ["id", "col"])
        result = DataValidator.check_null_percentage(df, "col", "test", max_null_pct=0.50)
        # 5 nulls out of 20 = 25%
        assert result.metric == pytest.approx(0.25, abs=0.01)


# ---------------------------------------------------------------------------
# Deduplication Tests
# ---------------------------------------------------------------------------

class TestDeduplication:

    def test_no_duplicates_in_clean_data(self, sample_drivers_df):
        """Clean drivers data should pass duplicate check."""
        result = DataValidator.check_duplicates(
            sample_drivers_df, ["driver_id"], "drivers", max_dup_pct=0.02
        )
        assert result.passed

    def test_duplicate_detection(self, spark):
        """Duplicate rows should be flagged correctly."""
        data = [(1, "A"), (1, "A"), (2, "B"), (3, "C")]
        df = spark.createDataFrame(data, ["id", "name"])
        result = DataValidator.check_duplicates(df, ["id"], "test", max_dup_pct=0.02)
        assert not result.passed
        assert result.metric == pytest.approx(0.25, abs=0.01)  # 1/4


# ---------------------------------------------------------------------------
# Range Validation Tests
# ---------------------------------------------------------------------------

class TestRangeChecks:

    def test_rating_within_valid_range(self, sample_drivers_df):
        """All ratings in fixture are between 1.0 and 5.0."""
        result = DataValidator.check_numeric_range(
            sample_drivers_df, "rating", "drivers", min_val=1.0, max_val=5.0
        )
        assert result.passed

    def test_negative_distance_detected(self, spark):
        """Negative distance values must trigger range violation."""
        data = [(1, -5.0), (2, 10.0), (3, 0.0)]
        df = spark.createDataFrame(data, ["trip_id", "distance_km"])
        result = DataValidator.check_numeric_range(df, "distance_km", "trips", min_val=0.0)
        assert not result.passed
        assert result.metric == 1.0  # 1 violation

    def test_rating_out_of_range_caught(self, spark):
        """Rating > 5.0 or < 1.0 must be flagged."""
        data = [(1, 6.5), (2, 0.5), (3, 4.2)]
        df = spark.createDataFrame(data, ["driver_id", "rating"])
        result = DataValidator.check_numeric_range(df, "rating", "drivers", 1.0, 5.0)
        assert not result.passed
        assert result.metric == 2.0  # Both 6.5 and 0.5 violate


# ---------------------------------------------------------------------------
# Business Rule Tests
# ---------------------------------------------------------------------------

class TestBusinessRules:

    def test_fare_zero_for_cancelled(self, sample_trips_df):
        """All cancelled trips in fixture must have fare_amount = 0."""
        result = DataValidator.check_fare_cancellation_consistency(
            sample_trips_df, "trips"
        )
        assert result.passed, f"Expected PASS: {result.details}"

    def test_fare_nonzero_on_cancelled_detected(self, spark):
        """A cancelled trip with non-zero fare is a business rule violation."""
        data = [
            (1, 1, "Airport", "Mall", 10.0, 150.0, "Cancelled"),  # INVALID
            (2, 2, "Mall",    "IT Park", 5.0, 0.0,  "Cancelled"),  # valid
        ]
        schema = StructType([
            StructField("trip_id",         IntegerType(), True),
            StructField("driver_id",       IntegerType(), True),
            StructField("pickup_location", StringType(),  True),
            StructField("drop_location",   StringType(),  True),
            StructField("distance_km",     DoubleType(),  True),
            StructField("fare_amount",     DoubleType(),  True),
            StructField("trip_status",     StringType(),  True),
        ])
        df = spark.createDataFrame(data, schema)
        result = DataValidator.check_fare_cancellation_consistency(df, "test")
        assert not result.passed
        assert result.metric == 1.0

    def test_allowed_trip_status_values(self, sample_trips_df):
        """trip_status must only contain 'Completed' or 'Cancelled'."""
        result = DataValidator.check_allowed_values(
            sample_trips_df, "trip_status", "trips", ["Completed", "Cancelled"]
        )
        assert result.passed

    def test_invalid_status_detected(self, spark):
        """Unknown status values should be flagged."""
        data = [(1, "Completed"), (2, "Pending"), (3, "Unknown")]
        df = spark.createDataFrame(data, ["trip_id", "trip_status"])
        result = DataValidator.check_allowed_values(
            df, "trip_status", "trips", ["Completed", "Cancelled"]
        )
        assert not result.passed
        assert result.metric == 2.0  # Pending + Unknown

    def test_cancellation_consistency(self, sample_trips_df, sample_logs_df):
        """cancellation_flag must align with trip_status across both tables."""
        result = DataValidator.check_cancellation_consistency(
            sample_trips_df, sample_logs_df, "trips+logs"
        )
        assert result.passed


# ---------------------------------------------------------------------------
# Referential Integrity Tests
# ---------------------------------------------------------------------------

class TestReferentialIntegrity:

    def test_all_trips_have_valid_driver(self, sample_trips_df, sample_drivers_df):
        """All driver_ids in trips must exist in drivers."""
        result = DataValidator.check_referential_integrity(
            sample_trips_df, sample_drivers_df, "driver_id", "driver_id", "trips"
        )
        assert result.passed

    def test_orphan_driver_detected(self, spark, sample_trips_df):
        """Trips with unknown driver_id should be detected as orphans."""
        # Remove driver_id=5 from drivers — trip 5 becomes orphan
        drivers_data = [(1, "A", "Delhi", 4.0), (2, "B", "Mumbai", 3.5),
                        (3, "C", "Pune", 4.5), (4, "D", "Bangalore", 4.2)]
        drivers_df = spark.createDataFrame(
            drivers_data, ["driver_id", "name", "city", "rating"]
        )
        result = DataValidator.check_referential_integrity(
            sample_trips_df, drivers_df, "driver_id", "driver_id", "trips"
        )
        assert not result.passed
        assert result.metric == 1.0  # driver_id=5 is orphan


# ---------------------------------------------------------------------------
# Timestamp Tests
# ---------------------------------------------------------------------------

class TestTimestampChecks:

    def test_valid_timestamps_pass(self, sample_logs_df):
        """All end_times after start_times should pass the order check."""
        result = DataValidator.check_timestamp_order(
            sample_logs_df, "start_time", "end_time", "trip_logs"
        )
        assert result.passed

    def test_inverted_timestamps_detected(self, spark):
        """end_time before start_time must be flagged."""
        data = [
            (1, "2025-01-01 10:00:00", "2025-01-01 09:00:00"),  # INVERTED
            (2, "2025-01-01 08:00:00", "2025-01-01 09:00:00"),  # Valid
        ]
        df = spark.createDataFrame(data, ["log_id", "start_time", "end_time"])
        result = DataValidator.check_timestamp_order(df, "start_time", "end_time", "test")
        assert not result.passed
        assert result.metric == 1.0


# ---------------------------------------------------------------------------
# Edge Case Tests
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_empty_dataframe_null_check(self, spark):
        """Empty DataFrame should return failed check (meaningless data)."""
        df = spark.createDataFrame([], StructType([
            StructField("id", IntegerType(), True),
            StructField("val", StringType(), True),
        ]))
        result = DataValidator.check_null_percentage(df, "val", "empty", 0.05)
        assert not result.passed  # empty = fails

    def test_all_nulls_in_column(self, spark):
        """A column with 100% nulls should fail null check."""
        data = [(1, None), (2, None), (3, None)]
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("val", StringType(), True),
        ])
        df = spark.createDataFrame(data, schema=schema)
        result = DataValidator.check_null_percentage(df, "val", "test", 0.05)
        assert not result.passed
        assert result.metric == pytest.approx(1.0)

    def test_zero_distance_trip(self, spark):
        """Zero-distance trips are technically valid (driver at pickup) — should pass range."""
        data = [(1, 0.0), (2, 5.5)]
        df = spark.createDataFrame(data, ["trip_id", "distance_km"])
        result = DataValidator.check_numeric_range(df, "distance_km", "trips", min_val=0.0)
        assert result.passed
