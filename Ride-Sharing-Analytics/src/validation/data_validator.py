"""
================================================================================
RideSharing Analytics Platform — Data Validation Framework
================================================================================
Module      : src/validation/data_validator.py
Description : Reusable validation engine that runs schema, null, duplicate,
              referential integrity, and business rule checks against any
              PySpark DataFrame. Produces a structured DataQualityReport.

Design Decisions:
    - Validator is stateless (all methods are pure functions over DataFrames).
      This makes it trivially testable and reusable across all pipeline layers.
    - ValidationResult is a dataclass (not a dict) — named fields prevent
      typo bugs and make IDE autocomplete available to all callers.
    - Every check is logged as a structured DQ_EVENT so a downstream dashboard
      can pick up quality trends over time.
    - We deliberately do NOT drop bad records here. The validator flags them;
      the transformation layer decides whether to drop, quarantine, or impute.

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger, log_data_quality_event

logger = get_logger("validation.data_validator")


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    """Result of a single data quality check."""
    check_name:  str
    dataset:     str
    passed:      bool
    metric:      Optional[float] = None
    threshold:   Optional[float] = None
    details:     str = ""


@dataclass
class DataQualityReport:
    """
    Aggregated report for a dataset after all checks are run.
    Written to reports/ at the end of each Silver/Bronze stage.
    """
    dataset:      str
    total_rows:   int
    checks:       List[CheckResult] = field(default_factory=list)
    passed_count: int = 0
    failed_count: int = 0

    def add_check(self, result: CheckResult) -> None:
        """Append a check result and update pass/fail counters."""
        self.checks.append(result)
        if result.passed:
            self.passed_count += 1
        else:
            self.failed_count += 1

    @property
    def overall_passed(self) -> bool:
        return self.failed_count == 0

    def summary_str(self) -> str:
        lines = [
            f"\n{'═'*60}",
            f"  DATA QUALITY REPORT — {self.dataset.upper()}",
            f"{'═'*60}",
            f"  Total Rows  : {self.total_rows:,}",
            f"  Checks Run  : {len(self.checks)}",
            f"  ✅ Passed   : {self.passed_count}",
            f"  ❌ Failed   : {self.failed_count}",
            f"  Overall     : {'PASS' if self.overall_passed else 'FAIL'}",
            f"{'─'*60}",
        ]
        for chk in self.checks:
            status = "✅" if chk.passed else "❌"
            metric_str = f" | metric={chk.metric:.4f}" if chk.metric is not None else ""
            threshold_str = f" | threshold={chk.threshold}" if chk.threshold is not None else ""
            lines.append(f"  {status} {chk.check_name:<35}{metric_str}{threshold_str}")
            if chk.details:
                lines.append(f"       ↳ {chk.details}")
        lines.append(f"{'═'*60}\n")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Validator Engine
# ---------------------------------------------------------------------------

class DataValidator:
    """
    Stateless data quality validation engine.

    All check methods accept a DataFrame and return a CheckResult.
    The orchestrate_checks() method composes all checks into a report.
    """

    # ------------------------------------------------------------------
    # Null Checks
    # ------------------------------------------------------------------

    @staticmethod
    def check_null_percentage(
        df: DataFrame,
        column: str,
        dataset: str,
        max_null_pct: float = 0.05,
    ) -> CheckResult:
        """
        Verify that null values in `column` do not exceed `max_null_pct`.

        We use COUNT(*) - COUNT(col) rather than COUNT_IF(col IS NULL) for
        compatibility with older Spark versions.
        """
        total = df.count()
        if total == 0:
            return CheckResult(
                check_name=f"null_pct:{column}",
                dataset=dataset,
                passed=False,
                details="Dataset is empty — null check meaningless.",
            )

        null_count = df.filter(F.col(column).isNull()).count()
        null_pct = null_count / total
        passed = null_pct <= max_null_pct

        result = CheckResult(
            check_name=f"null_pct:{column}",
            dataset=dataset,
            passed=passed,
            metric=null_pct,
            threshold=max_null_pct,
            details=f"{null_count:,} nulls in column '{column}' ({null_pct:.2%})",
        )
        log_data_quality_event(
            logger, f"null_pct:{column}", dataset, passed,
            {"null_count": null_count, "null_pct": round(null_pct, 4)},
        )
        return result

    # ------------------------------------------------------------------
    # Duplicate Checks
    # ------------------------------------------------------------------

    @staticmethod
    def check_duplicates(
        df: DataFrame,
        key_columns: List[str],
        dataset: str,
        max_dup_pct: float = 0.02,
    ) -> CheckResult:
        """
        Verify that duplicate rows on `key_columns` do not exceed `max_dup_pct`.
        """
        total = df.count()
        distinct = df.dropDuplicates(key_columns).count()
        dup_count = total - distinct
        dup_pct = dup_count / total if total > 0 else 0.0
        passed = dup_pct <= max_dup_pct

        result = CheckResult(
            check_name=f"duplicates:{'+'.join(key_columns)}",
            dataset=dataset,
            passed=passed,
            metric=dup_pct,
            threshold=max_dup_pct,
            details=f"{dup_count:,} duplicate rows on key {key_columns}",
        )
        log_data_quality_event(
            logger, f"duplicates:{key_columns}", dataset, passed,
            {"dup_count": dup_count, "dup_pct": round(dup_pct, 4)},
        )
        return result

    # ------------------------------------------------------------------
    # Range Checks
    # ------------------------------------------------------------------

    @staticmethod
    def check_numeric_range(
        df: DataFrame,
        column: str,
        dataset: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> CheckResult:
        """Verify all non-null values in `column` fall within [min_val, max_val]."""
        conditions = []
        if min_val is not None:
            conditions.append(F.col(column) < min_val)
        if max_val is not None:
            conditions.append(F.col(column) > max_val)

        if not conditions:
            return CheckResult(
                check_name=f"range:{column}",
                dataset=dataset,
                passed=True,
                details="No range bounds specified — check skipped.",
            )

        combined = conditions[0]
        for c in conditions[1:]:
            combined = combined | c

        violation_count = df.filter(F.col(column).isNotNull() & combined).count()
        passed = violation_count == 0

        result = CheckResult(
            check_name=f"range:{column}",
            dataset=dataset,
            passed=passed,
            metric=float(violation_count),
            details=f"{violation_count:,} values outside [{min_val}, {max_val}]",
        )
        log_data_quality_event(
            logger, f"range:{column}", dataset, passed,
            {"violations": violation_count, "min": min_val, "max": max_val},
        )
        return result

    # ------------------------------------------------------------------
    # Referential Integrity
    # ------------------------------------------------------------------

    @staticmethod
    def check_referential_integrity(
        child_df: DataFrame,
        parent_df: DataFrame,
        foreign_key: str,
        primary_key: str,
        dataset: str,
    ) -> CheckResult:
        """
        Verify that all foreign key values in child_df exist in parent_df.

        Uses a LEFT ANTI JOIN — the most efficient Spark approach for
        detecting orphaned foreign keys without a full cross-join.
        """
        orphan_count = (
            child_df.select(F.col(foreign_key))
            .join(
                parent_df.select(F.col(primary_key)),
                on=child_df[foreign_key] == parent_df[primary_key],
                how="left_anti",
            )
            .count()
        )
        passed = orphan_count == 0

        result = CheckResult(
            check_name=f"ref_integrity:{foreign_key}→{primary_key}",
            dataset=dataset,
            passed=passed,
            metric=float(orphan_count),
            details=f"{orphan_count:,} orphaned '{foreign_key}' values not found in parent.",
        )
        log_data_quality_event(
            logger, f"ref_integrity:{foreign_key}", dataset, passed,
            {"orphan_count": orphan_count},
        )
        return result

    # ------------------------------------------------------------------
    # Value Set Checks
    # ------------------------------------------------------------------

    @staticmethod
    def check_allowed_values(
        df: DataFrame,
        column: str,
        dataset: str,
        allowed_values: List[Any],
    ) -> CheckResult:
        """Verify `column` contains only values from `allowed_values`."""
        violation_count = df.filter(
            F.col(column).isNotNull() & ~F.col(column).isin(allowed_values)
        ).count()
        passed = violation_count == 0

        result = CheckResult(
            check_name=f"allowed_values:{column}",
            dataset=dataset,
            passed=passed,
            metric=float(violation_count),
            details=f"{violation_count:,} values outside allowed set {allowed_values}",
        )
        log_data_quality_event(
            logger, f"allowed_values:{column}", dataset, passed,
            {"violations": violation_count, "allowed": allowed_values},
        )
        return result

    # ------------------------------------------------------------------
    # Business Rule Checks
    # ------------------------------------------------------------------

    @staticmethod
    def check_cancellation_consistency(
        trips_df: DataFrame,
        trip_logs_df: DataFrame,
        dataset: str,
    ) -> CheckResult:
        """
        Verify that cancellation_flag in trip_logs matches trip_status in trips.
        Cancelled trips (trip_status='Cancelled') should have cancellation_flag=1.
        Completed trips should have cancellation_flag=0.
        """
        joined = trips_df.join(trip_logs_df, on="trip_id", how="inner")
        inconsistent = joined.filter(
            (
                (F.col("trip_status") == "Cancelled") & (F.col("cancellation_flag") != 1)
            ) | (
                (F.col("trip_status") == "Completed") & (F.col("cancellation_flag") != 0)
            )
        ).count()
        passed = inconsistent == 0

        result = CheckResult(
            check_name="cancellation_consistency",
            dataset=dataset,
            passed=passed,
            metric=float(inconsistent),
            details=f"{inconsistent:,} records where trip_status and cancellation_flag disagree.",
        )
        log_data_quality_event(
            logger, "cancellation_consistency", dataset, passed,
            {"inconsistent_count": inconsistent},
        )
        return result

    @staticmethod
    def check_fare_cancellation_consistency(
        df: DataFrame,
        dataset: str,
    ) -> CheckResult:
        """
        Validate that cancelled trips have fare_amount = 0.
        A non-zero fare on a cancelled trip is a business rule violation.
        """
        violations = df.filter(
            (F.col("trip_status") == "Cancelled") & (F.col("fare_amount") > 0)
        ).count()
        passed = violations == 0

        result = CheckResult(
            check_name="fare_cancellation_consistency",
            dataset=dataset,
            passed=passed,
            metric=float(violations),
            details=f"{violations:,} cancelled trips with non-zero fare (business rule violation).",
        )
        log_data_quality_event(
            logger, "fare_cancellation_consistency", dataset, passed,
            {"violations": violations},
        )
        return result

    # ------------------------------------------------------------------
    # Timestamp Checks
    # ------------------------------------------------------------------

    @staticmethod
    def check_timestamp_order(
        df: DataFrame,
        start_col: str,
        end_col: str,
        dataset: str,
    ) -> CheckResult:
        """
        Verify that end_time is always after start_time for non-null end_times.
        Trips where end < start indicate data entry errors or timezone issues.
        """
        from pyspark.sql.functions import to_timestamp
        violations = df.filter(
            F.col(end_col).isNotNull()
        ).filter(
            F.to_timestamp(F.col(end_col)) <= F.to_timestamp(F.col(start_col))
        ).count()
        passed = violations == 0

        result = CheckResult(
            check_name=f"timestamp_order:{start_col}<{end_col}",
            dataset=dataset,
            passed=passed,
            metric=float(violations),
            details=f"{violations:,} records where {end_col} ≤ {start_col}.",
        )
        log_data_quality_event(
            logger, "timestamp_order", dataset, passed,
            {"violations": violations},
        )
        return result

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    @staticmethod
    def run_drivers_checks(
        df: DataFrame,
        cfg_quality: Dict[str, Any],
    ) -> DataQualityReport:
        """Run all validation checks for the drivers dataset."""
        report = DataQualityReport(dataset="drivers", total_rows=df.count())
        max_null = cfg_quality.get("max_null_pct", 0.05)
        max_dup  = cfg_quality.get("max_duplicate_pct", 0.02)

        for col in ["driver_id", "name", "city", "rating"]:
            report.add_check(DataValidator.check_null_percentage(df, col, "drivers", max_null))

        report.add_check(DataValidator.check_duplicates(df, ["driver_id"], "drivers", max_dup))
        report.add_check(DataValidator.check_numeric_range(df, "rating", "drivers", 1.0, 5.0))
        report.add_check(DataValidator.check_allowed_values(
            df, "city", "drivers",
            ["Delhi", "Mumbai", "Pune", "Bangalore", "Hyderabad"]
        ))

        logger.info(report.summary_str())
        return report

    @staticmethod
    def run_trips_checks(
        df: DataFrame,
        cfg_quality: Dict[str, Any],
    ) -> DataQualityReport:
        """Run all validation checks for the trips dataset."""
        report = DataQualityReport(dataset="trips", total_rows=df.count())
        max_null = cfg_quality.get("max_null_pct", 0.05)
        max_dup  = cfg_quality.get("max_duplicate_pct", 0.02)

        for col in ["trip_id", "driver_id", "pickup_location", "drop_location",
                    "distance_km", "fare_amount", "trip_status"]:
            report.add_check(DataValidator.check_null_percentage(df, col, "trips", max_null))

        report.add_check(DataValidator.check_duplicates(df, ["trip_id"], "trips", max_dup))
        report.add_check(DataValidator.check_numeric_range(df, "distance_km", "trips", 0.0, 500.0))
        report.add_check(DataValidator.check_numeric_range(df, "fare_amount",  "trips", 0.0))
        report.add_check(DataValidator.check_allowed_values(
            df, "trip_status", "trips", ["Completed", "Cancelled"]
        ))
        report.add_check(DataValidator.check_fare_cancellation_consistency(df, "trips"))

        logger.info(report.summary_str())
        return report

    @staticmethod
    def run_trip_logs_checks(
        df: DataFrame,
        cfg_quality: Dict[str, Any],
    ) -> DataQualityReport:
        """Run all validation checks for the trip_logs dataset."""
        report = DataQualityReport(dataset="trip_logs", total_rows=df.count())
        max_null = cfg_quality.get("max_null_pct", 0.05)
        max_dup  = cfg_quality.get("max_duplicate_pct", 0.02)

        for col in ["log_id", "trip_id", "start_time", "delay_minutes", "cancellation_flag"]:
            report.add_check(DataValidator.check_null_percentage(df, col, "trip_logs", max_null))

        report.add_check(DataValidator.check_duplicates(df, ["log_id"], "trip_logs", max_dup))
        report.add_check(DataValidator.check_duplicates(df, ["trip_id"], "trip_logs", max_dup))
        report.add_check(DataValidator.check_numeric_range(df, "delay_minutes", "trip_logs", 0, 120))
        report.add_check(DataValidator.check_allowed_values(
            df, "cancellation_flag", "trip_logs", [0, 1]
        ))
        report.add_check(DataValidator.check_timestamp_order(
            df, "start_time", "end_time", "trip_logs"
        ))

        logger.info(report.summary_str())
        return report
