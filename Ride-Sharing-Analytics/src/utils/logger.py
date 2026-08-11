"""
================================================================================
RideSharing Analytics Platform — Logging Utility
================================================================================
Module      : src/utils/logger.py
Description : Centralized structured logging factory for the entire platform.
              Every pipeline stage uses this to produce consistent, structured
              logs consumable by log aggregators (e.g. Splunk, Datadog, ELK).

Design Decisions:
    - Single factory pattern avoids duplicate handlers across modules.
    - Named loggers per module enable fine-grained filtering.
    - File handler persists logs even if the terminal session is lost.
    - DataQualityEvent structured log enables downstream alerting.

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

import logging
import os
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any, Dict


# ---------------------------------------------------------------------------
# Module-level sentinel to prevent duplicate handler registration
# ---------------------------------------------------------------------------
_REGISTERED_LOGGERS: set[str] = set()

# Default log directory — overridden by config at runtime
_DEFAULT_LOG_DIR = Path("logs")
_DEFAULT_LOG_FILE = _DEFAULT_LOG_DIR / "pipeline.log"


def get_logger(
    name: str,
    log_file: Optional[str | Path] = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Factory function returning a named, deduplicated logger.

    Each unique `name` receives exactly one set of handlers regardless of
    how many times get_logger() is called. This prevents log duplication
    in long-running Spark driver processes.

    Args:
        name:     Module or component name (e.g., 'bronze.ingestion').
        log_file: Absolute or relative path to the log file. Defaults to
                  logs/pipeline.log if not provided.
        level:    Python logging level (default: INFO).

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)

    # Guard: only add handlers once per named logger
    if name in _REGISTERED_LOGGERS:
        return logger

    logger.setLevel(level)
    logger.propagate = False  # Avoid bubbling to root logger

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(name)-40s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ------------------------------------------------------------------
    # Console Handler — Always present
    # ------------------------------------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    # ------------------------------------------------------------------
    # File Handler — Persistent across runs
    # ------------------------------------------------------------------
    target_log = Path(log_file) if log_file else _DEFAULT_LOG_FILE
    target_log.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(target_log, mode="a", encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    _REGISTERED_LOGGERS.add(name)
    return logger


def log_pipeline_start(logger: logging.Logger, stage: str, dataset: str) -> None:
    """Emit a standardized pipeline-start event."""
    logger.info(
        "=" * 70 + f"\n{'':>12}PIPELINE STAGE START"
        f"\n{'':>12}Stage   : {stage}"
        f"\n{'':>12}Dataset : {dataset}"
        f"\n{'':>12}UTC Time: {datetime.now(timezone.utc).isoformat()}"
        f"\n{'':>12}" + "=" * 70
    )


def log_pipeline_end(logger: logging.Logger, stage: str, dataset: str, row_count: int) -> None:
    """Emit a standardized pipeline-end event with row count."""
    logger.info(
        f"PIPELINE STAGE COMPLETE | Stage={stage} | Dataset={dataset} | Rows={row_count:,}"
    )


def log_data_quality_event(
    logger: logging.Logger,
    check_name: str,
    dataset: str,
    passed: bool,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Emit a structured JSON data-quality event.

    These events are intentionally machine-parseable so a downstream
    quality dashboard can ingest them directly.

    Args:
        logger:     Logger instance to write to.
        check_name: Short identifier for the quality check.
        dataset:    Name of the dataset being validated.
        passed:     Whether the quality check passed.
        details:    Optional dict with numeric metrics (null_pct, dup_count, etc.).
    """
    event: Dict[str, Any] = {
        "event_type": "DATA_QUALITY",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "check": check_name,
        "dataset": dataset,
        "passed": passed,
        "details": details or {},
    }
    level = logging.INFO if passed else logging.WARNING
    logger.log(level, "DQ_EVENT | %s", json.dumps(event))


def log_optimization_note(logger: logging.Logger, optimization: str, reason: str) -> None:
    """
    Emit a structured Spark optimization annotation.

    Optimization decisions are logged so reviewers can understand WHY
    each optimization was (or was not) applied — a key interview talking point.
    """
    logger.info(
        "SPARK_OPT | %-30s | %s", optimization, reason
    )
