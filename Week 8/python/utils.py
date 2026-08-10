"""
utils.py
--------
Reusable, dependency-free helpers shared across the pipeline:
logging setup, an execution-timer decorator, a lightweight console
progress bar (no tqdm dependency), and a simple data-profiling routine.
"""

from __future__ import annotations

import logging
import sys
import time
from functools import wraps
from pathlib import Path
from typing import Callable, Iterable, TypeVar

import pandas as pd

T = TypeVar("T")


def get_logger(name: str) -> logging.Logger:
    """Return a configured module-level logger (idempotent — safe to call repeatedly)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def timer(logger: logging.Logger | None = None) -> Callable:
    """Decorator that logs (or prints) how long a function took to run."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            start = time.perf_counter()
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start
            message = f"{func.__name__} completed in {elapsed:.3f}s"
            if logger:
                logger.info(message)
            else:
                print(message)
            return result
        return wrapper
    return decorator


class ProgressBar:
    """
    A minimal, dependency-free console progress bar.

    Usage:
        bar = ProgressBar(total=1000, label="Generating orders")
        for i in range(1000):
            ...
            bar.update(1)
        bar.finish()
    """

    def __init__(self, total: int, label: str = "", width: int = 30):
        self.total = max(total, 1)
        self.label = label
        self.width = width
        self.count = 0
        self._last_pct = -1

    def update(self, n: int = 1) -> None:
        self.count += n
        pct = int(100 * self.count / self.total)
        if pct != self._last_pct:
            self._last_pct = pct
            filled = int(self.width * self.count / self.total)
            bar = "#" * filled + "-" * (self.width - filled)
            print(f"\r{self.label:<28} [{bar}] {pct:3d}%", end="", flush=True)

    def finish(self) -> None:
        self.count = self.total
        filled = self.width
        bar = "#" * filled
        print(f"\r{self.label:<28} [{bar}] 100%")


def profile_dataframe(df: pd.DataFrame, name: str) -> dict:
    """Return a lightweight profile (shape, dtypes, null counts) for a DataFrame."""
    return {
        "dataset": name,
        "rows": len(df),
        "columns": len(df.columns),
        "null_counts": df.isna().sum().to_dict(),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
    }


def write_data_profile(profiles: Iterable[dict], path: Path) -> None:
    """Write a human-readable data-profiling report."""
    lines = ["DATA PROFILE REPORT", "=" * 60, ""]
    for p in profiles:
        lines.append(f"Dataset: {p['dataset']}")
        lines.append(f"  Rows: {p['rows']}, Columns: {p['columns']}")
        lines.append("  Null counts per column:")
        for col, n in p["null_counts"].items():
            if n:
                lines.append(f"    {col}: {n}")
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
