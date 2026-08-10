"""
cli.py
------
Menu-driven CLI BI application for the E-Commerce Order Analytics System.

Supports:
    1. Daily report
    2. Weekly report
    3. Monthly report
    4. Custom date range report    ← NEW (required by spec)
    5. Exit

Per the assignment spec, this module (and report_generator.py) use NO
external libraries — only the standard-library `sqlite3` module.

Run directly:
    python cli.py
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import report_generator as rg

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "ecommerce.db"

REPORT_SPANS_DAYS = {"daily": 1, "weekly": 7, "monthly": 30}


# --------------------------------------------------------------------------- #
# Input validation helpers
# --------------------------------------------------------------------------- #

def prompt_choice(prompt: str, valid_choices: set[str]) -> str:
    """Repeatedly prompt until the user enters one of valid_choices (case-insensitive)."""
    while True:
        choice = input(prompt).strip().lower()
        if choice in valid_choices:
            return choice
        print(f"  Invalid choice. Please enter one of: {', '.join(sorted(valid_choices))}")


def prompt_date(prompt: str) -> str:
    """Repeatedly prompt until the user enters a valid YYYY-MM-DD date."""
    while True:
        raw = input(prompt).strip()
        try:
            datetime.strptime(raw, "%Y-%m-%d")
            return raw
        except ValueError:
            print("  Invalid date. Please use YYYY-MM-DD format (e.g. 2024-03-15).")


def resolve_date_range(report_type: str, start_str: str) -> tuple[str, str, str, str]:
    """Given a report_type and start date, derive (start, end, prev_start, prev_end)."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    span = REPORT_SPANS_DAYS[report_type]
    end = start + timedelta(days=span)
    prev_start = start - timedelta(days=span)
    prev_end = start
    fmt = "%Y-%m-%d"
    return start.strftime(fmt), end.strftime(fmt), prev_start.strftime(fmt), prev_end.strftime(fmt)


def resolve_custom_range(start_str: str, end_str: str) -> tuple[str, str, str, str]:
    """For a custom date range, compute an equal-length previous window."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    if end <= start:
        raise ValueError("End date must be after start date.")
    span = (end - start).days
    prev_start = start - timedelta(days=span)
    prev_end = start
    fmt = "%Y-%m-%d"
    return start.strftime(fmt), end.strftime(fmt), prev_start.strftime(fmt), prev_end.strftime(fmt)


# --------------------------------------------------------------------------- #
# Menu loop
# --------------------------------------------------------------------------- #

MENU_TEXT = """
E-COMMERCE ORDER ANALYTICS — REPORT TOOL
=========================================
1. Daily Report
2. Weekly Report
3. Monthly Report
4. Custom Date Range Report
5. Exit
"""


def main() -> None:
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}.")
        print("Run generate_data.py, clean_data.py, then loader.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        while True:
            print(MENU_TEXT)
            choice = prompt_choice("Select an option (1-5): ", {"1", "2", "3", "4", "5"})

            if choice == "5":
                print("Goodbye!")
                break

            try:
                if choice == "4":
                    # Custom date range
                    start_str = prompt_date("Enter start date (YYYY-MM-DD): ")
                    end_str = prompt_date("Enter end date   (YYYY-MM-DD): ")
                    start, end, prev_start, prev_end = resolve_custom_range(start_str, end_str)
                    report = rg.build_report(conn, "custom", start, end, prev_start, prev_end)
                else:
                    report_type = {
                        "1": "daily", "2": "weekly", "3": "monthly"
                    }[choice]
                    start_str = prompt_date("Enter start date (YYYY-MM-DD): ")
                    start, end, prev_start, prev_end = resolve_date_range(report_type, start_str)
                    report = rg.build_report(conn, report_type, start, end, prev_start, prev_end)

                print()
                print(rg.render_report(report))
                print()

            except ValueError as exc:
                print(f"  Invalid input: {exc}")
            except Exception as exc:  # noqa: BLE001 — CLI must never crash on bad input
                print(f"  Could not generate report: {exc}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
