"""
validators.py
-------------
Standalone validation functions for the E-Commerce Order Analytics System.
These functions only INSPECT data and report problems — they never mutate
or drop rows (that responsibility belongs to clean_data.py's clean_*
functions). Kept separate so validation logic can be reused, tested, or
run independently of the cleaning pipeline (e.g. as a pre-load CI gate).
"""

from __future__ import annotations

import re

import pandas as pd

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
VALID_STATUSES = {"PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"}
VALID_CATEGORIES = {"Electronics", "Clothing", "Home", "Books"}


def validate_emails(customers_df: pd.DataFrame) -> list:
    """Return customer_ids whose email is missing '@' or a valid domain."""
    mask = ~customers_df["email"].astype(str).apply(lambda e: bool(EMAIL_REGEX.match(e.strip())))
    return customers_df.loc[mask, "customer_id"].tolist()


def check_referential_integrity(orders_df: pd.DataFrame, order_items_df: pd.DataFrame,
                                 products_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Return order_items rows referencing a non-existent order_id (and/or product_id)."""
    valid_order_ids = set(orders_df["order_id"])
    mask = ~order_items_df["order_id"].isin(valid_order_ids)
    if products_df is not None:
        valid_product_ids = set(products_df["product_id"])
        mask = mask | ~order_items_df["product_id"].isin(valid_product_ids)
    return order_items_df[mask]


def find_duplicate_ids(df: pd.DataFrame, id_column: str) -> pd.DataFrame:
    """Return all rows involved in a duplicate id_column value (both copies)."""
    return df[df.duplicated(subset=[id_column], keep=False)]


def find_invalid_discounts(order_items_df: pd.DataFrame) -> pd.DataFrame:
    """Return order_items rows where discount_percent is outside [0, 100]."""
    values = pd.to_numeric(order_items_df["discount_percent"], errors="coerce")
    return order_items_df[~values.between(0, 100)]


def find_future_dates(orders_df: pd.DataFrame, now: pd.Timestamp | None = None) -> pd.DataFrame:
    """Return orders rows whose parsed order_date is later than `now` (defaults to current time)."""
    now = now or pd.Timestamp.now()
    parsed = pd.to_datetime(orders_df["order_date"], errors="coerce")
    return orders_df[parsed > now]


def find_blank_strings(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Return rows where any of the given text columns is blank/whitespace-only."""
    mask = pd.Series(False, index=df.index)
    for col in columns:
        mask = mask | df[col].astype(str).str.strip().eq("")
    return df[mask]


def find_mixed_casing(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Return rows where a text column isn't consistently Title Case (candidate for normalization)."""
    return df[df[column].astype(str) != df[column].astype(str).str.strip().str.title()]


def find_invalid_categories(products_df: pd.DataFrame) -> pd.DataFrame:
    """Return product rows with a category outside the allowed set."""
    return products_df[~products_df["category"].isin(VALID_CATEGORIES)]


def find_negative_prices(products_df: pd.DataFrame) -> pd.DataFrame:
    """Return product rows with cost_price <= 0."""
    values = pd.to_numeric(products_df["cost_price"], errors="coerce")
    return products_df[values <= 0]


def find_missing_foreign_keys(child_df: pd.DataFrame, child_fk_column: str,
                               parent_df: pd.DataFrame, parent_pk_column: str) -> pd.DataFrame:
    """Generic FK check: rows in child_df whose FK value doesn't exist in parent_df's PK."""
    valid_keys = set(parent_df[parent_pk_column])
    return child_df[~child_df[child_fk_column].isin(valid_keys)]


def find_invalid_status(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Return orders rows with a status outside the allowed set."""
    return orders_df[~orders_df["status"].isin(VALID_STATUSES)]
