"""
clean_data.py
-------------
Cleans and validates the raw e-commerce CSVs, producing:
    - data/cleaned/*.csv        (cleaned datasets, all 5 entities)
    - data/rejected/*.csv       (quarantine: rows that failed validation, non-destructive)
    - data/reports/quality_report.csv   (row-level / rule-level counts)
    - data/reports/quality_summary.txt  (human-readable before/after summary)
    - data/reports/quality_audit.md     (Markdown audit report)
    - data/reports/rfm_segments.csv     (RFM customer segmentation)

Functions implemented per the assignment spec:
    clean_orders()
    clean_products()
    validate_emails()
    check_referential_integrity()

Plus: duplicate IDs, invalid discounts, future dates, blank strings,
mixed casing, invalid categories, negative prices, missing foreign keys,
quarantine isolation, Markdown report generation, RFM segmentation.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from config import CLEAN_DIR, RAW_DIR, REJECTED_DIR, REPORT_DIR, VALID_CATEGORIES
from utils import get_logger, profile_dataframe, timer, write_data_profile
from validators import (
    VALID_STATUSES,
    check_referential_integrity,
    validate_emails,
)

logger = get_logger(__name__)
NOW = datetime.now()

# Collects one row per validation rule for quality_report.csv
ISSUE_LOG: list[dict] = []


def _log_issue(dataset: str, rule: str, count: int, description: str) -> None:
    ISSUE_LOG.append({
        "dataset": dataset,
        "rule": rule,
        "rows_affected": count,
        "description": description,
    })
    if count:
        logger.info("[%s] %s: %d row(s) — %s", dataset, rule, count, description)


def _quarantine(df: pd.DataFrame, name: str) -> None:
    """Write rejected rows to data/rejected/<name>.csv (non-destructive quarantine)."""
    if df.empty:
        return
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    path = REJECTED_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    logger.info("Quarantined %d rows -> %s", len(df), path)


# --------------------------------------------------------------------------- #
# clean_orders
# --------------------------------------------------------------------------- #

def _parse_order_date(value: str):
    """Try ISO format first, then fall back to DD-MM-YYYY. Return NaT on failure."""
    if pd.isna(value) or str(value).strip() == "":
        return pd.NaT
    value = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return pd.NaT


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix date formats (normalize to ISO), handle NULL/blank customer_ids,
    flag invalid statuses and future-dated orders. Duplicate order_id rows
    are removed (keeping the first occurrence). Unparseable-date rows are
    quarantined to data/rejected/orders_bad_date.csv.
    """
    df = df.copy()
    before = len(df)

    # Normalize blank-string customer_ids to real NaN
    df["customer_id"] = df["customer_id"].replace(r"^\s*$", pd.NA, regex=True)
    n_missing_customer = df["customer_id"].isna().sum()
    _log_issue("orders", "missing_customer_id", int(n_missing_customer),
               "customer_id was NULL or blank; retained but flagged (guest checkout)")

    # Parse + normalize dates
    df["order_date_parsed"] = df["order_date"].apply(_parse_order_date)
    bad_date_mask = df["order_date_parsed"].isna()
    n_bad_format = int(bad_date_mask.sum())
    _log_issue("orders", "unparseable_order_date", n_bad_format,
               "order_date did not match any known format — quarantined")
    _quarantine(df[bad_date_mask].drop(columns=["order_date_parsed"]),
                "orders_bad_date")

    n_future = int((df["order_date_parsed"] > NOW).sum())
    _log_issue("orders", "future_order_date", n_future,
               "order_date is later than current date (data-entry error); retained, flagged")

    df["order_date"] = df["order_date_parsed"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df.drop(columns=["order_date_parsed"])

    # Duplicate order_id
    dup_mask = df.duplicated(subset=["order_id"], keep="first")
    n_dupes = int(dup_mask.sum())
    _log_issue("orders", "duplicate_order_id", n_dupes, "duplicate order_id rows removed")
    _quarantine(df[dup_mask], "orders_duplicates")
    df = df[~dup_mask]

    # Invalid status values
    n_invalid_status = int((~df["status"].isin(VALID_STATUSES)).sum())
    _log_issue("orders", "invalid_status", n_invalid_status,
               "status value outside PLACED/SHIPPED/DELIVERED/CANCELLED/RETURNED")

    # Drop rows where the date could not be parsed (unusable for time-series analysis)
    df = df[df["order_date"].notna()]

    after = len(df)
    _log_issue("orders", "rows_removed_total", before - after,
               "rows dropped (unparseable date or duplicate order_id)")
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# clean_products
# --------------------------------------------------------------------------- #

def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize product names (trim whitespace, title case), flag invalid
    categories and non-positive cost prices, and drop duplicate product_ids.
    """
    df = df.copy()
    before = len(df)

    messy_mask = df["product_name"].astype(str).apply(lambda x: x != x.strip() or x != x.title())
    _log_issue("products", "messy_product_name", int(messy_mask.sum()),
               "extra whitespace or inconsistent casing; normalized to trimmed Title Case")
    df["product_name"] = df["product_name"].astype(str).str.strip().str.title()

    n_invalid_category = int((~df["category"].isin(VALID_CATEGORIES)).sum())
    _log_issue("products", "invalid_category", n_invalid_category,
               "category outside Electronics/Clothing/Home/Books")

    n_negative_price = int((pd.to_numeric(df["cost_price"], errors="coerce") <= 0).sum())
    _log_issue("products", "non_positive_cost_price", n_negative_price,
               "cost_price <= 0, which is not physically valid")

    dup_mask = df.duplicated(subset=["product_id"], keep="first")
    _log_issue("products", "duplicate_product_id", int(dup_mask.sum()),
               "duplicate product_id rows removed")
    _quarantine(df[dup_mask], "products_duplicates")
    df = df[~dup_mask]

    after = len(df)
    _log_issue("products", "rows_removed_total", before - after,
               "duplicate product_id rows dropped")
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# clean_customers
# --------------------------------------------------------------------------- #

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Trim names, drop duplicate customer_ids, standardize customer_type casing."""
    df = df.copy()
    before = len(df)

    df["customer_name"] = df["customer_name"].astype(str).str.strip()
    df["customer_type"] = df["customer_type"].astype(str).str.strip().str.upper()

    dup_mask = df.duplicated(subset=["customer_id"], keep="first")
    _log_issue("customers", "duplicate_customer_id", int(dup_mask.sum()),
               "duplicate customer_id rows removed")
    _quarantine(df[dup_mask], "customers_duplicates")
    df = df[~dup_mask]

    after = len(df)
    _log_issue("customers", "rows_removed_total", before - after,
               "duplicate customer_id rows dropped")
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# clean_order_items
# --------------------------------------------------------------------------- #

def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """Flag zero-quantity rows (genuine no-ops) and clip invalid discounts to [0, 100]."""
    df = df.copy()

    n_zero_qty = int((pd.to_numeric(df["quantity"], errors="coerce") == 0).sum())
    _log_issue("order_items", "zero_quantity", n_zero_qty,
               "quantity is exactly 0; represents no actual transaction — flagged")

    n_invalid_discount = int(
        (~pd.to_numeric(df["discount_percent"], errors="coerce").between(0, 100)).sum()
    )
    _log_issue("order_items", "invalid_discount_percent", n_invalid_discount,
               "discount_percent outside the valid 0-100 range; clipped to [0, 100]")
    # Quarantine the offending rows before clipping
    bad_disc_mask = ~pd.to_numeric(df["discount_percent"], errors="coerce").between(0, 100)
    _quarantine(df[bad_disc_mask], "order_items_bad_discount")
    df["discount_percent"] = pd.to_numeric(df["discount_percent"], errors="coerce").clip(0, 100)

    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# clean_returns
# --------------------------------------------------------------------------- #

def clean_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Validate returns: positive quantity, valid return_date, non-negative refund."""
    df = df.copy()
    before = len(df)

    # Non-positive quantities
    neg_mask = pd.to_numeric(df["quantity"], errors="coerce") <= 0
    n_neg = int(neg_mask.sum())
    _log_issue("returns", "non_positive_quantity", n_neg,
               "return quantity <= 0; physically invalid — quarantined and removed")
    _quarantine(df[neg_mask], "returns_bad_quantity")
    df = df[~neg_mask]

    # Negative refund amounts
    neg_refund_mask = pd.to_numeric(df["refund_amount"], errors="coerce") < 0
    n_neg_refund = int(neg_refund_mask.sum())
    _log_issue("returns", "negative_refund", n_neg_refund,
               "refund_amount < 0; clipped to 0")
    df["refund_amount"] = pd.to_numeric(df["refund_amount"], errors="coerce").clip(lower=0)

    # Duplicate return_id
    dup_mask = df.duplicated(subset=["return_id"], keep="first")
    _log_issue("returns", "duplicate_return_id", int(dup_mask.sum()),
               "duplicate return_id rows removed")
    _quarantine(df[dup_mask], "returns_duplicates")
    df = df[~dup_mask]

    after = len(df)
    _log_issue("returns", "rows_removed_total", before - after,
               "returns rows removed (bad quantity or duplicate id)")
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# validate_emails / check_referential_integrity
# --------------------------------------------------------------------------- #

def _validate_emails_logged(customers_df: pd.DataFrame) -> list:
    invalid_ids = validate_emails(customers_df)
    _log_issue("customers", "invalid_email", len(invalid_ids),
               "email missing '@' or a valid domain")
    _quarantine(
        customers_df[customers_df["customer_id"].isin(invalid_ids)],
        "customers_invalid_email"
    )
    return invalid_ids


def _check_referential_integrity_logged(
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame,
    products_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    orphans = check_referential_integrity(orders_df, order_items_df, products_df)
    _log_issue("order_items", "orphan_order_or_product_id", len(orphans),
               "order_items row references a non-existent order_id or product_id")
    _quarantine(orphans, "order_items_orphans")
    return orphans


# --------------------------------------------------------------------------- #
# RFM Segmentation
# --------------------------------------------------------------------------- #

def compute_rfm_segments(orders_df: pd.DataFrame, order_items_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute RFM (Recency, Frequency, Monetary) scores and assign customer segments.

    Segments (matching assignment spec):
        VIP          — R=1, F=1, M=1  (most recent, most frequent, highest spend)
        High Value   — High monetary, good frequency
        Regular      — Average across all dimensions
        Occasional   — Low frequency
        At Risk      — Low recency (hasn't purchased recently)
    """
    snapshot_date = pd.Timestamp(NOW)

    # Parse order dates
    orders_df = orders_df.copy()
    orders_df["order_date_dt"] = pd.to_datetime(orders_df["order_date"], errors="coerce")
    orders_df = orders_df.dropna(subset=["customer_id", "order_date_dt"])
    orders_df["customer_id"] = pd.to_numeric(orders_df["customer_id"], errors="coerce")
    orders_df = orders_df.dropna(subset=["customer_id"])

    # Merge with order_items to get monetary value
    items = order_items_df[order_items_df["quantity"] > 0].copy()
    items["line_revenue"] = (
        items["quantity"] * items["unit_price"] * (1 - items["discount_percent"] / 100.0)
    )
    order_revenue = items.groupby("order_id")["line_revenue"].sum().reset_index()
    orders_merged = orders_df.merge(order_revenue, on="order_id", how="left").fillna({"line_revenue": 0})

    rfm = orders_merged.groupby("customer_id").agg(
        last_order_date=("order_date_dt", "max"),
        frequency=("order_id", "nunique"),
        monetary=("line_revenue", "sum"),
    ).reset_index()

    rfm["recency_days"] = (snapshot_date - rfm["last_order_date"]).dt.days

    # Score 1 (best) to 4 (worst) for R, F, M
    rfm["r_score"] = pd.qcut(rfm["recency_days"].rank(method="first"),  q=4, labels=[1, 2, 3, 4]).astype(int)
    rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"),     q=4, labels=[4, 3, 2, 1]).astype(int)
    rfm["m_score"] = pd.qcut(rfm["monetary"].rank(method="first"),      q=4, labels=[4, 3, 2, 1]).astype(int)
    rfm["rfm_score"] = rfm["r_score"].astype(str) + rfm["f_score"].astype(str) + rfm["m_score"].astype(str)

    def _segment(row: pd.Series) -> str:
        r, f, m = row["r_score"], row["f_score"], row["m_score"]
        if r == 1 and f == 1 and m == 1:
            return "VIP"
        if m == 1 and f <= 2:
            return "High Value"
        if r >= 4:
            return "At Risk"
        if f >= 3:
            return "Occasional"
        return "Regular"

    rfm["segment"] = rfm.apply(_segment, axis=1)

    return rfm[[
        "customer_id", "recency_days", "frequency", "monetary",
        "r_score", "f_score", "m_score", "rfm_score", "segment",
    ]].sort_values("monetary", ascending=False).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Markdown audit report
# --------------------------------------------------------------------------- #

def _write_markdown_audit(
    raw_counts: dict,
    clean_counts: dict,
    invalid_email_count: int,
    orphan_count: int,
    rfm_df: pd.DataFrame,
    path,
) -> None:
    """Generate a Markdown-formatted data-governance audit report."""
    seg_counts = rfm_df["segment"].value_counts().to_dict()
    lines = [
        "# Data Governance Audit Report",
        "",
        f"**Generated:** {NOW.strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Pipeline:** E-Commerce Order Analytics System",
        "",
        "---",
        "",
        "## 1. Row Count Summary",
        "",
        "| Entity | Raw Rows | Clean Rows | Δ Removed |",
        "|--------|----------|------------|-----------|",
    ]
    for k in raw_counts:
        raw = raw_counts[k]
        clean = clean_counts.get(k, 0)
        lines.append(f"| {k} | {raw:,} | {clean:,} | {raw - clean:,} |")

    lines += [
        "",
        "---",
        "",
        "## 2. Data Quality Issues Detected",
        "",
        "| Dataset | Rule | Rows Affected | Description |",
        "|---------|------|--------------|-------------|",
    ]
    for entry in ISSUE_LOG:
        if entry["rows_affected"] > 0:
            lines.append(
                f"| {entry['dataset']} | `{entry['rule']}` "
                f"| {entry['rows_affected']:,} | {entry['description']} |"
            )

    lines += [
        "",
        "---",
        "",
        "## 3. Quarantine Summary",
        "",
        f"- Invalid emails: **{invalid_email_count}** customer records quarantined to `data/rejected/customers_invalid_email.csv`",
        f"- Orphan order_items: **{orphan_count}** rows quarantined to `data/rejected/order_items_orphans.csv`",
        "- All other rejected rows saved to `data/rejected/` with dataset-specific filenames.",
        "",
        "---",
        "",
        "## 4. RFM Customer Segmentation",
        "",
        "| Segment | Customer Count |",
        "|---------|---------------|",
    ]
    for seg in ["VIP", "High Value", "Regular", "Occasional", "At Risk"]:
        lines.append(f"| {seg} | {seg_counts.get(seg, 0):,} |")

    lines += [
        "",
        "---",
        "",
        "## 5. Injected Anomaly Types",
        "",
        "| Anomaly | Target Rate | Purpose |",
        "|---------|-------------|---------|",
        "| NULL customer_id | 5% of orders | Guest checkout simulation |",
        "| Malformed date (DD-MM-YYYY) | 6% of orders | Format inconsistency |",
        "| Future-dated orders | 1% of orders | Data-entry error |",
        "| Invalid discount (>100%) | 1.5% of items | Out-of-bounds value |",
        "| Orphan order_items | 1% of items | Referential integrity violation |",
        "| Invalid email syntax | 2% of customers | Format error |",
        "| Messy product names | 8% of products | Casing/whitespace inconsistency |",
        "",
        "---",
        "",
        "*Report auto-generated by `clean_data.py`. Re-run to refresh.*",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Markdown audit report written -> %s", path)


# --------------------------------------------------------------------------- #
# Pipeline entry point
# --------------------------------------------------------------------------- #

@timer(logger)
def run_pipeline() -> None:
    logger.info("Loading raw data from %s", RAW_DIR)
    customers = pd.read_csv(RAW_DIR / "customers.csv", dtype=str)
    products = pd.read_csv(RAW_DIR / "products.csv")
    orders = pd.read_csv(RAW_DIR / "orders.csv", dtype={"order_id": int})
    order_items = pd.read_csv(RAW_DIR / "order_items.csv")
    returns_raw = pd.read_csv(RAW_DIR / "returns.csv") if (RAW_DIR / "returns.csv").exists() else pd.DataFrame()

    raw_counts = {
        "customers": len(customers),
        "products": len(products),
        "orders": len(orders),
        "order_items": len(order_items),
        "returns": len(returns_raw),
    }

    # --- Data profiling (pre-clean snapshot) ---
    profiles = [
        profile_dataframe(customers, "customers"),
        profile_dataframe(products, "products"),
        profile_dataframe(orders, "orders"),
        profile_dataframe(order_items, "order_items"),
    ]
    if not returns_raw.empty:
        profiles.append(profile_dataframe(returns_raw, "returns"))
    write_data_profile(profiles, REPORT_DIR / "data_profile.txt")

    # --- Validation that doesn't remove rows, just reports ---
    invalid_email_ids = _validate_emails_logged(customers)
    orphan_items = _check_referential_integrity_logged(orders, order_items, products)

    # --- Cleaning ---
    customers_clean = clean_customers(customers)
    products_clean = clean_products(products)
    orders_clean = clean_orders(orders)
    order_items_clean = clean_order_items(order_items)
    returns_clean = clean_returns(returns_raw) if not returns_raw.empty else pd.DataFrame()

    # Remove orphaned order_items (referential integrity) from the cleaned output
    valid_order_ids = set(orders_clean["order_id"])
    valid_product_ids = set(products_clean["product_id"])
    before_items = len(order_items_clean)
    order_items_clean = order_items_clean[
        order_items_clean["order_id"].isin(valid_order_ids)
        & order_items_clean["product_id"].isin(valid_product_ids)
    ]
    _log_issue("order_items", "rows_removed_orphans",
               before_items - len(order_items_clean),
               "order_items rows removed for referencing a non-existent order or product")

    # Filter returns to valid orders + products
    if not returns_clean.empty:
        before_returns = len(returns_clean)
        returns_clean["order_id"] = pd.to_numeric(returns_clean["order_id"], errors="coerce")
        returns_clean["product_id"] = pd.to_numeric(returns_clean["product_id"], errors="coerce")
        returns_clean = returns_clean[
            returns_clean["order_id"].isin(valid_order_ids)
            & returns_clean["product_id"].isin(valid_product_ids)
        ]
        _log_issue("returns", "rows_removed_orphans",
                   before_returns - len(returns_clean),
                   "returns rows removed for referencing a non-existent order or product")

    clean_counts = {
        "customers": len(customers_clean),
        "products": len(products_clean),
        "orders": len(orders_clean),
        "order_items": len(order_items_clean),
        "returns": len(returns_clean) if not returns_clean.empty else 0,
    }

    # --- RFM Segmentation ---
    rfm_df = compute_rfm_segments(orders_clean, order_items_clean)

    # --- Write cleaned CSVs ---
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    customers_clean.to_csv(CLEAN_DIR / "customers.csv", index=False)
    products_clean.to_csv(CLEAN_DIR / "products.csv", index=False)
    orders_clean.to_csv(CLEAN_DIR / "orders.csv", index=False)
    order_items_clean.to_csv(CLEAN_DIR / "order_items.csv", index=False)
    if not returns_clean.empty:
        returns_clean.to_csv(CLEAN_DIR / "returns.csv", index=False)
    logger.info("Cleaned CSVs written -> %s", CLEAN_DIR)

    # --- Reports ---
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(ISSUE_LOG).to_csv(REPORT_DIR / "quality_report.csv", index=False)
    rfm_df.to_csv(REPORT_DIR / "rfm_segments.csv", index=False)

    summary_lines = ["DATA QUALITY SUMMARY", "=" * 60, ""]
    summary_lines.append("Row counts (before -> after cleaning):")
    for k in raw_counts:
        summary_lines.append(f"  {k:<15} {raw_counts[k]:>6}  ->  {clean_counts[k]:>6}")
    summary_lines.append("")
    summary_lines.append(f"Invalid emails found: {len(invalid_email_ids)}")
    summary_lines.append(f"Orphaned order_items found (pre-clean): {len(orphan_items)}")
    summary_lines.append("")
    summary_lines.append("Per-rule issue counts:")
    for entry in ISSUE_LOG:
        summary_lines.append(
            f"  [{entry['dataset']}] {entry['rule']}: {entry['rows_affected']} - {entry['description']}"
        )

    (REPORT_DIR / "quality_summary.txt").write_text("\n".join(summary_lines), encoding="utf-8")

    # Markdown audit report
    _write_markdown_audit(
        raw_counts, clean_counts,
        len(invalid_email_ids), len(orphan_items),
        rfm_df,
        REPORT_DIR / "quality_audit.md",
    )

    logger.info("Quality reports written -> %s", REPORT_DIR)
    logger.info("RFM segments: %s", rfm_df["segment"].value_counts().to_dict())


if __name__ == "__main__":
    run_pipeline()
