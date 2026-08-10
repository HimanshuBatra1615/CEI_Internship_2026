"""
visualizer.py
-------------
Generates 8 high-resolution analytical visualizations and saves them to
docs/charts/ as required by the Week 8 OMIS assignment specification.

Charts produced:
    01_revenue_by_category.png        — Bar chart: total revenue per category
    02_monthly_order_volume.png       — Line chart: orders per month (trend)
    03_region_revenue_share.png       — Pie chart: revenue contribution by region
    04_top10_customers.png            — Horizontal bar: top 10 customers by LTV
    05_rfm_segment_distribution.png   — Bar chart: customer count per RFM segment
    06_cohort_retention_heatmap.png   — Heatmap: cohort retention Month 0–5
    07_return_reasons.png             — Bar chart: return count by reason
    08_weekly_revenue_trend.png       — Dual-axis line: weekly revenue + MoM change

Run directly:
    python visualizer.py
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")   # headless — no display needed
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "ecommerce.db"
RFM_CSV = BASE_DIR / "data" / "reports" / "rfm_segments.csv"
CHARTS_DIR = BASE_DIR / "docs" / "charts"

PALETTE = ["#4361EE", "#3A0CA3", "#7209B7", "#F72585", "#4CC9F0",
           "#06D6A0", "#FFD166", "#EF476F", "#118AB2", "#073B4C"]

STYLE = {
    "axes.facecolor":   "#F8F9FA",
    "figure.facecolor": "#FFFFFF",
    "axes.spines.top":  False,
    "axes.spines.right": False,
    "axes.grid":        True,
    "grid.color":       "#E9ECEF",
    "grid.linestyle":   "--",
    "grid.linewidth":   0.6,
    "font.family":      "DejaVu Sans",
    "axes.labelsize":   11,
    "axes.titlesize":   13,
    "axes.titleweight": "bold",
    "xtick.labelsize":  9,
    "ytick.labelsize":  9,
}


def _save(fig: plt.Figure, name: str) -> None:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    path = CHARTS_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


# --------------------------------------------------------------------------- #
# Chart 1: Revenue by category (bar)
# --------------------------------------------------------------------------- #

def chart_revenue_by_category(conn: sqlite3.Connection) -> None:
    df = pd.read_sql_query("""
        SELECT p.category,
               ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100.0)), 2) AS revenue
        FROM order_items oi
        JOIN products p ON p.product_id = oi.product_id
        WHERE oi.quantity > 0
        GROUP BY p.category
        ORDER BY revenue DESC
    """, conn)

    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(8, 5))
        bars = ax.bar(df["category"], df["revenue"], color=PALETTE[:len(df)], width=0.55, zorder=3)
        ax.bar_label(bars, labels=[f"${v/1e6:.1f}M" for v in df["revenue"]], padding=4, fontsize=9)
        ax.set_title("Total Revenue by Product Category")
        ax.set_xlabel("Category")
        ax.set_ylabel("Revenue (USD)")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
        fig.tight_layout()
    _save(fig, "01_revenue_by_category.png")


# --------------------------------------------------------------------------- #
# Chart 2: Monthly order volume (line)
# --------------------------------------------------------------------------- #

def chart_monthly_order_volume(conn: sqlite3.Connection) -> None:
    df = pd.read_sql_query("""
        SELECT strftime('%Y-%m', order_date) AS month, COUNT(*) AS order_count
        FROM orders
        WHERE order_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """, conn)

    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(df["month"], df["order_count"], color=PALETTE[0], linewidth=2.5, marker="o",
                markersize=5, zorder=3)
        ax.fill_between(df["month"], df["order_count"], alpha=0.12, color=PALETTE[0])
        ax.set_title("Monthly Order Volume")
        ax.set_xlabel("Month")
        ax.set_ylabel("Number of Orders")
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()
    _save(fig, "02_monthly_order_volume.png")


# --------------------------------------------------------------------------- #
# Chart 3: Revenue by region (pie)
# --------------------------------------------------------------------------- #

def chart_region_revenue_share(conn: sqlite3.Connection) -> None:
    df = pd.read_sql_query("""
        SELECT o.region_code,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100.0)) AS revenue
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        WHERE oi.quantity > 0
        GROUP BY o.region_code
        ORDER BY revenue DESC
    """, conn)

    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(7, 7))
        wedges, texts, autotexts = ax.pie(
            df["revenue"], labels=df["region_code"],
            autopct="%1.1f%%", colors=PALETTE[:len(df)],
            pctdistance=0.8, startangle=140,
            wedgeprops={"edgecolor": "white", "linewidth": 1.5},
        )
        for at in autotexts:
            at.set_fontsize(9)
        ax.set_title("Revenue Share by Region")
        fig.tight_layout()
    _save(fig, "03_region_revenue_share.png")


# --------------------------------------------------------------------------- #
# Chart 4: Top 10 customers by lifetime value (horizontal bar)
# --------------------------------------------------------------------------- #

def chart_top10_customers(conn: sqlite3.Connection) -> None:
    df = pd.read_sql_query("""
        SELECT c.customer_name,
               ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100.0)), 2) AS ltv
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        WHERE oi.quantity > 0
        GROUP BY c.customer_id, c.customer_name
        ORDER BY ltv DESC
        LIMIT 10
    """, conn)
    df = df.sort_values("ltv")   # ascending for horizontal bar

    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(9, 6))
        bars = ax.barh(df["customer_name"], df["ltv"], color=PALETTE[2], height=0.6, zorder=3)
        ax.bar_label(bars, labels=[f"${v:,.0f}" for v in df["ltv"]], padding=4, fontsize=9)
        ax.set_title("Top 10 Customers by Lifetime Value")
        ax.set_xlabel("Lifetime Value (USD)")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
        fig.tight_layout()
    _save(fig, "04_top10_customers.png")


# --------------------------------------------------------------------------- #
# Chart 5: RFM segment distribution (bar)
# --------------------------------------------------------------------------- #

def chart_rfm_segments() -> None:
    if not RFM_CSV.exists():
        print("  rfm_segments.csv not found — skipping chart 5.")
        return
    df = pd.read_csv(RFM_CSV)
    seg_order = ["VIP", "High Value", "Regular", "Occasional", "At Risk"]
    counts = df["segment"].value_counts().reindex(seg_order, fill_value=0)
    colors = [PALETTE[3], PALETTE[0], PALETTE[6], PALETTE[4], PALETTE[7]]

    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(8, 5))
        bars = ax.bar(counts.index, counts.values, color=colors, width=0.55, zorder=3)
        ax.bar_label(bars, padding=4, fontsize=9)
        ax.set_title("Customer Count by RFM Segment")
        ax.set_xlabel("Segment")
        ax.set_ylabel("Number of Customers")
        fig.tight_layout()
    _save(fig, "05_rfm_segment_distribution.png")


# --------------------------------------------------------------------------- #
# Chart 6: Cohort retention heatmap (Month 0–5)
# --------------------------------------------------------------------------- #

def chart_cohort_retention(conn: sqlite3.Connection) -> None:
    df = pd.read_sql_query("""
        WITH cohorts AS (
            SELECT customer_id, strftime('%Y-%m', registration_date) AS cohort_month FROM customers
        ),
        offsets AS (
            SELECT co.customer_id, c.cohort_month,
                   CAST((strftime('%Y',o.order_date)-strftime('%Y',c.cohort_month||'-01'))*12
                        +(strftime('%m',o.order_date)-strftime('%m',c.cohort_month||'-01')) AS INTEGER) AS mo
            FROM orders o
            JOIN cohorts c  ON c.customer_id = o.customer_id
            JOIN cohorts co ON co.customer_id = o.customer_id
        ),
        activity AS (
            SELECT cohort_month, mo, COUNT(DISTINCT customer_id) AS n
            FROM offsets WHERE mo BETWEEN 0 AND 5
            GROUP BY cohort_month, mo
        )
        SELECT cohort_month,
               MAX(CASE WHEN mo=0 THEN n ELSE 0 END) m0,
               MAX(CASE WHEN mo=1 THEN n ELSE 0 END) m1,
               MAX(CASE WHEN mo=2 THEN n ELSE 0 END) m2,
               MAX(CASE WHEN mo=3 THEN n ELSE 0 END) m3,
               MAX(CASE WHEN mo=4 THEN n ELSE 0 END) m4,
               MAX(CASE WHEN mo=5 THEN n ELSE 0 END) m5
        FROM activity GROUP BY cohort_month ORDER BY cohort_month
    """, conn)

    if df.empty:
        print("  No cohort data — skipping chart 6.")
        return

    month_cols = ["m0", "m1", "m2", "m3", "m4", "m5"]
    matrix = df[month_cols].div(df["m0"].replace(0, np.nan), axis=0) * 100
    matrix.index = df["cohort_month"]

    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(11, max(4, len(matrix) * 0.45 + 1)))
        im = ax.imshow(matrix.values, cmap="YlOrRd_r", aspect="auto", vmin=0, vmax=100)
        ax.set_xticks(range(6))
        ax.set_xticklabels([f"Month {i}" for i in range(6)])
        ax.set_yticks(range(len(matrix)))
        ax.set_yticklabels(matrix.index, fontsize=8)
        ax.set_title("Customer Cohort Retention Heatmap (Month 0–5)")
        for r in range(matrix.shape[0]):
            for c in range(matrix.shape[1]):
                val = matrix.values[r, c]
                if not np.isnan(val):
                    ax.text(c, r, f"{val:.0f}%", ha="center", va="center",
                            fontsize=7, color="black" if val > 40 else "white")
        plt.colorbar(im, ax=ax, label="Retention %")
        fig.tight_layout()
    _save(fig, "06_cohort_retention_heatmap.png")


# --------------------------------------------------------------------------- #
# Chart 7: Return reasons breakdown (bar)
# --------------------------------------------------------------------------- #

def chart_return_reasons(conn: sqlite3.Connection) -> None:
    try:
        df = pd.read_sql_query("""
            SELECT reason, COUNT(*) AS return_count, ROUND(AVG(refund_amount), 2) AS avg_refund
            FROM returns
            GROUP BY reason
            ORDER BY return_count DESC
        """, conn)
    except Exception:
        print("  returns table empty or missing — skipping chart 7.")
        return

    if df.empty:
        print("  No returns data — skipping chart 7.")
        return

    with plt.rc_context(STYLE):
        fig, ax1 = plt.subplots(figsize=(9, 5))
        x = np.arange(len(df))
        ax1.bar(x, df["return_count"], color=PALETTE[7], width=0.45, zorder=3, label="Return Count")
        ax1.set_xticks(x)
        ax1.set_xticklabels(df["reason"], rotation=20, ha="right")
        ax1.set_ylabel("Return Count", color=PALETTE[7])
        ax1.tick_params(axis="y", labelcolor=PALETTE[7])

        ax2 = ax1.twinx()
        ax2.plot(x, df["avg_refund"], color=PALETTE[0], marker="D", linewidth=2,
                 markersize=7, label="Avg Refund ($)", zorder=4)
        ax2.set_ylabel("Avg Refund Amount (USD)", color=PALETTE[0])
        ax2.tick_params(axis="y", labelcolor=PALETTE[0])

        ax1.set_title("Returns: Count and Avg Refund by Reason")
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
        fig.tight_layout()
    _save(fig, "07_return_reasons.png")


# --------------------------------------------------------------------------- #
# Chart 8: Weekly revenue trend (line with momentum annotation)
# --------------------------------------------------------------------------- #

def chart_weekly_revenue_trend(conn: sqlite3.Connection) -> None:
    df = pd.read_sql_query("""
        SELECT strftime('%Y-W%W', order_date) AS week,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100.0)) AS revenue
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        WHERE oi.quantity > 0
        GROUP BY week
        ORDER BY week
    """, conn)

    if df.empty or len(df) < 2:
        print("  Not enough weekly data — skipping chart 8.")
        return

    df["prev_revenue"] = df["revenue"].shift(1)
    df["wow_pct"] = 100.0 * (df["revenue"] - df["prev_revenue"]) / df["prev_revenue"].replace(0, np.nan)

    # Sample every 2nd week label to avoid clutter
    xticks = range(0, len(df), max(1, len(df) // 20))

    with plt.rc_context(STYLE):
        fig, ax1 = plt.subplots(figsize=(14, 5))
        ax1.plot(df.index, df["revenue"], color=PALETTE[0], linewidth=2.5, zorder=3)
        ax1.fill_between(df.index, df["revenue"], alpha=0.10, color=PALETTE[0])
        ax1.set_ylabel("Weekly Revenue (USD)", color=PALETTE[0])
        ax1.tick_params(axis="y", labelcolor=PALETTE[0])
        ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
        ax1.set_xticks(list(xticks))
        ax1.set_xticklabels([df["week"].iloc[i] for i in xticks], rotation=45, ha="right", fontsize=7)

        ax2 = ax1.twinx()
        ax2.bar(df.index, df["wow_pct"].fillna(0), color=PALETTE[3], alpha=0.35,
                width=0.8, zorder=2, label="WoW Change %")
        ax2.axhline(0, color="#888888", linewidth=0.8, linestyle="--")
        ax2.set_ylabel("Week-over-Week Change (%)", color=PALETTE[3])
        ax2.tick_params(axis="y", labelcolor=PALETTE[3])

        ax1.set_title("Weekly Revenue Trend with Week-over-Week Change")
        fig.tight_layout()
    _save(fig, "08_weekly_revenue_trend.png")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> None:
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run the full pipeline first.")
        sys.exit(1)

    print(f"Generating 8 analytical visualizations -> {CHARTS_DIR}")
    conn = sqlite3.connect(DB_PATH)
    try:
        chart_revenue_by_category(conn)
        chart_monthly_order_volume(conn)
        chart_region_revenue_share(conn)
        chart_top10_customers(conn)
        chart_rfm_segments()
        chart_cohort_retention(conn)
        chart_return_reasons(conn)
        chart_weekly_revenue_trend(conn)
    finally:
        conn.close()
    print("Done. All 8 charts saved.")


if __name__ == "__main__":
    main()
