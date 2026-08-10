"""
report_generator.py
--------------------
Report-computation logic for the CLI tool, kept separate from the menu/
input-handling code in cli.py so the queries can be reused, tested, or
called from another entry point. Uses only the standard-library `sqlite3`
module (no external dependencies), matching the assignment's constraint
for the Python + SQL integration part.
"""

from __future__ import annotations

import sqlite3

REVENUE_EXPR = "oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)"


def period_summary(conn: sqlite3.Connection, start: str, end: str) -> dict:
    """Compute total orders, revenue, and unique customers for a [start, end) date range."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            COUNT(DISTINCT o.order_id) AS total_orders,
            COALESCE(SUM({REVENUE_EXPR}), 0) AS total_revenue,
            COUNT(DISTINCT o.customer_id) AS unique_customers
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        WHERE o.order_date >= ? AND o.order_date < ? AND oi.quantity > 0
    """, (start, end))
    row = cur.fetchone()
    return {
        "total_orders": row[0] or 0,
        "total_revenue": round(row[1] or 0, 2),
        "unique_customers": row[2] or 0,
    }


def top_products(conn: sqlite3.Connection, start: str, end: str, limit: int = 3) -> list[tuple]:
    """Return the top `limit` products by revenue within [start, end)."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT p.product_name, SUM({REVENUE_EXPR}) AS revenue
        FROM order_items oi
        JOIN orders o ON o.order_id = oi.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.order_date >= ? AND o.order_date < ? AND oi.quantity > 0
        GROUP BY p.product_name
        ORDER BY revenue DESC
        LIMIT ?
    """, (start, end, limit))
    return cur.fetchall()


def pct_change(current_value: float, previous_value: float) -> str:
    """Human-readable percent change string, safe against divide-by-zero."""
    if previous_value == 0:
        return "n/a (no prior data)"
    change = 100.0 * (current_value - previous_value) / previous_value
    return f"{change:+.2f}%"


def build_report(conn: sqlite3.Connection, report_type: str, start: str, end: str,
                  prev_start: str, prev_end: str, top_n: int = 3) -> dict:
    """Assemble the full report payload: current period, previous period, top products."""
    current = period_summary(conn, start, end)
    previous = period_summary(conn, prev_start, prev_end)
    products = top_products(conn, start, end, limit=top_n)
    return {
        "report_type": report_type,
        "start": start,
        "end": end,
        "current": current,
        "previous": previous,
        "top_products": products,
    }


def render_report(report: dict) -> str:
    """Render a report payload (from build_report) as a formatted console string."""
    current, previous = report["current"], report["previous"]
    lines = []
    lines.append("=" * 60)
    lines.append(f"{report['report_type'].upper()} REPORT: {report['start']} to {report['end']}")
    lines.append("=" * 60)
    lines.append(f"{'Metric':<20}{'Current Period':<20}{'vs Previous Period':<20}")
    lines.append("-" * 60)
    lines.append(f"{'Total Orders':<20}{current['total_orders']:<20}"
                  f"{pct_change(current['total_orders'], previous['total_orders']):<20}")
    lines.append(f"{'Total Revenue':<20}{'$' + format(current['total_revenue'], ',.2f'):<20}"
                  f"{pct_change(current['total_revenue'], previous['total_revenue']):<20}")
    lines.append(f"{'Unique Customers':<20}{current['unique_customers']:<20}"
                  f"{pct_change(current['unique_customers'], previous['unique_customers']):<20}")
    lines.append("-" * 60)
    lines.append("Top Products by Revenue:")
    if report["top_products"]:
        for i, (name, revenue) in enumerate(report["top_products"], 1):
            lines.append(f"  {i}. {name:<35} ${revenue:,.2f}")
    else:
        lines.append("  No sales in this period.")
    lines.append("=" * 60)
    return "\n".join(lines)
