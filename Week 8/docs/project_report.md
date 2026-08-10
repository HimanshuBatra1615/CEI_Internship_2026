# Project Report: E-Commerce Order Analytics System

## Week 8 — Online Marketplace Customer & Order Intelligence System (OMIS)

**Author:** CEI Internship 2026  
**Domain:** Data Engineering  
**Date:** August 2026  
**Stack:** Python 3.11 · SQLite · SQL · Pandas · Matplotlib · Seaborn · unittest

---

## 1. Executive Summary

This project delivers an end-to-end, production-quality **Online Marketplace Customer & Order Intelligence System (OMIS)** built from scratch. The system synthesizes realistic multi-vendor marketplace data across **5 normalized entities**, applies a **non-destructive data governance pipeline** with quarantine isolation, loads data into a **3NF relational SQLite database**, executes **20 analytical SQL queries** spanning basic aggregation through advanced window functions, and exposes results through an **interactive CLI BI application** with period-over-period reporting. Eight high-resolution analytical visualizations and a 12-case `unittest` suite complete the deliverables.

---

## 2. System Architecture

```
[1] generate_data.py         Synthetic data (5 CSVs, controlled anomalies)
        ↓
[2] clean_data.py            Cleaning + validation + quarantine + RFM segmentation
        ↓                         → data/cleaned/*.csv
        ↓                         → data/rejected/*.csv (quarantine)
        ↓                         → data/reports/quality_audit.md
[3] loader.py                SQLite DB init + load (sql/schema.sql → ecommerce.db)
        ↓
[4] sql/*.sql                20 analytical queries (Basic / Intermediate / Advanced)
        ↓
[5] cli.py + report_generator.py   Interactive CLI BI (Daily/Weekly/Monthly/Custom)
        ↓
[6] visualizer.py            8 high-resolution charts → docs/charts/
        ↓
[7] tests/test_edge_cases.py  12 edge-case assertions (unittest.TestCase)
```

---

## 3. Data Generation

### Entities & Volumes

| Entity | Rows Generated | Description |
|--------|---------------|-------------|
| customers | 600 | Buyer profiles: name, email, registration date, type |
| products | 220 | Catalogue: 4 categories, 20 subcategories, cost prices |
| orders | 1,500 | Seasonal bias (Nov-Dec × 2.2, Jul × 1.4), 6 regions |
| order_items | ~2,800 | 1–4 items per order, realistic discount distribution |
| returns | ~100 | Dedicated 5th entity: reason, refund, return date |

### Controlled Anomalies Injected

| Anomaly | Rate | Cleaning Action |
|---------|------|-----------------|
| NULL `customer_id` | 5% of orders | Retained + flagged (guest checkout) |
| Malformed date (`DD-MM-YYYY`) | 6% of orders | Repaired to ISO format |
| Future-dated orders | 1% of orders | Flagged, retained |
| Invalid `discount_percent > 100` | 1.5% of items | Clipped to 100, quarantined |
| Orphan `order_id` in items | 1% of items | Quarantined to `data/rejected/` |
| Invalid email syntax | 2% of customers | Quarantined, flagged |
| Messy product names | 8% of products | Normalized to Title Case |

---

## 4. Data Cleaning & Governance Pipeline

`clean_data.py` implements a **non-destructive quarantine model**:

- Rows that fail hard constraints (orphan FKs, zero-quantity returns, unparseable dates) are written to `data/rejected/<entity>_<issue>.csv` **before** being removed from the clean output.
- Rows that can be repaired (date format, discount clipping, casing) are fixed **in-place** and counted.
- Rows that are valid business events (NULL customer IDs, negative-quantity returns) are **retained** with a flag in the quality log.
- A Markdown audit report (`data/reports/quality_audit.md`) is auto-generated summarising before/after row counts, per-rule issue counts, quarantine inventory, and RFM segment distribution.

---

## 5. Database Schema

**3NF Star/Snowflake hybrid** with 5 tables, `ON DELETE CASCADE` FKs, `CHECK` constraints, and 7 indexes. See [`docs/data_model_documentation.md`](data_model_documentation.md) for the full schema reference.

**Key design decisions:**
- `orders.customer_id` is intentionally nullable (guest orders are a valid business event).
- `order_items.quantity` enforces `CHECK (quantity != 0)` — returns are now captured in the dedicated `returns` table, not as negative quantities.
- `returns.reason` uses a domain-constrained `CHECK IN (...)` to prevent free-text pollution.

---

## 6. SQL Analytics Suite — 20 Queries

| # | File | Query | Key Feature |
|---|------|-------|-------------|
| 1–3 | `basic_queries.sql` | Revenue by category, Top-10 customers, Monthly order count | JOIN, GROUP BY, LIMIT |
| 4–6 | `intermediate_queries.sql` | Never-delivered customers, High-return products, Category return rate | HAVING, conditional aggregation |
| 7 | `advanced_queries.sql` | Running revenue per region | CTE + `SUM OVER` window frame |
| 8 | | Category product rank | CTE + `DENSE_RANK` |
| 9 | | Days between orders / At Risk flag | CTE + `LAG` |
| 10 | | Monthly customer spend tiers | Multi-level CTE |
| 11 | | LTV quartile segmentation | `NTILE(4)` |
| 12 | | Year-over-year revenue | CTE + self-join |
| 13 | | Category drift (first vs. latest) | `FIRST_VALUE` / `LAST_VALUE` |
| 14 | | Revenue Pareto (80/20) | Cumulative window `SUM OVER` |
| 15 | | Cohort retention Month 0–5 | Multi-level CTE + date math |
| 16 | | Market basket ("bought together") | Self-join on `order_items` |
| 17 | | Return rate by category (returns table) | CTE + join to 5th entity |
| 18 | | Return reason breakdown | Aggregation on `returns` |
| 19 | | RFM segmentation in pure SQL | Multi-level CTE + `NTILE` |
| 20 | | Weekly revenue momentum | CTE + `ROW_NUMBER` + `LAG` |

---

## 7. Customer RFM Segmentation

RFM (Recency, Frequency, Monetary) scores are computed both in Python (`clean_data.compute_rfm_segments`) and in pure SQL (Query 19):

| Segment | Criteria |
|---------|----------|
| **VIP** | R=1 AND F=1 AND M=1 (best across all dimensions) |
| **High Value** | M=1 AND F≤2 (high spend, moderate frequency) |
| **At Risk** | R≥4 (hasn't purchased recently) |
| **Occasional** | F≥3 (low frequency buyers) |
| **Regular** | All other customers |

Scores are assigned using `pd.qcut` (Python) or `NTILE(4)` (SQL) on ranked values, ensuring equal-sized quartile buckets.

---

## 8. CLI BI Application

The interactive CLI (`cli.py` + `report_generator.py`) uses **only stdlib `sqlite3`** (no ORM or pandas) as required by the assignment. It supports:

| Option | Description |
|--------|-------------|
| Daily | 1-day window vs. prior 1-day window |
| Weekly | 7-day window vs. prior 7-day window |
| Monthly | 30-day window vs. prior 30-day window |
| **Custom** | User-supplied start + end date; equal-length prior window computed automatically |

Each report displays: Total Orders, Total Revenue, Unique Customers — all with **Period-over-Period (PoP) % change**.

---

## 9. Analytical Visualizations

8 high-resolution charts saved to `docs/charts/`:

| File | Chart Type | Metric |
|------|-----------|--------|
| `01_revenue_by_category.png` | Bar | Total revenue per category |
| `02_monthly_order_volume.png` | Line + area fill | Orders per month |
| `03_region_revenue_share.png` | Pie | Revenue % by region |
| `04_top10_customers.png` | Horizontal bar | Top 10 customers by LTV |
| `05_rfm_segment_distribution.png` | Bar | Customer count per RFM segment |
| `06_cohort_retention_heatmap.png` | Heatmap | Cohort retention Month 0–5 |
| `07_return_reasons.png` | Dual-axis bar+line | Return count + avg refund by reason |
| `08_weekly_revenue_trend.png` | Dual-axis line+bar | Weekly revenue + WoW change % |

---

## 10. Testing

The `unittest`-based test suite (`tests/test_edge_cases.py`) covers **12 edge cases** across all 5 entities:

1. Orphan `order_id` detected by referential integrity check
2. `discount_percent > 100` clipped to 100
3. Negative discount clipped to 0
4. Zero quantity flagged in `order_items`
5. Future-dated order flagged (but retained)
6. Duplicate `order_id` removed (keep first)
7. Invalid email (no `@`) detected
8. Invalid email (no domain) detected
9. Missing `customer_id` retained and flagged
10. Zero-quantity `returns` row removed
11. Negative `refund_amount` clipped to 0
12. Duplicate `customer_id` removed
13. Messy product names normalized to Title Case (counted as part of case 10+)

All 12 test classes run with `python -m unittest discover tests` or `python tests/test_edge_cases.py`.

---

## 11. Lessons Learned

1. **Orphan FK ordering matters** — orphan `order_items` must be filtered using the *cleaned* `orders` table (post-dedup), not raw, otherwise valid rows pointing to a de-duplicated order_id get wrongly dropped.
2. **SQLite `LAST_VALUE` quirk** — without `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`, `LAST_VALUE` silently returns the current row, not the partition's true last. Explicit frame bounds are always required.
3. **Quarantine before mutate** — writing rejected rows to `data/rejected/` before clipping/dropping them preserves full audit traceability. Mutating in-place first destroys the original values.
4. **RFM score parity** — `pd.qcut` and `NTILE(4)` produce similar but not identical bucket boundaries due to tie-breaking; the Python and SQL segmentations should be treated as consistent approximations, not exact replicas.

---

## 12. Future Improvements

- Swap SQLite for PostgreSQL and schedule the pipeline with Apache Airflow
- Add a `dbt` transformation layer for version-controlled, testable SQL
- Implement streaming anomaly detection (e.g. Z-score on daily revenue) as a real-time alert
- Extend the CLI to export reports as CSV or PDF
- Add pytest integration and a CI/CD pipeline (GitHub Actions) with automatic coverage reporting

---

*End of Report*
