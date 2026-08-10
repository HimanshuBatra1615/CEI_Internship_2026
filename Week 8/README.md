# E-Commerce Order Analytics System
## Week 8 — Online Marketplace Customer & Order Intelligence System (OMIS)

An end-to-end Python + SQL data engineering project: synthesize realistic multi-vendor marketplace data across **5 normalized entities**, govern it through a **non-destructive quarantine pipeline**, load into a **3NF SQLite database**, analyze with **20 progressive SQL queries**, and expose results through an **interactive CLI BI application** with period-over-period reporting.

---

## 📐 Architecture

```
[1] generate_data.py    →  5 raw CSVs (600 customers, 220 products, 1500 orders, ~2800 items, ~97 returns)
[2] clean_data.py       →  data/cleaned/*.csv  |  data/rejected/*.csv (quarantine)
                           data/reports/quality_audit.md  |  rfm_segments.csv
[3] loader.py           →  data/ecommerce.db   (SQLite, 5 tables, schema.sql)
[4] sql/*.sql           →  20 analytical queries (basic → intermediate → advanced)
[5] cli.py              →  Interactive BI: Daily/Weekly/Monthly/Custom + PoP % change
[6] visualizer.py       →  docs/charts/ (8 high-resolution PNG charts)
[7] tests/test_edge_cases.py  →  15 tests across 12 edge cases (100% pass, unittest)
```

---

## 📂 Folder Structure

```
ecommerce_analytics/
├── data/
│   ├── raw/                # generated CSVs (5 entities)
│   ├── cleaned/            # pipeline output CSVs
│   ├── rejected/           # quarantine: rows failing validation (non-destructive)
│   ├── reports/
│   │   ├── quality_report.csv
│   │   ├── quality_summary.txt
│   │   ├── quality_audit.md   ← Markdown audit report
│   │   ├── data_profile.txt
│   │   ├── rfm_segments.csv   ← RFM customer segmentation
│   │   └── testing_report.txt
│   └── ecommerce.db        # SQLite database
├── sql/
│   ├── schema.sql          # 5 tables, ON DELETE CASCADE, CHECK constraints, 7 indexes
│   ├── basic_queries.sql   # Q1–Q3: aggregation, joins, LIMIT
│   ├── intermediate_queries.sql  # Q4–Q6: HAVING, conditional aggregation
│   └── advanced_queries.sql      # Q7–Q20: window functions, CTEs, RFM, cohort
├── python/
│   ├── config.py           # all paths/constants (nothing hardcoded elsewhere)
│   ├── utils.py            # logging, @timer, ProgressBar, data profiling
│   ├── generate_data.py    # 5-entity synthetic data generation
│   ├── validators.py       # pure inspection functions (no side effects)
│   ├── clean_data.py       # pipeline: clean + quarantine + RFM + Markdown audit
│   ├── loader.py           # init_db() + load_all() (FK-safe order)
│   ├── cli.py              # menu-driven BI CLI (Daily/Weekly/Monthly/Custom)
│   ├── report_generator.py # stdlib sqlite3 only — report queries & rendering
│   └── visualizer.py       # 8 analytical charts → docs/charts/
├── tests/
│   └── test_edge_cases.py  # 15 tests, 12 edge cases, unittest.TestCase
├── docs/
│   ├── architecture.png
│   ├── er_diagram.png
│   ├── project_report.md          ← Formal technical report (Markdown)
│   ├── data_model_documentation.md ← Full schema reference (Markdown)
│   └── charts/                    ← 8 analytical visualizations
│       ├── 01_revenue_by_category.png
│       ├── 02_monthly_order_volume.png
│       ├── 03_region_revenue_share.png
│       ├── 04_top10_customers.png
│       ├── 05_rfm_segment_distribution.png
│       ├── 06_cohort_retention_heatmap.png
│       ├── 07_return_reasons.png
│       └── 08_weekly_revenue_trend.png
├── screenshots/
├── project_report.pdf
├── README.md
└── requirements.txt
```

---

## ▶️ How to Run

```bash
pip install -r requirements.txt

cd python
python generate_data.py   # → data/raw/*.csv  (5 entities incl. returns)
python clean_data.py      # → data/cleaned/, data/rejected/, data/reports/
python loader.py          # → data/ecommerce.db
python cli.py             # interactive BI report menu

python visualizer.py      # → docs/charts/ (8 PNG charts)

cd ../tests
python test_edge_cases.py # → 15 tests, all pass
```

Run SQL query files directly:
```bash
sqlite3 data/ecommerce.db < sql/advanced_queries.sql
```

---

## 🗄️ Data Model (5 Entities)

```
customers ──(1:N)── orders ──(1:N)── order_items ──(N:1)── products
                       │                                        │
                       └────────(1:N)── returns ──(N:1)────────┘
```

**Schema:** 3NF Star/Snowflake hybrid. All FKs use `ON DELETE CASCADE`. `CHECK` constraints on `customer_type`, `status`, `cost_price`, `discount_percent`, `quantity`, `reason`, `refund_amount`. See [`docs/data_model_documentation.md`](docs/data_model_documentation.md) for the full column-level reference.

`orders.customer_id` is nullable — guest checkouts are a deliberately injected anomaly, not a schema bug.

---

## 🧹 Data Quality Issues Injected

| Anomaly | Rate | Cleaning Action |
|---------|------|-----------------|
| NULL `customer_id` | 5% of orders | Retained + flagged (guest checkout) |
| Malformed date (`DD-MM-YYYY`) | 6% of orders | Repaired to ISO |
| Future-dated orders | 1% of orders | Flagged, retained |
| `discount_percent > 100` | 1.5% of items | Clipped to 100, quarantined |
| Orphan `order_id` in items | 1% of items | Quarantined to `data/rejected/` |
| Invalid email syntax | 2% of customers | Quarantined, flagged |
| Messy product names | 8% of products | Normalized to Title Case |

Rejected rows are written to `data/rejected/` **before** any mutation — full audit traceability.  
A Markdown audit report (`data/reports/quality_audit.md`) is auto-generated each run.

---

## 📊 SQL Features — 20 Queries

| Tier | Queries | Features |
|------|---------|----------|
| Basic (Q1–Q3) | Revenue by category, Top-10 customers, Monthly order count | JOIN, GROUP BY, LIMIT |
| Intermediate (Q4–Q6) | Never-delivered customers, High-return products, Category return rate | HAVING, conditional aggregation |
| Advanced (Q7–Q20) | Running totals, DENSE_RANK, LAG, multi-level CTEs, NTILE, YoY, FIRST/LAST_VALUE, Pareto/cumulative dist, Cohort retention Month 0–5, Market basket, Returns analysis, Return reasons, RFM SQL, Weekly momentum | All major window functions |

Every query documented inline: **Purpose / Approach / Complexity / Expected Output / Business Insight**.

---

## 🐍 Python Features

- **Config-driven:** every constant in `config.py`, nothing hardcoded in pipeline scripts
- **Type hints + docstrings** throughout all modules
- **`@timer` decorator** on all pipeline entry points
- **Dependency-free progress bars** (`utils.ProgressBar`)
- **`validators.py`** kept side-effect-free and independently testable
- **`report_generator.py` / `cli.py`** restricted to stdlib `sqlite3` only
- **Non-destructive quarantine:** `data/rejected/` captures bad rows before mutation
- **RFM Segmentation:** Python (`pd.qcut`) + SQL (`NTILE`) dual implementation

---

## 🤖 RFM Customer Segmentation

| Segment | Criteria | Count (seed=42) |
|---------|----------|-----------------|
| VIP | R=1, F=1, M=1 — best across all dimensions | 48 |
| High Value | High monetary, moderate frequency | 68 |
| Regular | Average profile | 110 |
| Occasional | Low frequency | 138 |
| At Risk | Low recency — hasn't purchased recently | 117 |

---

## 📈 Visualizations (`docs/charts/`)

| Chart | Type | Metric |
|-------|------|--------|
| 01_revenue_by_category | Bar | Total revenue per category |
| 02_monthly_order_volume | Line + fill | Orders per month |
| 03_region_revenue_share | Pie | Revenue % by region |
| 04_top10_customers | Horizontal bar | Top 10 customers by LTV |
| 05_rfm_segment_distribution | Bar | Customer count per RFM segment |
| 06_cohort_retention_heatmap | Heatmap | Cohort retention Month 0–5 |
| 07_return_reasons | Dual-axis bar+line | Return count + avg refund by reason |
| 08_weekly_revenue_trend | Dual-axis line+bar | Weekly revenue + WoW change % |

---

## 🧪 Tests (`tests/test_edge_cases.py`)

15 tests across 12 edge cases using **`unittest.TestCase`**:

| # | Class | Edge Case |
|---|-------|-----------|
| 1 | TestOrphanOrderId | Orphan order_id detected |
| 2–3 | TestInvalidDiscount | discount > 100 clipped; negative clipped to 0 |
| 4 | TestZeroQuantity | Zero quantity flagged |
| 5 | TestFutureOrderDate | Future date flagged, retained |
| 6 | TestDuplicateOrderId | Duplicate order_id removed |
| 7–8 | TestInvalidEmail | No `@` detected; no domain detected |
| 9 | TestMissingCustomerId | NULL customer_id retained + flagged |
| 10–11 | TestReturnsNonPositiveQuantity | Zero-qty return removed; negative refund clipped |
| 12 | TestDuplicateCustomerId | Duplicate customer_id removed |
| 13 | TestMessyProductNames | Messy names normalized to Title Case |
| 14 | TestMalformedDateRepair | `DD-MM-YYYY` repaired to ISO |
| 15 | TestRFMSegmentation | RFM segments computed correctly |

**Result: 15/15 PASS**

---

## 🔮 Lessons Learned

1. **Orphan FK ordering** — filter orphans against *cleaned* orders (post-dedup), not raw.
2. **SQLite `LAST_VALUE` quirk** — requires explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame; without it, returns current row.
3. **Quarantine before mutate** — writing to `data/rejected/` before clipping preserves original values for audit.
4. **RFM duality** — Python (`pd.qcut`) and SQL (`NTILE`) bucket boundaries differ on tie-breaking; treat as consistent approximations.

---

## 🔮 Future Improvements

- Swap SQLite for PostgreSQL + Airflow scheduling
- `dbt` transformation layer for version-controlled SQL
- Streaming anomaly detection (Z-score on daily revenue)
- CLI export to CSV / PDF
- GitHub Actions CI with pytest + coverage gate
