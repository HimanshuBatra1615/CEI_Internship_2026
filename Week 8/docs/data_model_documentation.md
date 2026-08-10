# Data Model Documentation

## E-Commerce Order Analytics System — Database Schema Reference

---

## Overview

The database uses a **3NF Star/Snowflake hybrid** schema implemented in SQLite. It consists of **5 normalized entities** (tables) with enforced referential integrity (`FOREIGN KEY … ON DELETE CASCADE`), `CHECK` constraints, and 7 performance indexes.

---

## Entity-Relationship Summary

```
customers (1) ─────────── (N) orders (1) ─────────── (N) order_items (N) ─── (1) products
                                  │                                                   │
                                  └────────────────── (N) returns  (N) ───────────────┘
```

`orders.customer_id` is nullable (guest / anonymous orders are a deliberately injected data anomaly — not a schema bug).

---

## Table Definitions

### 1. `customers`

Stores registered buyer profiles.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `customer_id` | INTEGER | PRIMARY KEY | Auto-assigned surrogate key |
| `customer_name` | TEXT | NOT NULL | Full name (Title Case after cleaning) |
| `email` | TEXT | — | Contact email (nullable; validated separately) |
| `registration_date` | TEXT | NOT NULL | ISO date: `YYYY-MM-DD` |
| `customer_type` | TEXT | NOT NULL, CHECK IN ('REGULAR','PREMIUM','VIP') | Loyalty tier |

**Injected anomalies:** ~2% of emails have invalid syntax (missing `@` or domain).

---

### 2. `products`

Product catalogue across 4 categories and 20 subcategories.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `product_id` | INTEGER | PRIMARY KEY | Auto-assigned surrogate key |
| `product_name` | TEXT | NOT NULL | Cleaned to Title Case; raw may have extra whitespace or all-caps |
| `category` | TEXT | NOT NULL | One of: `Electronics`, `Clothing`, `Home`, `Books` |
| `subcategory` | TEXT | — | e.g. `Phones`, `Laptops`, `Fiction`, `Kitchen` |
| `cost_price` | REAL | NOT NULL, CHECK > 0 | Unit wholesale cost in USD |

**Injected anomalies:** ~8% of product names have extra whitespace or inconsistent casing.

---

### 3. `orders`

Each row is one customer order (or guest order when `customer_id` is NULL).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `order_id` | INTEGER | PRIMARY KEY | Auto-assigned surrogate key |
| `customer_id` | INTEGER | FK → customers ON DELETE CASCADE, nullable | NULL = guest/unknown buyer |
| `order_date` | TEXT | NOT NULL | ISO datetime: `YYYY-MM-DD HH:MM:SS` (normalized during cleaning) |
| `status` | TEXT | NOT NULL, CHECK IN ('PLACED','SHIPPED','DELIVERED','CANCELLED','RETURNED') | Fulfilment lifecycle stage |
| `region_code` | TEXT | NOT NULL | One of: `NA-EAST`, `NA-WEST`, `EU-CENTRAL`, `EU-WEST`, `APAC`, `LATAM` |

**Injected anomalies:** ~5% NULL `customer_id`; ~6% malformed dates (`DD-MM-YYYY`); ~1% future-dated.

---

### 4. `order_items`

Line items within each order. One order has 1–4 items.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `item_id` | INTEGER | PRIMARY KEY | Auto-assigned surrogate key |
| `order_id` | INTEGER | NOT NULL, FK → orders ON DELETE CASCADE | Parent order |
| `product_id` | INTEGER | NOT NULL, FK → products ON DELETE CASCADE | Product purchased |
| `quantity` | INTEGER | NOT NULL, CHECK ≠ 0 | Units purchased (always positive after v2; returns now in `returns` table) |
| `unit_price` | REAL | NOT NULL, CHECK ≥ 0 | Selling price at time of purchase |
| `discount_percent` | REAL | NOT NULL, CHECK BETWEEN 0 AND 100 | Percentage discount applied (clipped during cleaning) |

**Injected anomalies:** ~1.5% `discount_percent > 100`; ~1% orphan `order_id` references.

---

### 5. `returns` *(dedicated 5th entity)*

Explicit return events — each row is one product return from a delivered order.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `return_id` | INTEGER | PRIMARY KEY | Auto-assigned surrogate key |
| `order_id` | INTEGER | NOT NULL, FK → orders ON DELETE CASCADE | Source order |
| `product_id` | INTEGER | NOT NULL, FK → products ON DELETE CASCADE | Returned product |
| `quantity` | INTEGER | NOT NULL, CHECK > 0 | Units returned (always positive) |
| `return_date` | TEXT | NOT NULL | ISO date: `YYYY-MM-DD` |
| `reason` | TEXT | NOT NULL, CHECK IN ('DEFECTIVE','WRONG_ITEM','NOT_AS_DESCRIBED','CHANGED_MIND','DAMAGED_SHIPPING','OTHER') | Return reason code |
| `refund_amount` | REAL | NOT NULL, CHECK ≥ 0 | Refund issued in USD |

**Generated from:** ~12% of `DELIVERED` orders produce a return row; return dates are 1–30 days after order date.

---

## Indexes

| Index Name | Table | Column(s) | Purpose |
|------------|-------|-----------|---------|
| `idx_orders_customer` | orders | customer_id | Customer order history lookups |
| `idx_orders_date` | orders | order_date | Time-range filtering (CLI reports) |
| `idx_items_order` | order_items | order_id | JOIN performance |
| `idx_items_product` | order_items | product_id | Product revenue aggregation |
| `idx_products_category` | products | category | Category-level grouping |
| `idx_returns_order` | returns | order_id | Returns-per-order lookups |
| `idx_returns_date` | returns | return_date | Return trend queries |

---

## Data Volume (generated with seed=42)

| Entity | Raw Rows | Cleaned Rows (approx) |
|--------|----------|-----------------------|
| customers | 600 | ~600 |
| products | 220 | ~220 |
| orders | 1,500 | ~1,455 |
| order_items | ~2,800 | ~2,720 |
| returns | ~100 | ~95 |

---

## Referential Integrity Rules

- `orders.customer_id` → `customers.customer_id` ON DELETE CASCADE (guest orders retained with NULL FK)
- `order_items.order_id` → `orders.order_id` ON DELETE CASCADE
- `order_items.product_id` → `products.product_id` ON DELETE CASCADE
- `returns.order_id` → `orders.order_id` ON DELETE CASCADE
- `returns.product_id` → `products.product_id` ON DELETE CASCADE

Orphan rows (referencing non-existent parent IDs) are **quarantined** to `data/rejected/` during cleaning, not silently dropped.

---

## Normalization

The schema satisfies **Third Normal Form (3NF)**:

- **1NF:** All columns are atomic; no repeating groups.
- **2NF:** All non-key attributes are fully functionally dependent on the primary key.
- **3NF:** No transitive dependencies — `customer_type` belongs to `customers`, `category` belongs to `products`, etc.

The `order_items` table acts as a **bridge / fact table** connecting the `orders` and `products` dimensions, creating a Star/Snowflake hybrid appropriate for analytical queries.

---

*Document auto-maintained alongside `sql/schema.sql`. Update both when schema changes.*
