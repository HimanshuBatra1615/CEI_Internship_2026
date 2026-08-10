-- basic_queries.sql
-- =============================================================================
-- Query 1: Total revenue per category
-- Purpose:   Measure top-line revenue contribution of each product category.
-- Approach:  Join order_items -> products, compute line revenue, GROUP BY category.
-- Complexity: Basic (single join + aggregation)
-- Expected Output: one row per category, sorted by revenue descending.
-- Business Insight: Tells merchandising which categories to invest inventory/ads in.
--                    Amazon-style category teams use this to set quarterly targets.
-- =============================================================================
SELECT
    p.category,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_revenue
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
WHERE oi.quantity > 0                      -- exclude returns from revenue
GROUP BY p.category
ORDER BY total_revenue DESC;


-- =============================================================================
-- Query 2: Top 10 customers by total order value
-- Purpose:   Identify the highest-value customers.
-- Approach:  Join order_items -> orders -> customers, aggregate revenue per customer.
-- Complexity: Basic (two joins + aggregation + LIMIT)
-- Expected Output: 10 rows: customer_id, customer_name, total_value.
-- Business Insight: Powers VIP/loyalty targeting and account-based marketing —
--                    e.g. Uber Eats might comp these customers to protect churn risk.
-- =============================================================================
SELECT
    c.customer_id,
    c.customer_name,
    ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_order_value
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
WHERE oi.quantity > 0
GROUP BY c.customer_id, c.customer_name
ORDER BY total_order_value DESC
LIMIT 10;


-- =============================================================================
-- Query 3: Month-wise order count for the last 12 months
-- Purpose:   Track order-volume trend for capacity planning and seasonality.
-- Approach:  Bucket order_date into YYYY-MM using SQLite strftime, filter to last 12
--            months relative to the most recent order in the data, GROUP BY month.
-- Complexity: Basic (date bucketing + aggregation)
-- Expected Output: month, order_count, ordered chronologically.
-- Business Insight: Ops/logistics teams use this to staff warehouses ahead of
--                    predictable demand spikes (e.g. holiday season).
-- =============================================================================
WITH bounds AS (
    SELECT MAX(order_date) AS max_date FROM orders
)
SELECT
    strftime('%Y-%m', o.order_date) AS month,
    COUNT(*) AS order_count
FROM orders o, bounds b
WHERE o.order_date >= date(b.max_date, '-12 months')
GROUP BY month
ORDER BY month;
