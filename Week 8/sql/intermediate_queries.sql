-- intermediate_queries.sql
-- =============================================================================
-- Query 4: Customers who placed orders but never had any item delivered
-- Purpose:   Surface at-risk customers whose orders never complete successfully.
-- Approach:  Aggregate per customer, use a HAVING clause to require zero
--            DELIVERED orders while having placed at least one order.
-- Complexity: Intermediate (conditional aggregation)
-- Expected Output: customer_id, customer_name, total_orders_placed.
-- Business Insight: Flags fulfillment/logistics failures tied to specific
--                    customers or regions — useful for customer-support triage.
-- =============================================================================
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS total_orders_placed
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
HAVING SUM(CASE WHEN o.status = 'DELIVERED' THEN 1 ELSE 0 END) = 0;


-- =============================================================================
-- Query 5: Products that were ordered but had more returns than purchases
-- Purpose:   Detect products with unusually high / defective-looking return volume.
-- Approach:  Sum positive quantity (purchases) vs. absolute negative quantity
--            (returns) per product, filter where returns > purchases.
-- Complexity: Intermediate (conditional aggregation + HAVING)
-- Expected Output: product_id, product_name, total_purchased, total_returned.
-- Business Insight: Feeds into quality-control / vendor-review processes —
--                    a product with returns > purchases likely has a defect
--                    or a misleading listing and should be pulled for review.
-- =============================================================================
SELECT
    p.product_id,
    p.product_name,
    SUM(CASE WHEN oi.quantity > 0 THEN oi.quantity ELSE 0 END) AS total_purchased,
    SUM(CASE WHEN oi.quantity < 0 THEN -oi.quantity ELSE 0 END) AS total_returned
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name
HAVING total_returned > total_purchased;


-- =============================================================================
-- Query 6: Return rate (returned items / total items) per category
-- Purpose:   Quantify category-level return rate as a quality/fit KPI.
-- Approach:  Sum absolute quantity for returns and total absolute quantity
--            per category; divide to get a percentage.
-- Complexity: Intermediate (conditional aggregation + derived ratio)
-- Expected Output: category, return_rate_percent.
-- Business Insight: Clothing categories with high return rates often signal
--                    sizing/fit issues; informs product-page improvements
--                    (size charts, better photos) the way most large retailers do.
-- =============================================================================
SELECT
    p.category,
    ROUND(
        100.0 * SUM(CASE WHEN oi.quantity < 0 THEN -oi.quantity ELSE 0 END)
        / NULLIF(SUM(ABS(oi.quantity)), 0),
        2
    ) AS return_rate_percent
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.category
ORDER BY return_rate_percent DESC;
