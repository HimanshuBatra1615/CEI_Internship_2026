-- advanced_queries.sql
-- Window functions, CTEs, subqueries, cohort analysis, self-joins.
-- Queries 7–20 (advanced tier)

-- =============================================================================
-- Query 7: Running total of revenue per region, ordered by date
-- Purpose:   Track cumulative revenue growth within each region over time.
-- Approach:  CTE aggregates daily revenue per region; a window SUM() with
--            PARTITION BY region ORDER BY date computes the running total.
-- Complexity: Advanced (CTE + window function with frame)
-- Expected Output: region_code, order_date, daily_revenue, running_total
-- Business Insight: Regional GMs use running totals to track pacing against
--                    monthly/quarterly targets in real time.
-- =============================================================================
WITH daily_revenue AS (
    SELECT
        o.region_code,
        date(o.order_date) AS order_date,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS daily_revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE oi.quantity > 0
    GROUP BY o.region_code, date(o.order_date)
)
SELECT
    region_code,
    order_date,
    ROUND(daily_revenue, 2) AS daily_revenue,
    ROUND(SUM(daily_revenue) OVER (
        PARTITION BY region_code
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS running_total
FROM daily_revenue
ORDER BY region_code, order_date;


-- =============================================================================
-- Query 8: Rank products within each category by total revenue (DENSE_RANK)
-- Purpose:   Identify each category's top performers without skipping ranks
--            on ties (unlike RANK).
-- Approach:  CTE computes total revenue per product; DENSE_RANK() windowed
--            by category, ordered by revenue descending.
-- Complexity: Advanced (CTE + DENSE_RANK window function)
-- Expected Output: category, product_name, total_revenue, rank_in_category
-- Business Insight: Category managers use this leaderboard to decide which
--                    SKUs get featured placement / ad spend.
-- =============================================================================
WITH product_revenue AS (
    SELECT
        p.category,
        p.product_name,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS total_revenue
    FROM order_items oi
    JOIN products p ON p.product_id = oi.product_id
    WHERE oi.quantity > 0
    GROUP BY p.category, p.product_name
)
SELECT
    category,
    product_name,
    ROUND(total_revenue, 2) AS total_revenue,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rank_in_category
FROM product_revenue
ORDER BY category, rank_in_category;


-- =============================================================================
-- Query 9: Days between consecutive orders per customer (LAG), flag "At Risk"
-- Purpose:   Measure purchase cadence per customer and flag churn risk.
-- Approach:  CTE gets one row per (customer, order_date); LAG() window fetches
--            the previous order date per customer; outer query computes the
--            gap in days and a second CTE averages gaps to flag "At Risk".
-- Complexity: Advanced (CTE + LAG window function + aggregation)
-- Expected Output: customer_id, order_date, previous_order_date, days_gap, risk_flag
-- Business Insight: CRM/retention teams trigger win-back emails once a
--                    customer's gap exceeds their historical average — this
--                    is the same logic Uber uses for "we miss you" pushes.
-- =============================================================================
WITH customer_orders AS (
    SELECT DISTINCT customer_id, date(order_date) AS order_date
    FROM orders
    WHERE customer_id IS NOT NULL
),
gaps AS (
    SELECT
        customer_id,
        order_date,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_date
    FROM customer_orders
),
gaps_with_diff AS (
    SELECT
        customer_id,
        order_date,
        previous_order_date,
        CASE WHEN previous_order_date IS NOT NULL
             THEN julianday(order_date) - julianday(previous_order_date)
        END AS days_gap
    FROM gaps
),
customer_avg AS (
    SELECT customer_id, AVG(days_gap) AS avg_gap
    FROM gaps_with_diff
    WHERE days_gap IS NOT NULL
    GROUP BY customer_id
)
SELECT
    g.customer_id,
    g.order_date,
    g.previous_order_date,
    g.days_gap,
    CASE WHEN a.avg_gap > 30 THEN 'At Risk' ELSE 'Healthy' END AS risk_flag
FROM gaps_with_diff g
JOIN customer_avg a ON a.customer_id = g.customer_id
ORDER BY g.customer_id, g.order_date;


-- =============================================================================
-- Query 10: Multi-level CTE — monthly revenue per customer -> High/Medium/Low
--           tier -> count of customers per tier per month
-- Purpose:   Segment the customer base by spend tier, tracked monthly.
-- Approach:  Level 1 CTE aggregates monthly revenue per customer. Level 2 CTE
--            classifies each (customer, month) into a tier. Final query counts
--            customers per tier per month.
-- Complexity: Advanced (nested/multi-level CTEs)
-- Expected Output: month, tier, customer_count
-- Business Insight: Finance/marketing use tier trends to see whether the
--                    high-value segment is growing or shrinking month over month.
-- =============================================================================
WITH monthly_customer_revenue AS (
    SELECT
        strftime('%Y-%m', o.order_date) AS month,
        c.customer_id,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS revenue
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE oi.quantity > 0
    GROUP BY month, c.customer_id
),
tiered AS (
    SELECT
        month,
        customer_id,
        revenue,
        CASE
            WHEN revenue > 10000 THEN 'High'
            WHEN revenue >= 5000 THEN 'Medium'
            ELSE 'Low'
        END AS tier
    FROM monthly_customer_revenue
)
SELECT
    month,
    tier,
    COUNT(*) AS customer_count
FROM tiered
GROUP BY month, tier
ORDER BY month, tier;


-- =============================================================================
-- Query 11: NTILE quartiles by customer lifetime value
-- Purpose:   Segment customers into 4 equal-sized value tiers for tiered
--            loyalty benefits.
-- Approach:  CTE computes lifetime value per customer; NTILE(4) window
--            function assigns a quartile; CASE maps quartile -> label.
-- Complexity: Advanced (CTE + NTILE window function)
-- Expected Output: customer_id, total_value, quartile, quartile_label
-- Business Insight: Loyalty programs (Platinum/Gold/Silver/Bronze) are
--                    literally built on this kind of quartile segmentation.
-- =============================================================================
WITH customer_ltv AS (
    SELECT
        c.customer_id,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS total_value
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE oi.quantity > 0
    GROUP BY c.customer_id
)
SELECT
    customer_id,
    ROUND(total_value, 2) AS total_value,
    NTILE(4) OVER (ORDER BY total_value DESC) AS quartile,
    CASE NTILE(4) OVER (ORDER BY total_value DESC)
        WHEN 1 THEN 'Platinum'
        WHEN 2 THEN 'Gold'
        WHEN 3 THEN 'Silver'
        ELSE 'Bronze'
    END AS quartile_label
FROM customer_ltv
ORDER BY quartile, total_value DESC;


-- =============================================================================
-- Query 12: Year-over-year revenue comparison per month
-- Purpose:   Compare each month's revenue with the same month one year prior.
-- Approach:  CTE aggregates revenue per (year, month); self-referencing join
--            via a second instance of the CTE offset by year - 1 (a form of
--            LAG that works across a sparse/irregular calendar).
-- Complexity: Advanced (CTE + self-join, NULL-safe growth calc)
-- Expected Output: year, month, revenue, prev_year_revenue, yoy_growth_percent
-- Business Insight: The standard board-deck metric for judging whether
--                    growth is real or just seasonal noise.
-- =============================================================================
WITH monthly_revenue AS (
    SELECT
        CAST(strftime('%Y', o.order_date) AS INTEGER) AS year,
        CAST(strftime('%m', o.order_date) AS INTEGER) AS month,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE oi.quantity > 0
    GROUP BY year, month
)
SELECT
    cur.year,
    cur.month,
    ROUND(cur.revenue, 2) AS revenue,
    ROUND(prev.revenue, 2) AS prev_year_revenue,
    CASE
        WHEN prev.revenue IS NULL OR prev.revenue = 0 THEN NULL
        ELSE ROUND(100.0 * (cur.revenue - prev.revenue) / prev.revenue, 2)
    END AS yoy_growth_percent
FROM monthly_revenue cur
LEFT JOIN monthly_revenue prev
    ON prev.year = cur.year - 1 AND prev.month = cur.month
ORDER BY cur.year, cur.month;


-- =============================================================================
-- Query 13: First / most-recent purchased category per customer (category shift)
-- Purpose:   See whether customers' taste/category preference drifts over time.
-- Approach:  CTE orders each customer's purchases by date; FIRST_VALUE and
--            LAST_VALUE window functions (with full-frame bounds) pull the
--            first and most recent category per customer.
-- Complexity: Advanced (CTE + FIRST_VALUE/LAST_VALUE window functions)
-- NOTE: SQLite requires explicit ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
--       FOLLOWING for LAST_VALUE to return the true partition last, not current row.
-- Expected Output: customer_id, first_category, latest_category, category_shift
-- Business Insight: Cross-category drift (e.g. Books -> Electronics) signals
--                    life-stage changes and is a cue for re-targeting ads.
-- =============================================================================
WITH customer_category_events AS (
    SELECT
        c.customer_id,
        o.order_date,
        p.category,
        FIRST_VALUE(p.category) OVER (
            PARTITION BY c.customer_id ORDER BY o.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_category,
        LAST_VALUE(p.category) OVER (
            PARTITION BY c.customer_id ORDER BY o.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS latest_category
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
    JOIN order_items oi ON oi.order_id = o.order_id
    JOIN products p ON p.product_id = oi.product_id
    WHERE oi.quantity > 0
)
SELECT DISTINCT
    customer_id,
    first_category,
    latest_category,
    CASE WHEN first_category <> latest_category THEN 'Yes' ELSE 'No' END AS category_shift
FROM customer_category_events
ORDER BY customer_id;


-- =============================================================================
-- Query 14: Cumulative distribution of revenue by customer (Pareto / top-N%)
-- Purpose:   Quantify what share of revenue comes from the top N% of customers
--            (classic 80/20 analysis).
-- Approach:  CTE aggregates revenue per customer; window SUM() computes a
--            running cumulative total ordered by revenue descending; divide
--            by the grand total (also a window function) for a percentage.
-- Complexity: Advanced (CTE + window SUM + CUME_DIST-style ratio)
-- Expected Output: customer_id, revenue, cumulative_revenue, cumulative_percent
-- Business Insight: If ~20% of customers drive ~80% of revenue, retention
--                    spend should concentrate there rather than spread evenly.
-- =============================================================================
WITH customer_revenue AS (
    SELECT
        c.customer_id,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS revenue
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE oi.quantity > 0
    GROUP BY c.customer_id
)
SELECT
    customer_id,
    ROUND(revenue, 2) AS revenue,
    ROUND(SUM(revenue) OVER (ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS cumulative_revenue,
    ROUND(
        100.0 * SUM(revenue) OVER (ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        / SUM(revenue) OVER (),
        2
    ) AS cumulative_percent
FROM customer_revenue
ORDER BY revenue DESC;


-- =============================================================================
-- Query 15: Cohort analysis — retention by registration month (Month 0–5+)
-- Purpose:   Track how well each signup cohort keeps ordering in the months
--            following registration (classic cohort retention curve).
-- Approach:  CTE 1 assigns each customer a cohort_month (registration month).
--            CTE 2 computes, for each order, the "month offset" from the
--            customer's cohort month. CTE 3 counts distinct customers active
--            at each offset per cohort. Final query pivots offsets 0-5 and
--            computes retention rate relative to offset 0 (cohort size).
-- Complexity: Advanced (multi-level CTE + date math + conditional aggregation)
-- Expected Output: cohort_month, month_0..month_5, retention_month_1..5_pct
-- Business Insight: The single most-used SaaS/retail growth metric — shows
--                    whether newer cohorts stick around better than older ones.
-- =============================================================================
WITH cohorts AS (
    SELECT customer_id, strftime('%Y-%m', registration_date) AS cohort_month
    FROM customers
),
order_offsets AS (
    SELECT
        co.customer_id,
        c.cohort_month,
        CAST(
            (strftime('%Y', o.order_date) - strftime('%Y', c.cohort_month || '-01')) * 12
            + (strftime('%m', o.order_date) - strftime('%m', c.cohort_month || '-01'))
        AS INTEGER) AS month_offset
    FROM orders o
    JOIN cohorts c ON c.customer_id = o.customer_id
    JOIN cohorts co ON co.customer_id = o.customer_id
),
cohort_activity AS (
    SELECT
        cohort_month,
        month_offset,
        COUNT(DISTINCT customer_id) AS active_customers
    FROM order_offsets
    WHERE month_offset BETWEEN 0 AND 5
    GROUP BY cohort_month, month_offset
)
SELECT
    cohort_month,
    MAX(CASE WHEN month_offset = 0 THEN active_customers ELSE 0 END) AS month_0,
    MAX(CASE WHEN month_offset = 1 THEN active_customers ELSE 0 END) AS month_1,
    MAX(CASE WHEN month_offset = 2 THEN active_customers ELSE 0 END) AS month_2,
    MAX(CASE WHEN month_offset = 3 THEN active_customers ELSE 0 END) AS month_3,
    MAX(CASE WHEN month_offset = 4 THEN active_customers ELSE 0 END) AS month_4,
    MAX(CASE WHEN month_offset = 5 THEN active_customers ELSE 0 END) AS month_5,
    ROUND(100.0 * MAX(CASE WHEN month_offset = 1 THEN active_customers ELSE 0 END)
        / NULLIF(MAX(CASE WHEN month_offset = 0 THEN active_customers ELSE 0 END), 0), 2) AS retention_month_1_pct,
    ROUND(100.0 * MAX(CASE WHEN month_offset = 2 THEN active_customers ELSE 0 END)
        / NULLIF(MAX(CASE WHEN month_offset = 0 THEN active_customers ELSE 0 END), 0), 2) AS retention_month_2_pct,
    ROUND(100.0 * MAX(CASE WHEN month_offset = 3 THEN active_customers ELSE 0 END)
        / NULLIF(MAX(CASE WHEN month_offset = 0 THEN active_customers ELSE 0 END), 0), 2) AS retention_month_3_pct,
    ROUND(100.0 * MAX(CASE WHEN month_offset = 4 THEN active_customers ELSE 0 END)
        / NULLIF(MAX(CASE WHEN month_offset = 0 THEN active_customers ELSE 0 END), 0), 2) AS retention_month_4_pct,
    ROUND(100.0 * MAX(CASE WHEN month_offset = 5 THEN active_customers ELSE 0 END)
        / NULLIF(MAX(CASE WHEN month_offset = 0 THEN active_customers ELSE 0 END), 0), 2) AS retention_month_5_pct
FROM cohort_activity
GROUP BY cohort_month
ORDER BY cohort_month;


-- =============================================================================
-- Query 16: Products frequently bought together (self-join market basket)
-- Purpose:   Classic "customers who bought X also bought Y" analysis.
-- Approach:  Self-join order_items to itself on matching order_id with
--            product_a.product_id < product_b.product_id (this single
--            inequality both excludes self-pairs and de-duplicates A-B/B-A).
-- Complexity: Advanced (self-join + aggregation)
-- Expected Output: product_a, product_b, times_bought_together (top pairs)
-- Business Insight: Powers "frequently bought together" widgets and
--                    cross-sell bundling — a direct revenue lever.
-- =============================================================================
SELECT
    pa.product_name AS product_a,
    pb.product_name AS product_b,
    COUNT(*) AS times_bought_together
FROM order_items oi1
JOIN order_items oi2
    ON oi1.order_id = oi2.order_id
    AND oi1.product_id < oi2.product_id      -- ensures each pair counted once, no self-pairs
JOIN products pa ON pa.product_id = oi1.product_id
JOIN products pb ON pb.product_id = oi2.product_id
WHERE oi1.quantity > 0 AND oi2.quantity > 0
GROUP BY pa.product_name, pb.product_name
ORDER BY times_bought_together DESC
LIMIT 25;


-- =============================================================================
-- Query 17: Return rate analysis by product category using the returns table
-- Purpose:   Measure return volume and refund cost per category to identify
--            quality issues and financial exposure.
-- Approach:  Join returns -> products, aggregate return count and total refund
--            per category. CTE computes purchase revenue for return-rate %.
-- Complexity: Advanced (CTE + join to dedicated returns entity)
-- Expected Output: category, return_count, total_refund, avg_refund, return_rate_pct
-- Business Insight: Electronics with high return rates often indicate quality
--                    failures; Clothing returns point to fit/sizing problems.
--                    Procurement and category teams use this to renegotiate
--                    vendor SLAs and improve product listings.
-- =============================================================================
WITH category_sales AS (
    SELECT
        p.category,
        COUNT(oi.item_id)                                                       AS total_items_sold,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0))   AS total_revenue
    FROM order_items oi
    JOIN products p ON p.product_id = oi.product_id
    WHERE oi.quantity > 0
    GROUP BY p.category
),
category_returns AS (
    SELECT
        p.category,
        COUNT(r.return_id)              AS return_count,
        SUM(r.refund_amount)            AS total_refund,
        AVG(r.refund_amount)            AS avg_refund
    FROM returns r
    JOIN products p ON p.product_id = r.product_id
    GROUP BY p.category
)
SELECT
    cs.category,
    cr.return_count,
    ROUND(cr.total_refund,  2) AS total_refund,
    ROUND(cr.avg_refund,    2) AS avg_refund,
    ROUND(100.0 * cr.return_count / NULLIF(cs.total_items_sold, 0), 2) AS return_rate_pct
FROM category_sales cs
LEFT JOIN category_returns cr USING (category)
ORDER BY return_rate_pct DESC;


-- =============================================================================
-- Query 18: Top return reasons and their average refund amounts
-- Purpose:   Understand *why* customers return products and the financial
--            cost associated with each reason type.
-- Approach:  Simple aggregation on the dedicated returns table, grouping by
--            the reason field.
-- Complexity: Advanced (aggregation on 5th entity)
-- Expected Output: reason, return_count, total_refund, avg_refund_per_return
-- Business Insight: "DEFECTIVE" or "WRONG_ITEM" reasons indicate operational
--                    failures; "CHANGED_MIND" indicates a listing accuracy problem.
--                    Customer service teams use this to prioritize process fixes.
-- =============================================================================
SELECT
    reason,
    COUNT(*)                        AS return_count,
    ROUND(SUM(refund_amount),  2)   AS total_refund,
    ROUND(AVG(refund_amount),  2)   AS avg_refund_per_return
FROM returns
GROUP BY reason
ORDER BY return_count DESC;


-- =============================================================================
-- Query 19: RFM-style customer segmentation via SQL
-- Purpose:   Compute Recency, Frequency, Monetary scores in pure SQL and
--            assign human-readable segments matching the Python RFM output.
-- Approach:  CTE 1 computes R/F/M raw values per customer.
--            CTE 2 assigns quartile scores (1=best) using NTILE(4).
--            Final query applies segment labels based on score combinations.
-- Complexity: Advanced (multi-level CTE + NTILE + CASE segmentation)
-- Expected Output: customer_id, recency_days, frequency, monetary,
--                  r_score, f_score, m_score, segment
-- Business Insight: RFM segmentation is the industry-standard first step for
--                    building personalized marketing campaigns, churn prediction
--                    models, and loyalty programme tiers at companies like
--                    Amazon, Flipkart, and Lazada.
-- =============================================================================
WITH customer_rfm_raw AS (
    SELECT
        c.customer_id,
        CAST(julianday('now') - julianday(MAX(o.order_date)) AS INTEGER) AS recency_days,
        COUNT(DISTINCT o.order_id)                                         AS frequency,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100.0)) AS monetary
    FROM customers c
    JOIN orders      o  ON o.customer_id  = c.customer_id
    JOIN order_items oi ON oi.order_id    = o.order_id
    WHERE oi.quantity > 0
    GROUP BY c.customer_id
),
scored AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        ROUND(monetary, 2) AS monetary,
        NTILE(4) OVER (ORDER BY recency_days  ASC)  AS r_score,   -- lower recency = better
        NTILE(4) OVER (ORDER BY frequency     DESC) AS f_score,
        NTILE(4) OVER (ORDER BY monetary      DESC) AS m_score
    FROM customer_rfm_raw
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    CASE
        WHEN r_score = 1 AND f_score = 1 AND m_score = 1            THEN 'VIP'
        WHEN m_score = 1 AND f_score <= 2                           THEN 'High Value'
        WHEN r_score >= 4                                           THEN 'At Risk'
        WHEN f_score >= 3                                           THEN 'Occasional'
        ELSE 'Regular'
    END AS segment
FROM scored
ORDER BY monetary DESC;


-- =============================================================================
-- Query 20: Weekly revenue momentum — week-over-week change with ROW_NUMBER
-- Purpose:   Surface the fastest-growing or most volatile weeks, enabling
--            the ops team to investigate spikes or investigate drop-offs
--            in near real-time.
-- Approach:  CTE 1 buckets revenue into ISO weeks. CTE 2 uses ROW_NUMBER()
--            to assign a sequential rank so each week can be compared to the
--            previous week using LAG() without relying on calendar arithmetic.
-- Complexity: Advanced (CTE + ROW_NUMBER + LAG + NULL-safe % change)
-- Expected Output: week, week_revenue, prev_week_revenue, wow_change_pct, momentum
-- Business Insight: Weekly momentum is the primary KPI dashboard metric at
--                    marketplace operations centres — a >15% drop triggers an
--                    immediate root-cause investigation.
-- =============================================================================
WITH weekly_revenue AS (
    SELECT
        strftime('%Y-W%W', o.order_date) AS week,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS week_revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE oi.quantity > 0
    GROUP BY week
),
ranked AS (
    SELECT
        week,
        week_revenue,
        ROW_NUMBER() OVER (ORDER BY week)                                  AS rn,
        LAG(week_revenue) OVER (ORDER BY week)                             AS prev_week_revenue
    FROM weekly_revenue
)
SELECT
    week,
    ROUND(week_revenue,      2) AS week_revenue,
    ROUND(prev_week_revenue, 2) AS prev_week_revenue,
    CASE
        WHEN prev_week_revenue IS NULL OR prev_week_revenue = 0 THEN NULL
        ELSE ROUND(100.0 * (week_revenue - prev_week_revenue) / prev_week_revenue, 2)
    END AS wow_change_pct,
    CASE
        WHEN prev_week_revenue IS NULL THEN 'Baseline'
        WHEN week_revenue > prev_week_revenue * 1.15  THEN 'Accelerating'
        WHEN week_revenue < prev_week_revenue * 0.85  THEN 'Decelerating'
        ELSE 'Stable'
    END AS momentum
FROM ranked
ORDER BY week;
