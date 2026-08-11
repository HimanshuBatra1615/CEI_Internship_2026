-- =============================================================================
-- RideSharing Analytics Platform — SQL Analytics Suite
-- =============================================================================
-- File        : sql/analytics_queries.sql
-- Description : Production-grade analytical SQL queries using Spark SQL.
--               These queries run against Silver/Gold Parquet tables registered
--               as Spark temporary views. Each query is designed to answer a
--               specific business question with optimized execution.
-- Usage       : spark.sql("<query>") or via notebook SQL cells
-- =============================================================================


-- =============================================================================
-- SECTION 1: DRIVER PERFORMANCE ANALYTICS
-- =============================================================================

-- Q1: Top 10 Drivers by Total Revenue with City Context
-- Business Use: Identify revenue champions for reward programs
SELECT
    d.driver_id,
    d.name,
    d.city,
    d.rating,
    COUNT(t.trip_id)                                        AS total_trips,
    SUM(CASE WHEN t.trip_status = 'Completed' THEN 1 ELSE 0 END)  AS completed_trips,
    ROUND(SUM(CASE WHEN t.trip_status = 'Completed' THEN t.fare_amount ELSE 0 END), 2) AS total_revenue,
    ROUND(AVG(CASE WHEN t.trip_status = 'Completed' THEN t.fare_amount END), 2)        AS avg_fare,
    ROUND(SUM(CASE WHEN t.trip_status = 'Completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(t.trip_id), 1) AS completion_pct,
    RANK() OVER (ORDER BY SUM(CASE WHEN t.trip_status = 'Completed' THEN t.fare_amount ELSE 0 END) DESC) AS revenue_rank
FROM silver_trips t
JOIN silver_drivers d ON t.driver_id = d.driver_id
GROUP BY d.driver_id, d.name, d.city, d.rating
ORDER BY total_revenue DESC
LIMIT 10;


-- Q2: Driver Performance Tiers using NTILE Window Function
-- Business Use: Segment drivers into performance quartiles for coaching
SELECT
    driver_id,
    name,
    city,
    rating,
    total_revenue,
    completion_rate,
    NTILE(4) OVER (ORDER BY total_revenue DESC) AS revenue_quartile,
    NTILE(4) OVER (ORDER BY completion_rate DESC) AS completion_quartile,
    CASE NTILE(4) OVER (ORDER BY total_revenue DESC)
        WHEN 1 THEN 'Top 25% — Champion'
        WHEN 2 THEN 'Top 50% — Performer'
        WHEN 3 THEN 'Bottom 50% — Developing'
        WHEN 4 THEN 'Bottom 25% — At-Risk'
    END AS performance_tier
FROM gold_driver_performance
ORDER BY revenue_quartile, total_revenue DESC;


-- Q3: Driver Revenue Trend using LAG / LEAD
-- Business Use: Identify drivers whose revenue is declining month-over-month
SELECT
    driver_id,
    name,
    trip_date,
    daily_revenue,
    LAG(daily_revenue, 1)  OVER (PARTITION BY driver_id ORDER BY trip_date) AS prev_day_rev,
    LEAD(daily_revenue, 1) OVER (PARTITION BY driver_id ORDER BY trip_date) AS next_day_rev,
    daily_revenue - LAG(daily_revenue, 1) OVER (PARTITION BY driver_id ORDER BY trip_date) AS revenue_delta,
    FIRST_VALUE(daily_revenue) OVER (PARTITION BY driver_id ORDER BY trip_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_day_revenue,
    LAST_VALUE(daily_revenue)  OVER (PARTITION BY driver_id ORDER BY trip_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_day_revenue
FROM (
    SELECT t.driver_id, d.name, l.trip_date,
           SUM(t.fare_amount) AS daily_revenue
    FROM silver_trips t
    JOIN silver_drivers d     ON t.driver_id = d.driver_id
    JOIN silver_trip_logs l   ON t.trip_id   = l.trip_id
    WHERE t.trip_status = 'Completed'
    GROUP BY t.driver_id, d.name, l.trip_date
) driver_daily
ORDER BY driver_id, trip_date;


-- Q4: Intra-City Driver Ranking using DENSE_RANK
-- Business Use: City-level leaderboard for localized incentive programs
SELECT
    city,
    driver_id,
    name,
    rating,
    total_revenue,
    completed_trips,
    DENSE_RANK()  OVER (PARTITION BY city ORDER BY total_revenue DESC)    AS city_revenue_rank,
    ROW_NUMBER()  OVER (PARTITION BY city ORDER BY completed_trips DESC)  AS city_trips_rank,
    RANK()        OVER (PARTITION BY city ORDER BY rating DESC)            AS city_rating_rank
FROM gold_driver_performance
ORDER BY city, city_revenue_rank;


-- =============================================================================
-- SECTION 2: REVENUE ANALYTICS
-- =============================================================================

-- Q5: Daily Revenue with 3-Day Rolling Average
-- Business Use: Smooth revenue volatility for executive reporting
SELECT
    trip_date,
    daily_revenue,
    ROUND(AVG(daily_revenue) OVER (
        ORDER BY trip_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_3day_avg,
    ROUND(SUM(daily_revenue) OVER (
        ORDER BY trip_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS cumulative_revenue,
    completed_trips,
    avg_fare
FROM gold_revenue_trend
ORDER BY trip_date;


-- Q6: Revenue Distribution by City
-- Business Use: City budget allocation and market expansion decisions
SELECT
    city,
    total_revenue,
    ROUND(total_revenue * 100.0 / SUM(total_revenue) OVER (), 2) AS revenue_share_pct,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    completed_trips,
    active_drivers,
    ROUND(total_revenue / NULLIF(active_drivers, 0), 2) AS revenue_per_driver
FROM gold_city_analytics
ORDER BY revenue_rank;


-- =============================================================================
-- SECTION 3: CANCELLATION ANALYTICS
-- =============================================================================

-- Q7: Cancellation Heatmap by City and Location
-- Business Use: Identify cancellation hotspots for operational intervention
SELECT
    city,
    pickup_location,
    total_trips,
    cancelled_trips,
    ROUND(cancelled_trips * 100.0 / total_trips, 1) AS cancellation_pct,
    RANK() OVER (ORDER BY cancelled_trips * 100.0 / total_trips DESC) AS cancellation_rank
FROM gold_cancellation_analytics
ORDER BY cancellation_pct DESC;


-- Q8: Same-Location Trip Analysis (Potential GPS/Data Quality Issue)
-- Business Use: Operational data quality signal — these trips are suspicious
SELECT
    t.pickup_location,
    COUNT(*)                                                    AS same_location_trips,
    SUM(CASE WHEN t.trip_status = 'Completed' THEN 1 ELSE 0 END) AS completed_same_loc,
    SUM(CASE WHEN t.trip_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_same_loc,
    ROUND(AVG(t.distance_km), 2)                                 AS avg_distance_km,
    ROUND(AVG(t.fare_amount), 2)                                 AS avg_fare
FROM silver_trips t
WHERE t.is_same_location = 1
GROUP BY t.pickup_location
ORDER BY same_location_trips DESC;


-- =============================================================================
-- SECTION 4: DELAY ANALYTICS
-- =============================================================================

-- Q9: Delay Distribution by Time of Day
-- Business Use: Identify peak-delay windows for driver dispatch optimization
SELECT
    l.time_of_day,
    l.trip_hour,
    COUNT(*)                         AS total_trips,
    ROUND(AVG(l.delay_minutes), 2)   AS avg_delay_min,
    MAX(l.delay_minutes)             AS max_delay_min,
    ROUND(
        AVG(l.delay_minutes) OVER (ORDER BY l.trip_hour ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
        2
    ) AS smoothed_delay
FROM silver_trip_logs l
WHERE l.cancellation_flag = 0
GROUP BY l.time_of_day, l.trip_hour
ORDER BY l.trip_hour;


-- Q10: Top 10 Highest-Delay Drivers
-- Business Use: Targeted driver coaching for punctuality improvement
SELECT
    t.driver_id,
    d.name,
    d.city,
    COUNT(t.trip_id)              AS completed_trips,
    ROUND(AVG(l.delay_minutes), 2) AS avg_delay_min,
    MAX(l.delay_minutes)           AS max_delay_min,
    RANK() OVER (ORDER BY AVG(l.delay_minutes) DESC) AS delay_rank
FROM silver_trips t
JOIN silver_drivers d   ON t.driver_id = d.driver_id
JOIN silver_trip_logs l ON t.trip_id   = l.trip_id
WHERE t.trip_status = 'Completed'
GROUP BY t.driver_id, d.name, d.city
HAVING COUNT(t.trip_id) >= 2
ORDER BY avg_delay_min DESC
LIMIT 10;


-- =============================================================================
-- SECTION 5: LOCATION & DEMAND ANALYTICS
-- =============================================================================

-- Q11: Location Demand Matrix (Pickup → Drop Pairs)
-- Business Use: Driver positioning strategy — where should idle drivers wait?
SELECT
    pickup_location,
    drop_location,
    COUNT(*)                                                       AS trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)            AS demand_share_pct,
    ROUND(AVG(CASE WHEN trip_status='Completed' THEN fare_amount END), 2) AS avg_fare,
    SUM(CASE WHEN trip_status='Completed' THEN 1 ELSE 0 END)       AS completed,
    SUM(CASE WHEN trip_status='Cancelled'  THEN 1 ELSE 0 END)       AS cancelled
FROM silver_trips
GROUP BY pickup_location, drop_location
ORDER BY trip_count DESC;


-- Q12: Peak Hour Revenue and Demand Analysis
-- Business Use: Dynamic pricing window identification (surge pricing logic)
SELECT
    trip_hour,
    time_of_day,
    total_trips,
    completed_trips,
    revenue,
    avg_fare,
    ROUND(completed_trips * 100.0 / NULLIF(total_trips, 0), 1) AS completion_pct,
    ROUND(revenue / NULLIF(completed_trips, 0), 2)              AS effective_rev_per_trip,
    RANK() OVER (ORDER BY revenue DESC)                         AS revenue_rank_by_hour
FROM gold_peak_hour_dataset
ORDER BY trip_hour;


-- =============================================================================
-- SECTION 6: EXECUTIVE DASHBOARD QUERIES
-- =============================================================================

-- Q13: Executive Summary KPI Table
SELECT kpi_name, kpi_value, kpi_unit, kpi_category
FROM gold_executive_kpis
ORDER BY kpi_category, kpi_name;


-- Q14: City Revenue Leaderboard
SELECT
    city,
    total_revenue,
    revenue_share_pct,
    revenue_rank,
    completion_rate,
    cancellation_rate,
    active_drivers,
    avg_driver_rating,
    avg_delay_minutes
FROM gold_city_analytics
ORDER BY revenue_rank;


-- Q15: Driver Segmentation Summary
SELECT
    segment,
    COUNT(*) AS driver_count,
    ROUND(AVG(driver_efficiency_score), 3) AS avg_efficiency_score,
    ROUND(AVG(total_revenue), 2)           AS avg_revenue,
    ROUND(AVG(completion_rate), 3)         AS avg_completion_rate,
    SUM(intervention_flag)                 AS intervention_needed
FROM gold_driver_segmentation
GROUP BY segment
ORDER BY avg_efficiency_score DESC;
