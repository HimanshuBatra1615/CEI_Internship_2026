-- Row Count

SELECT COUNT(*) AS total_rows
FROM superstore;

--------------------------------------------------

-- Region Filter

SELECT *
FROM superstore
WHERE Region = 'West';

--------------------------------------------------

-- Category Filter

SELECT *
FROM superstore
WHERE Category = 'Technology';

--------------------------------------------------

-- Sales Filter

SELECT *
FROM superstore
WHERE Sales > 1000;

--------------------------------------------------

-- Sales by Region

SELECT Region,
       SUM(Sales) AS Total_Sales
FROM superstore
GROUP BY Region
ORDER BY Total_Sales DESC;

--------------------------------------------------

-- Sales by Category

SELECT Category,
       SUM(Sales) AS Total_Sales
FROM superstore
GROUP BY Category
ORDER BY Total_Sales DESC;

--------------------------------------------------

-- Average Sales & Quantity

SELECT Category,
       AVG(Sales) AS Average_Sales,
       SUM(Quantity) AS Total_Quantity
FROM superstore
GROUP BY Category;

--------------------------------------------------

-- Top Products

SELECT [Product Name],
       SUM(Sales) AS Total_Sales
FROM superstore
GROUP BY [Product Name]
ORDER BY Total_Sales DESC
LIMIT 10;

--------------------------------------------------

-- Top Customers

SELECT [Customer Name],
       SUM(Sales) AS Total_Sales
FROM superstore
GROUP BY [Customer Name]
ORDER BY Total_Sales DESC
LIMIT 10;

--------------------------------------------------

-- Monthly Trend

SELECT
substr([Order Date],7,4) || '-' ||
substr([Order Date],1,2) AS Month,
SUM(Sales) AS Revenue
FROM superstore
GROUP BY Month
ORDER BY Month;

--------------------------------------------------

-- Duplicate Check

SELECT [Order ID],
       COUNT(*) AS duplicate_count
FROM superstore
GROUP BY [Order ID]
HAVING COUNT(*) > 1;

--------------------------------------------------

-- Data Validation

SELECT COUNT(*) AS missing_customer_names
FROM superstore
WHERE [Customer Name] IS NULL;

SELECT COUNT(*) AS missing_product_names
FROM superstore
WHERE [Product Name] IS NULL;

SELECT COUNT(*) AS missing_sales
FROM superstore
WHERE Sales IS NULL;