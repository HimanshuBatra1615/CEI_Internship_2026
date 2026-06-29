/*=========================================================
 CELEBAL EXCELLENCE INTERNSHIP (CEI) 2026
 Week 3 - Advanced SQL
 Dataset: Superstore
=========================================================*/

DROP TABLE IF EXISTS customers;

CREATE TABLE customers AS
SELECT DISTINCT
    [Customer ID],
    [Customer Name],
    Segment,
    Country,
    City,
    State,
    Region
FROM superstore_raw;

DROP TABLE IF EXISTS orders;

CREATE TABLE orders AS
SELECT DISTINCT
    [Order ID],
    [Order Date],
    [Ship Date],
    [Ship Mode],
    [Customer ID]
FROM superstore_raw;

DROP TABLE IF EXISTS products;

CREATE TABLE products AS
SELECT DISTINCT
    [Product ID],
    Category,
    [Sub-Category],
    [Product Name]
FROM superstore_raw;

/*---------------------------------------------------------
QUERY 1 : Orders where Sales > Average Sales
---------------------------------------------------------*/
SELECT *
FROM superstore_raw
WHERE Sales >
(
    SELECT AVG(Sales)
    FROM superstore_raw
);

/*---------------------------------------------------------
QUERY 2 : Highest Sales Order per Customer
---------------------------------------------------------*/
SELECT *
FROM superstore_raw s
WHERE Sales =
(
    SELECT MAX(Sales)
    FROM superstore_raw
    WHERE [Customer ID] = s.[Customer ID]
);

/*---------------------------------------------------------
QUERY 3 : Total Sales per Customer (CTE)
---------------------------------------------------------*/
WITH customer_sales AS
(
    SELECT
        [Customer ID],
        [Customer Name],
        SUM(Sales) AS Total_Sales
    FROM superstore_raw
    GROUP BY [Customer ID],[Customer Name]
)
SELECT *
FROM customer_sales;

/*---------------------------------------------------------
QUERY 4 : Customers Above Average Sales
---------------------------------------------------------*/
WITH customer_sales AS
(
    SELECT
        [Customer ID],
        [Customer Name],
        SUM(Sales) AS Total_Sales
    FROM superstore_raw
    GROUP BY [Customer ID],[Customer Name]
)
SELECT *
FROM customer_sales
WHERE Total_Sales >
(
    SELECT AVG(Total_Sales)
    FROM customer_sales
);

/*---------------------------------------------------------
QUERY 5 : Rank Customers
---------------------------------------------------------*/
SELECT
    [Customer Name],
    SUM(Sales) AS Total_Sales,
    RANK() OVER(ORDER BY SUM(Sales) DESC) AS Sales_Rank
FROM superstore_raw
GROUP BY [Customer Name];

/*---------------------------------------------------------
QUERY 6 : Row Number Within Each Customer
---------------------------------------------------------*/
SELECT
    [Customer Name],
    [Order ID],
    Sales,
    ROW_NUMBER() OVER(
        PARTITION BY [Customer Name]
        ORDER BY Sales DESC
    ) AS Order_Number
FROM superstore_raw;

/*---------------------------------------------------------
QUERY 7 : Top 3 Customers
---------------------------------------------------------*/
SELECT *
FROM
(
    SELECT
        [Customer Name],
        SUM(Sales) AS Total_Sales,
        RANK() OVER(ORDER BY SUM(Sales) DESC) AS Sales_Rank
    FROM superstore_raw
    GROUP BY [Customer Name]
)
WHERE Sales_Rank <= 3;

/*---------------------------------------------------------
FINAL QUERY : JOIN + CTE + WINDOW FUNCTION
---------------------------------------------------------*/
WITH customer_sales AS
(
    SELECT
        c.[Customer ID],
        c.[Customer Name],
        SUM(s.Sales) AS Total_Sales
    FROM customers c
    JOIN superstore_raw s
    ON c.[Customer ID] = s.[Customer ID]
    GROUP BY c.[Customer ID], c.[Customer Name]
)
SELECT
    [Customer Name],
    Total_Sales,
    RANK() OVER(ORDER BY Total_Sales DESC) AS Customer_Rank
FROM customer_sales
ORDER BY Customer_Rank;

/*=========================================================
MINI PROJECT
=========================================================*/

/* Top 5 Customers */
SELECT
    [Customer Name],
    SUM(Sales) AS Total_Sales
FROM superstore_raw
GROUP BY [Customer Name]
ORDER BY Total_Sales DESC
LIMIT 5;

/* Bottom 5 Customers */
SELECT
    [Customer Name],
    SUM(Sales) AS Total_Sales
FROM superstore_raw
GROUP BY [Customer Name]
ORDER BY Total_Sales ASC
LIMIT 5;

/* Customers with Only One Order */
SELECT
    [Customer Name],
    COUNT(DISTINCT [Order ID]) AS Total_Orders
FROM superstore_raw
GROUP BY [Customer Name]
HAVING Total_Orders = 1;

/* Customers Above Average Sales */
WITH customer_sales AS
(
    SELECT
        [Customer Name],
        SUM(Sales) AS Total_Sales
    FROM superstore_raw
    GROUP BY [Customer Name]
)
SELECT *
FROM customer_sales
WHERE Total_Sales >
(
    SELECT AVG(Total_Sales)
    FROM customer_sales
);

/* Highest Order Value Per Customer */
SELECT
    [Customer Name],
    MAX(Sales) AS Highest_Order_Value
FROM superstore_raw
GROUP BY [Customer Name];
