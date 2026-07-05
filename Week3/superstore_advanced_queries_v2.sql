/* CEI 2026 Week 3 - Version 2 */

DROP TABLE IF EXISTS customers;
CREATE TABLE customers AS
SELECT DISTINCT [Customer ID],[Customer Name],Segment,Country,City,State,Region
FROM superstore_raw;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders AS
SELECT DISTINCT [Order ID],[Order Date],[Ship Date],[Ship Mode],[Customer ID]
FROM superstore_raw;

DROP TABLE IF EXISTS products;
CREATE TABLE products AS
SELECT DISTINCT [Product ID],Category,[Sub-Category],[Product Name]
FROM superstore_raw;

/* Q1 */
SELECT * FROM superstore_raw
WHERE Sales>(SELECT AVG(Sales) FROM superstore_raw);

/* Q2 */
SELECT *
FROM superstore_raw s
WHERE Sales=(SELECT MAX(Sales)
FROM superstore_raw
WHERE [Customer ID]=s.[Customer ID]);

/* Q3 */
WITH CustomerSales AS(
SELECT [Customer ID],[Customer Name],SUM(Sales) TotalSales
FROM superstore_raw
GROUP BY [Customer ID],[Customer Name])
SELECT * FROM CustomerSales;

/* Q4 */
WITH CustomerSales AS(
SELECT [Customer ID],[Customer Name],SUM(Sales) TotalSales
FROM superstore_raw
GROUP BY [Customer ID],[Customer Name])
SELECT *
FROM CustomerSales
WHERE TotalSales>(SELECT AVG(TotalSales) FROM CustomerSales);

/* Q5 */
SELECT [Customer Name],SUM(Sales) TotalSales,
RANK() OVER(ORDER BY SUM(Sales) DESC) CustomerRank
FROM superstore_raw
GROUP BY [Customer Name];

/* Q6 */
SELECT [Customer Name],[Order ID],Sales,
ROW_NUMBER() OVER(PARTITION BY [Customer Name] ORDER BY Sales DESC) OrderSequence
FROM superstore_raw;

/* Q7 */
SELECT *
FROM(
SELECT [Customer Name],SUM(Sales) TotalSales,
RANK() OVER(ORDER BY SUM(Sales) DESC) CustomerRank
FROM superstore_raw
GROUP BY [Customer Name])
WHERE CustomerRank<=3;

/* Final Query */
WITH CustomerSales AS(
SELECT c.[Customer ID],c.[Customer Name],SUM(r.Sales) TotalSales
FROM customers c
JOIN superstore_raw r
ON c.[Customer ID]=r.[Customer ID]
GROUP BY c.[Customer ID],c.[Customer Name])
SELECT [Customer Name],TotalSales,
RANK() OVER(ORDER BY TotalSales DESC) CustomerRank
FROM CustomerSales;

/* Mini Project */
SELECT [Customer Name],SUM(Sales) TotalSales FROM superstore_raw GROUP BY [Customer Name] ORDER BY TotalSales DESC LIMIT 5;
SELECT [Customer Name],SUM(Sales) TotalSales FROM superstore_raw GROUP BY [Customer Name] ORDER BY TotalSales ASC LIMIT 5;
SELECT [Customer Name],COUNT(DISTINCT [Order ID]) OrderCount FROM superstore_raw GROUP BY [Customer Name] HAVING OrderCount=1;
WITH CustomerSales AS(SELECT [Customer Name],SUM(Sales) TotalSales FROM superstore_raw GROUP BY [Customer Name])
SELECT * FROM CustomerSales WHERE TotalSales>(SELECT AVG(TotalSales) FROM CustomerSales);
SELECT [Customer Name],MAX(Sales) HighestOrderValue FROM superstore_raw GROUP BY [Customer Name];
