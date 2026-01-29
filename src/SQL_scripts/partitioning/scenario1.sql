-- Section 1: Create the Destination Partition Tables
-- Tests: Identifying multiple targets from a single source schema.
IF OBJECT_ID('Table_Orders_1996', 'U') IS NOT NULL DROP TABLE Table_Orders_1996;
IF OBJECT_ID('Table_Orders_1997', 'U') IS NOT NULL DROP TABLE Table_Orders_1997;
IF OBJECT_ID('Table_Orders_1998', 'U') IS NOT NULL DROP TABLE Table_Orders_1998;

CREATE TABLE Table_Orders_1996 (OrderID INT, CustID NCHAR(5), TotalValue DECIMAL(18,2));
CREATE TABLE Table_Orders_1997 (OrderID INT, CustID NCHAR(5), TotalValue DECIMAL(18,2));
CREATE TABLE Table_Orders_1998 (OrderID INT, CustID NCHAR(5), TotalValue DECIMAL(18,2));
GO

-- Section 2: Shredding the Data
-- Tests: Lineage through filtered INSERT statements.
-- 1996 Partition
INSERT INTO Table_Orders_1996 (OrderID, CustID, TotalValue)
SELECT o.OrderID, o.CustomerID, SUM(od.UnitPrice * od.Quantity)
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
WHERE YEAR(o.OrderDate) = 1996
GROUP BY o.OrderID, o.CustomerID;

-- 1997 Partition
INSERT INTO Table_Orders_1997 (OrderID, CustID, TotalValue)
SELECT o.OrderID, o.CustomerID, SUM(od.UnitPrice * od.Quantity)
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
WHERE YEAR(o.OrderDate) = 1997
GROUP BY o.OrderID, o.CustomerID;

-- 1998 Partition
INSERT INTO Table_Orders_1998 (OrderID, CustID, TotalValue)
SELECT o.OrderID, o.CustomerID, SUM(od.UnitPrice * od.Quantity)
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
WHERE YEAR(o.OrderDate) = 1998
GROUP BY o.OrderID, o.CustomerID;

-- Log Lineage (Generic example for 1996)
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Orders', 'OrderID', CAST(OrderID AS VARCHAR), 'Table_Orders_1996', 'OrderID', CAST(OrderID AS VARCHAR)
FROM Table_Orders_1996;
GO

INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Orders', 'OrderID', CAST(OrderID AS VARCHAR), 'Table_Orders_1997', 'OrderID', CAST(OrderID AS VARCHAR)
FROM Table_Orders_1997;
GO

INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Orders', 'OrderID', CAST(OrderID AS VARCHAR), 'Table_Orders_1998', 'OrderID', CAST(OrderID AS VARCHAR)
FROM Table_Orders_1998;
GO

-- Section 3: Consolidation View for Reporting
-- Tests: Convergent lineage where partitioned tables are reunited.
CREATE OR ALTER VIEW vw_AllTimeSalesSummary AS
SELECT '1996' AS SalesYear, SUM(TotalValue) AS YearlyTotal FROM Table_Orders_1996
UNION ALL
SELECT '1997' AS SalesYear, SUM(TotalValue) AS YearlyTotal FROM Table_Orders_1997
UNION ALL
SELECT '1998' AS SalesYear, SUM(TotalValue) AS YearlyTotal FROM Table_Orders_1998;
GO