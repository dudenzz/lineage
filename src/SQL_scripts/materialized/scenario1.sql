-- Section 1: Define the Materialized Reporting Table
-- Tests: Ability to map lineage to a table specifically designed for historical snapshots.
IF OBJECT_ID('Table_Monthly_Sales_Snapshot', 'U') IS NOT NULL DROP TABLE Table_Monthly_Sales_Snapshot;

CREATE TABLE Table_Monthly_Sales_Snapshot (
    SnapshotID INT PRIMARY KEY,
    ReportingMonth INT,
    ReportingYear INT,
    TotalRevenue DECIMAL(18,2),
    OrderCount INT,
    UniqueCustomerCount INT,
    LastRefreshDate DATETIME DEFAULT GETDATE()
);
GO

-- Section 2: Generation of the Materialized Summary
-- Tests: Tracking lineage through multiple layers of aggregation (SUM, COUNT, COUNT DISTINCT).
-- The tool must link 'Orders', 'Order Details', and 'Products' to the summary table.
INSERT INTO Table_Monthly_Sales_Snapshot (
    SnapshotID, 
    ReportingMonth, 
    ReportingYear, 
    TotalRevenue, 
    OrderCount, 
    UniqueCustomerCount
)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    MONTH(o.OrderDate) AS RMonth,
    YEAR(o.OrderDate) AS RYear,
    SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS Revenue,
    COUNT(o.OrderID) AS TotalOrders,
    COUNT(DISTINCT o.CustomerID) AS UniqueCusts
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
WHERE o.OrderDate >= '1997-01-01' AND o.OrderDate < '1997-02-01'
GROUP BY MONTH(o.OrderDate), YEAR(o.OrderDate);

-- Log Lineage:
-- Tool must attribute 'Revenue' to 'UnitPrice', 'Quantity', and 'Discount' columns.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Orders', 'OrderID', 'Aggregation', 'Table_Monthly_Sales_Snapshot', 'SnapshotID', 'Jan1997'
FROM Table_Monthly_Sales_Snapshot;
GO

-- Section 3: Derived KPI Calculation
-- Tests: Lineage from one materialized summary into a secondary "KPI" table.
IF OBJECT_ID('Table_Executive_KPIs', 'U') IS NOT NULL DROP TABLE Table_Executive_KPIs;
CREATE TABLE Table_Executive_KPIs (
    KPI_ID INT PRIMARY KEY,
    MetricName NVARCHAR(50),
    MetricValue DECIMAL(18,2)
);

-- Calculate "Average Revenue Per Order" (ARPO) from the summary table
INSERT INTO Table_Executive_KPIs (KPI_ID, MetricName, MetricValue)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    'Average Revenue Per Order',
    TotalRevenue / NULLIF(OrderCount, 0)
FROM Table_Monthly_Sales_Snapshot
WHERE ReportingMonth = 1 AND ReportingYear = 1997;

-- Log Lineage:
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Table_Monthly_Sales_Snapshot', 'SnapshotID', 'MetricCalculation', 'Table_Executive_KPIs', 'KPI_ID', 'ARPO'
FROM Table_Executive_KPIs;
GO