-- Section 0: Setup Partitioning Infrastructure
IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'SalesPartitionScheme')
    DROP PARTITION SCHEME SalesPartitionScheme;
IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'SalesPartitionFunction')
    DROP PARTITION FUNCTION SalesPartitionFunction;

CREATE PARTITION FUNCTION SalesPartitionFunction (DATETIME)
AS RANGE RIGHT FOR VALUES ('1998-01-01', '1998-02-01', '1998-03-01');

CREATE PARTITION SCHEME SalesPartitionScheme
AS PARTITION SalesPartitionFunction ALL TO ([PRIMARY]);
GO

-- Section 1: Create Tables with Synchronized Clustered Indexes
IF OBJECT_ID('Table_Monthly_Sales_Partitioned', 'U') IS NOT NULL 
    DROP TABLE Table_Monthly_Sales_Partitioned;
IF OBJECT_ID('Table_Sales_Staging_Jan', 'U') IS NOT NULL 
    DROP TABLE Table_Sales_Staging_Jan;

-- Staging Table with explicit Clustered Index
CREATE TABLE Table_Sales_Staging_Jan (
    OrderID INT NOT NULL,
    SaleDate DATETIME NOT NULL,
    Amount DECIMAL(18,2),
    CONSTRAINT PK_Staging PRIMARY KEY CLUSTERED (OrderID, SaleDate),
    CONSTRAINT CK_JanDate CHECK (SaleDate >= '1998-01-01' AND SaleDate < '1998-02-01')
);

-- Target Table with MATCHING Clustered Index on the Partition Scheme
CREATE TABLE Table_Monthly_Sales_Partitioned (
    OrderID INT NOT NULL,
    SaleDate DATETIME NOT NULL,
    Amount DECIMAL(18,2),
    CONSTRAINT PK_Partitioned PRIMARY KEY CLUSTERED (OrderID, SaleDate)
) ON SalesPartitionScheme(SaleDate); 
GO

-- Section 2: Populate Staging Table
INSERT INTO Table_Sales_Staging_Jan (OrderID, SaleDate, Amount)
SELECT o.OrderID, o.OrderDate, SUM(od.UnitPrice * od.Quantity)
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
WHERE o.OrderDate >= '1998-01-01' AND o.OrderDate < '1998-02-01'
GROUP BY o.OrderID, o.OrderDate;

-- Log Lineage
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Orders', 'OrderID', CAST(OrderID AS VARCHAR), 'Table_Sales_Staging_Jan', 'OrderID', CAST(OrderID AS VARCHAR)
FROM Table_Sales_Staging_Jan;
GO

-- Section 3: The Metadata Switch (Now with matching indexes)
-- We switch into Partition 2 (Jan 1998)
ALTER TABLE Table_Sales_Staging_Jan SWITCH TO Table_Monthly_Sales_Partitioned PARTITION 2;

-- Manual Lineage Log
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
VALUES ('Table_Sales_Staging_Jan', 'OrderID', 'Metadata-Switch', 'Table_Monthly_Sales_Partitioned', 'Partition', '2');
GO

-- Section 4: Final Aggregated Export
IF OBJECT_ID('Final_Monthly_Performance_Report', 'U') IS NOT NULL DROP TABLE Final_Monthly_Performance_Report;
CREATE TABLE Final_Monthly_Performance_Report (ReportID INT PRIMARY KEY, TotalMonthlySales DECIMAL(18,2));

INSERT INTO Final_Monthly_Performance_Report (ReportID, TotalMonthlySales)
SELECT NEXT VALUE FOR GlobalIDSequence, SUM(Amount)
FROM Table_Monthly_Sales_Partitioned
WHERE SaleDate >= '1998-01-01' AND SaleDate < '1998-02-01';

-- Log Lineage
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Table_Monthly_Sales_Partitioned', 'Partition_2', 'Aggregation', 'Final_Monthly_Performance_Report', 'ReportID', 'Monthly_Total'
FROM Final_Monthly_Performance_Report;
GO