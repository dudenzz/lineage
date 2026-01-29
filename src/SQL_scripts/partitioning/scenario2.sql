-- Section 0: Ensure Sequence Exists
IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'GlobalIDSequence')
    CREATE SEQUENCE GlobalIDSequence START WITH 1 INCREMENT BY 1;
GO

-- Section 1: Create Regional Sink Tables
IF OBJECT_ID('Table_Sales_Americas', 'U') IS NOT NULL DROP TABLE Table_Sales_Americas;
IF OBJECT_ID('Table_Sales_Europe', 'U') IS NOT NULL DROP TABLE Table_Sales_Europe;

CREATE TABLE Table_Sales_Americas (OrderID INT PRIMARY KEY, CustomerName NVARCHAR(40), RegionCode NVARCHAR(15));
CREATE TABLE Table_Sales_Europe (OrderID INT PRIMARY KEY, CustomerName NVARCHAR(40), RegionCode NVARCHAR(15));
GO

-- Section 2: Conditional Data Distribution
-- Americas Partition
INSERT INTO Table_Sales_Americas (OrderID, CustomerName, RegionCode)
SELECT o.OrderID, c.CompanyName, c.Region
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE c.Region IN ('WA', 'OR', 'CA', 'BC', 'SP', 'DF');

-- Europe Partition
INSERT INTO Table_Sales_Europe (OrderID, CustomerName, RegionCode)
SELECT o.OrderID, c.CompanyName, c.Region
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE c.Country IN ('UK', 'France', 'Germany', 'Italy', 'Norway');

-- Log Lineage: Capturing the Shredding
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Orders', 'OrderID', CAST(OrderID AS VARCHAR), 'Table_Sales_Americas', 'OrderID', CAST(OrderID AS VARCHAR)
FROM Table_Sales_Americas;

INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Orders', 'OrderID', CAST(OrderID AS VARCHAR), 'Table_Sales_Europe', 'OrderID', CAST(OrderID AS VARCHAR)
FROM Table_Sales_Europe;
GO

-- Section 3: Cross-Partition Validation Procedure
-- FIX: Using a CTE to separate the UNION ALL from the NEXT VALUE FOR function
CREATE OR ALTER PROCEDURE proc_DetectRegionalOrphans AS
BEGIN
    IF OBJECT_ID('Final_Orphan_Report', 'U') IS NOT NULL DROP TABLE Final_Orphan_Report;
    CREATE TABLE Final_Orphan_Report (ReportID INT PRIMARY KEY, SourceTable NVARCHAR(50), MissingOrderID INT);

    WITH UnionedOrphans AS (
        SELECT 'Americas' AS Src, OrderID FROM Table_Sales_Americas WHERE RegionCode IS NULL
        UNION ALL
        SELECT 'Europe' AS Src, OrderID FROM Table_Sales_Europe WHERE RegionCode IS NULL
    )
    INSERT INTO Final_Orphan_Report (ReportID, SourceTable, MissingOrderID)
    SELECT NEXT VALUE FOR GlobalIDSequence, Src, OrderID
    FROM UnionedOrphans;

    -- Log Lineage: Capture the consolidation into the report
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Sales_Americas', 'OrderID', CAST(MissingOrderID AS VARCHAR), 'Final_Orphan_Report', 'ReportID', 'Anomaly'
    FROM Final_Orphan_Report WHERE SourceTable = 'Americas';

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Sales_Europe', 'OrderID', CAST(MissingOrderID AS VARCHAR), 'Final_Orphan_Report', 'ReportID', 'Anomaly'
    FROM Final_Orphan_Report WHERE SourceTable = 'Europe';
END;
GO

EXEC proc_DetectRegionalOrphans;