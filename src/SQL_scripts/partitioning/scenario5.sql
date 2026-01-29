-- Section 1: Infrastructure Setup (Multiple Schemas)
-- Tests: Ability to resolve objects across different database schemas.


-- Create the destination tables in different schemas
IF OBJECT_ID('Orders_History', 'U') IS NOT NULL DROP TABLE Orders_History;
IF OBJECT_ID('Regional_Sales_Cube', 'U') IS NOT NULL DROP TABLE Regional_Sales_Cube;

CREATE TABLE Orders_History (
    OrderID INT PRIMARY KEY,
    ArchivedDate DATETIME DEFAULT GETDATE(),
    OriginalOrderData XML -- Testing complex data type lineage
);

CREATE TABLE Regional_Sales_Cube (
    RegionID INT,
    TotalRevenue DECIMAL(18,2),
    SnapshotDate DATETIME
);
GO

-- Section 2: Data Shredding into Archive Schema
-- Tests: Lineage through XML serialization into a different schema.
INSERT INTO Orders_History (OrderID, OriginalOrderData)
SELECT 
    OrderID, 
    (SELECT * FROM Orders WHERE OrderID = o.OrderID FOR XML AUTO)
FROM Orders o
WHERE OrderDate < '1997-01-01';

-- Log Lineage:
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'dbo.Orders', 'OrderID', CAST(OrderID AS VARCHAR), 'Orders_History', 'OrderID', CAST(OrderID AS VARCHAR)
FROM Orders_History;
GO

-- Section 3: Data Shredding into Reporting Schema
-- Tests: Multi-source join across schemas to create a partitioned aggregate.
INSERT INTO Regional_Sales_Cube (RegionID, TotalRevenue, SnapshotDate)
SELECT 
    r.RegionID,
    SUM(od.UnitPrice * od.Quantity),
    GETDATE()
FROM dbo.Region r
JOIN dbo.Territories t ON r.RegionID = t.RegionID
JOIN dbo.EmployeeTerritories et ON t.TerritoryID = et.TerritoryID
JOIN dbo.Orders o ON et.EmployeeID = o.EmployeeID
JOIN dbo.[Order Details] od ON o.OrderID = od.OrderID
GROUP BY r.RegionID;

-- Log Lineage:
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'dbo.Region', 'RegionID', CAST(RegionID AS VARCHAR), 'Regional_Sales_Cube', 'RegionID', CAST(RegionID AS VARCHAR)
FROM Regional_Sales_Cube;
GO

-- Section 4: Final Cross-Schema Reconciliation
-- Tests: Joining data from two different non-dbo schemas.
CREATE OR ALTER VIEW vw_Archive_Coverage_Audit AS
SELECT 
    a.OrderID,
    r.RegionID,
    'Archived' AS Status
FROM Orders_History a
CROSS JOIN Regional_Sales_Cube r;
GO