-- Section 1: Define the Multi-Dimensional Summary Table
-- Tests: Lineage into a "Data Mart" style object with multiple dimensional anchors.
IF OBJECT_ID('Table_Employee_Category_Matrix', 'U') IS NOT NULL DROP TABLE Table_Employee_Category_Matrix;

CREATE TABLE Table_Employee_Category_Matrix (
    MatrixID INT PRIMARY KEY,
    EmployeeName NVARCHAR(100),
    CategoryName NVARCHAR(50),
    TotalQuantitySold INT,
    TotalRevenue DECIMAL(18,2),
    SnapshotYear INT
);
GO

-- Section 2: Materializing the Cross-Join Aggregation
-- Tests: Tracking 'Used for Creation' across a 5-table join.
-- The tool must attribute the 'TotalRevenue' to the intersection of Employee and Category.
INSERT INTO Table_Employee_Category_Matrix (
    MatrixID, 
    EmployeeName, 
    CategoryName, 
    TotalQuantitySold, 
    TotalRevenue, 
    SnapshotYear
)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    e.FirstName + ' ' + e.LastName,
    c.CategoryName,
    SUM(od.Quantity),
    SUM(od.UnitPrice * od.Quantity),
    1997 -- Hardcoded for this specific annual report run
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.EmployeeID
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID
JOIN Categories c ON p.CategoryID = c.CategoryID
WHERE o.OrderDate >= '1997-01-01' AND o.OrderDate <= '1997-12-31'
GROUP BY e.FirstName, e.LastName, c.CategoryName;

-- Log Lineage:
-- A robust tool should show edges from Employees, Categories, and Order Details.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Employees', 'EmployeeID', 'Multi-Dim-Agg', 'Table_Employee_Category_Matrix', 'MatrixID', '1997_Run'
FROM Table_Employee_Category_Matrix;
GO

-- Section 3: High-Value Performance Alerting
-- Tests: Lineage from a multi-dimensional summary to a targeted exception report.
IF OBJECT_ID('Final_Top_Performer_Audit', 'U') IS NOT NULL DROP TABLE Final_Top_Performer_Audit;
CREATE TABLE Final_Top_Performer_Audit (
    AuditID INT PRIMARY KEY,
    TopPerformer NVARCHAR(100),
    SpecialtyCategory NVARCHAR(50),
    RevenueThreshold DECIMAL(18,2)
);

-- Extract only the "Top Performers" who exceeded $5,000 in a specific category
INSERT INTO Final_Top_Performer_Audit (AuditID, TopPerformer, SpecialtyCategory, RevenueThreshold)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    EmployeeName,
    CategoryName,
    TotalRevenue
FROM Table_Employee_Category_Matrix
WHERE TotalRevenue > 5000;

-- Log Lineage:
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Table_Employee_Category_Matrix', 'MatrixID', 'Filter_Logic', 'Final_Top_Performer_Audit', 'AuditID', 'HighValue'
FROM Final_Top_Performer_Audit;
GO