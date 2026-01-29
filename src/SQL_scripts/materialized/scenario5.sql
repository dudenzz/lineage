-- Section 0: Ensure Sequence Exists
IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'GlobalIDSequence')
    CREATE SEQUENCE GlobalIDSequence START WITH 1 INCREMENT BY 1;
GO

-- Section 1: Define the Hierarchical Leaderboard Table
IF OBJECT_ID('Table_Manager_Revenue_Leaderboard', 'U') IS NOT NULL DROP TABLE Table_Manager_Revenue_Leaderboard;

CREATE TABLE Table_Manager_Revenue_Leaderboard (
    LeaderboardID INT PRIMARY KEY,
    ManagerID INT,
    ManagerName NVARCHAR(100),
    TotalTeamRevenue DECIMAL(18,2),
    SubordinateCount INT,
    AsOfDate DATETIME DEFAULT GETDATE()
);
GO

-- Section 2: Recursive Aggregation and Materialization
-- Tests: Lineage through Recursive CTE + multi-table joins.
WITH OrgHierarchy AS (
    -- Anchor: Every employee
    SELECT 
        EmployeeID, 
        ReportsTo, 
        FirstName + ' ' + LastName AS EmpName,
        EmployeeID AS TopManagerID 
    FROM Employees

    UNION ALL

    -- Recursive: Link subordinates back up to their original TopManagerID anchor
    SELECT 
        e.EmployeeID, 
        e.ReportsTo, 
        e.FirstName + ' ' + e.LastName,
        oh.TopManagerID
    FROM Employees e
    INNER JOIN OrgHierarchy oh ON e.ReportsTo = oh.EmployeeID
)
INSERT INTO Table_Manager_Revenue_Leaderboard (
    LeaderboardID, 
    ManagerID, 
    ManagerName, 
    TotalTeamRevenue, 
    SubordinateCount
)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    oh.TopManagerID,
    MAX(m.FirstName + ' ' + m.LastName),
    SUM(od.UnitPrice * od.Quantity),
    COUNT(DISTINCT oh.EmployeeID) - 1 
FROM OrgHierarchy oh
JOIN Orders o ON oh.EmployeeID = o.EmployeeID
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Employees m ON oh.TopManagerID = m.EmployeeID
GROUP BY oh.TopManagerID;

-- Log Lineage: Recursive dependency
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Employees', 'ReportsTo', 'Recursive-Path', 'Table_Manager_Revenue_Leaderboard', 'LeaderboardID', 'Team-Rollup'
FROM Table_Manager_Revenue_Leaderboard;
GO

-- Section 3: Executive Tier Classification
-- FIX: Increased Tier length to NVARCHAR(30) to accommodate 'Individual Contributor'
IF OBJECT_ID('Final_Org_Structure_Report', 'U') IS NOT NULL DROP TABLE Final_Org_Structure_Report;
CREATE TABLE Final_Org_Structure_Report (
    ReportID INT PRIMARY KEY,
    ManagerName NVARCHAR(100),
    Tier NVARCHAR(30) -- Increased from 20 to 30
);

INSERT INTO Final_Org_Structure_Report (ReportID, ManagerName, Tier)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    ManagerName,
    CASE 
        WHEN SubordinateCount > 5 THEN 'Director'
        WHEN SubordinateCount > 0 THEN 'Manager'
        ELSE 'Individual Contributor'
    END
FROM Table_Manager_Revenue_Leaderboard;

-- Log Lineage: Logical classification
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Table_Manager_Revenue_Leaderboard', 'LeaderboardID', 'Logic-Classification', 'Final_Org_Structure_Report', 'ReportID', 'Tier-Audit'
FROM Final_Org_Structure_Report;
GO