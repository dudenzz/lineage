-- Section 1: Recursive CTE with Parameterized Entry Point
-- Tests: Lineage through recursion that starts at a filtered node.
DECLARE @TargetManager NVARCHAR(50) = 'Fuller';

WITH SubordinateBranch AS (
    -- Anchor: Find the specific manager by name
    SELECT 
        EmployeeID, 
        LastName, 
        FirstName, 
        ReportsTo, 
        0 AS Level
    FROM Employees
    WHERE LastName = @TargetManager

    UNION ALL

    -- Recursive: Find everyone who reports to the previous level
    SELECT 
        e.EmployeeID, 
        e.LastName, 
        e.FirstName, 
        e.ReportsTo, 
        sb.Level + 1
    FROM Employees e
    INNER JOIN SubordinateBranch sb ON e.ReportsTo = sb.EmployeeID
)
-- Move the branch into a temp table
SELECT * INTO #ManagerBranch FROM SubordinateBranch;

-- Log Lineage:
-- Tool must recognize 'Employees' as the source for the entire branch.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Employees', 'EmployeeID', CAST(EmployeeID AS VARCHAR), '#ManagerBranch', 'EmployeeID', CAST(EmployeeID AS VARCHAR)
FROM #ManagerBranch;
GO

-- Section 2: Creating a Physical Department Table
-- Tests: Mapping recursive results into a structured physical table.
IF OBJECT_ID('Table_Dept_Snapshot', 'U') IS NOT NULL DROP TABLE Table_Staff_Snapshot;
CREATE TABLE Table_Staff_Snapshot (
    SnapID INT PRIMARY KEY,
    EmpID INT,
    ManagerRefID INT,
    OrgLevel INT
);

-- We use the temp table to populate the physical snapshot
INSERT INTO Table_Staff_Snapshot (SnapID, EmpID, ManagerRefID, OrgLevel)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    EmployeeID,
    ReportsTo,
    Level
FROM #ManagerBranch;

-- Log Lineage
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT '#ManagerBranch', 'EmployeeID', CAST(EmployeeID AS VARCHAR), 'Table_Staff_Snapshot', 'SnapID', 'Sequence'
FROM #ManagerBranch;
GO

-- Section 3: Final Procedural Report Generation
-- Tests: Finalizing lineage through a procedure that calculates "Span of Control".
CREATE OR ALTER PROCEDURE proc_CalculateManagerSpan AS
BEGIN
    IF OBJECT_ID('Final_Manager_Span_Report', 'U') IS NOT NULL DROP TABLE Final_Manager_Span_Report;
    
    -- Creation involves aggregating the previously created snapshot
    CREATE TABLE Final_Manager_Span_Report (
        ReportID INT, 
        ManagerID INT, 
        TotalSubordinates INT
    );

    INSERT INTO Final_Manager_Span_Report (ReportID, ManagerID, TotalSubordinates)
    SELECT 
        NEXT VALUE FOR GlobalIDSequence,
        ManagerRefID,
        COUNT(EmpID)
    FROM Table_Staff_Snapshot
    WHERE ManagerRefID IS NOT NULL
    GROUP BY ManagerRefID;

    -- Log Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Staff_Snapshot', 'ManagerRefID', CAST(ManagerRefID AS VARCHAR), 'Final_Manager_Span_Report', 'ReportID', 'Aggregated'
    FROM Final_Manager_Span_Report;
END;
GO

EXEC proc_CalculateManagerSpan;