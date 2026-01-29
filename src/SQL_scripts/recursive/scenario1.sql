-- Section 1: Recursive CTE to calculate Management Hierarchy
-- Tests: Ability to resolve a self-referencing CTE back to a single physical source.
WITH EmployeeHierarchy AS (
    -- Anchor Member: Start with the top-level managers (those who report to no one)
    SELECT 
        EmployeeID,
        LastName,
        FirstName,
        ReportsTo,
        0 AS HierarchyLevel,
        CAST(LastName AS VARCHAR(MAX)) AS ManagementChain
    FROM Employees
    WHERE ReportsTo IS NULL

    UNION ALL

    -- Recursive Member: Join the CTE back to the Employees table
    SELECT 
        e.EmployeeID,
        e.LastName,
        e.FirstName,
        e.ReportsTo,
        eh.HierarchyLevel + 1,
        CAST(eh.ManagementChain + ' -> ' + e.LastName AS VARCHAR(MAX))
    FROM Employees e
    INNER JOIN EmployeeHierarchy eh ON e.ReportsTo = eh.EmployeeID
)
-- Persist the results into a physical table
-- Tests: Lineage from a recursive logical structure to a physical object.
SELECT * INTO #FlattenedHierarchy FROM EmployeeHierarchy;

-- Log Lineage: Captures the self-reference
-- Tool must recognize 'Employees' as the sole source despite the recursion.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Employees', 'EmployeeID', CAST(EmployeeID AS VARCHAR), '#FlattenedHierarchy', 'EmployeeID', CAST(EmployeeID AS VARCHAR)
FROM #FlattenedHierarchy;
GO

-- Section 2: Final Organizational Audit Table
-- Tests: Passing data from a recursive result to a final physical destination.
IF OBJECT_ID('Final_Org_Audit', 'U') IS NOT NULL DROP TABLE Final_Org_Audit;
CREATE TABLE Final_Org_Audit (
    AuditID INT PRIMARY KEY,
    EmployeeID INT,
    DepthLevel INT,
    PathString NVARCHAR(MAX)
);

DECLARE @eid INT, @depth INT, @path NVARCHAR(MAX), @nextAuditID INT;
DECLARE OrgCursor CURSOR FOR SELECT EmployeeID, HierarchyLevel, ManagementChain FROM #FlattenedHierarchy;

OPEN OrgCursor;
FETCH NEXT FROM OrgCursor INTO @eid, @depth, @path;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Final_Org_Audit (AuditID, EmployeeID, DepthLevel, PathString)
    VALUES (@nextAuditID, @eid, @depth, @path);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('#FlattenedHierarchy', 'EmployeeID', CAST(@eid AS VARCHAR), 'Final_Org_Audit', 'AuditID', CAST(@nextAuditID AS VARCHAR));

    FETCH NEXT FROM OrgCursor INTO @eid, @depth, @path;
END;
CLOSE OrgCursor; DEALLOCATE OrgCursor;

DROP TABLE #FlattenedHierarchy;
GO