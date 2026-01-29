-- Section 1: Create a View representing a 3-tier hierarchy
-- Tests: Lineage through multiple self-joins of the same table.
CREATE OR ALTER VIEW vw_EmployeeBreadcrumbs AS
SELECT 
    Staff.EmployeeID AS StaffID,
    Staff.LastName AS StaffName,
    ISNULL(Super.LastName, 'N/A') AS SupervisorName,
    ISNULL(Mgr.LastName, 'CEO') AS ManagerName,
    -- Constructing a logical path
    ISNULL(Mgr.LastName, 'CEO') + ' > ' + ISNULL(Super.LastName, 'N/A') + ' > ' + Staff.LastName AS OrgPath
FROM Employees Staff
LEFT JOIN Employees Super ON Staff.ReportsTo = Super.EmployeeID
LEFT JOIN Employees Mgr ON Super.ReportsTo = Mgr.EmployeeID;
GO

-- Log Lineage:
-- Tool must identify 'Employees' as the source for all three hierarchical columns.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Employees', 'EmployeeID', CAST(StaffID AS VARCHAR), 'vw_EmployeeBreadcrumbs', 'StaffID', CAST(StaffID AS VARCHAR)
FROM vw_EmployeeBreadcrumbs;
GO

-- Section 2: Materializing High-Level Org Units
-- Tests: Filtering a self-joined view into a physical target.
IF OBJECT_ID('Table_Management_Paths', 'U') IS NOT NULL DROP TABLE Table_Management_Paths;
CREATE TABLE Table_Management_Paths (
    PathID INT PRIMARY KEY,
    EmployeeID INT,
    FullBreadcrumb NVARCHAR(MAX)
);

DECLARE @e_id INT, @path NVARCHAR(MAX), @nextPathID INT;
DECLARE PathCursor CURSOR FOR 
    SELECT StaffID, OrgPath FROM vw_EmployeeBreadcrumbs 
    WHERE ManagerName <> 'CEO'; -- Filtering out the top-level branch

OPEN PathCursor;
FETCH NEXT FROM PathCursor INTO @e_id, @path;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextPathID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Management_Paths (PathID, EmployeeID, FullBreadcrumb)
    VALUES (@nextPathID, @e_id, @path);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_EmployeeBreadcrumbs', 'StaffID', CAST(@e_id AS VARCHAR), 'Table_Management_Paths', 'PathID', CAST(@nextPathID AS VARCHAR));

    FETCH NEXT FROM PathCursor INTO @e_id, @path;
END;
CLOSE PathCursor; DEALLOCATE PathCursor;
GO

-- Section 3: Procedural Update of Hierarchy Status
-- Tests: Finalizing lineage through an UPDATE based on hierarchical logic.
CREATE OR ALTER PROCEDURE proc_FinalizeOrgLabels AS
BEGIN
    IF OBJECT_ID('Final_Org_Labels', 'U') IS NOT NULL DROP TABLE Final_Org_Labels;
    CREATE TABLE Final_Org_Labels (LabelID INT, EmpID INT, Label NVARCHAR(50));

    INSERT INTO Final_Org_Labels (LabelID, EmpID, Label)
    SELECT 
        NEXT VALUE FOR GlobalIDSequence,
        EmployeeID,
        CASE 
            WHEN FullBreadcrumb LIKE '%Fuller%' THEN 'Fuller Org'
            ELSE 'Standard Org'
        END
    FROM Table_Management_Paths;

    -- Log Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Management_Paths', 'PathID', CAST(PathID AS VARCHAR), 'Final_Org_Labels', 'LabelID', 'Dynamic'
    FROM Table_Management_Paths;
END;
GO

EXEC proc_FinalizeOrgLabels;