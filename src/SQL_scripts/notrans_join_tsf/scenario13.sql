-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Employee-Territory Assignment & Regional Alignment Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for all entities involved in the Join relationship.

IF OBJECT_ID('Table_EmployeeTerritoryRegionLedger', 'U') IS NOT NULL DROP TABLE Table_EmployeeTerritoryRegionLedger;
CREATE TABLE Table_EmployeeTerritoryRegionLedger (
    AssignmentID INT,
    EmployeeID INT,           -- Native Projection from Employees
    TerritoryID NVARCHAR(20), -- Native Projection from Territories
    RegionID INT,             -- Native Projection from Region
    LastName NVARCHAR(20),    -- Native Projection from Employees
    TerritoryDescription NCHAR(50), -- Native Projection from Territories
    RegionDescription NCHAR(50)     -- Native Projection from Region
);
GO

DECLARE @v_EmployeeID INT,
        @v_TerritoryID NVARCHAR(20),
        @v_RegionID INT,
        @v_LastName NVARCHAR(20),
        @v_TerritoryDesc NCHAR(50),
        @v_RegionDesc NCHAR(50),
        @nextAssignmentID INT;

-- Cursor using a complex JOIN for strict projection across HR and Geographic domains.
-- Selection: Only employees with the title 'Inside Sales Coordinator' mapped to their respective regions.
-- All attributes are native; no string formatting or logic is applied to descriptions or names.
DECLARE AssignmentCursor CURSOR FOR 
    SELECT 
        E.EmployeeID, 
        T.TerritoryID, 
        R.RegionID, 
        E.LastName, 
        T.TerritoryDescription, 
        R.RegionDescription
    FROM Employees E
    INNER JOIN EmployeeTerritories ET ON E.EmployeeID = ET.EmployeeID
    INNER JOIN Territories T ON ET.TerritoryID = T.TerritoryID
    INNER JOIN Region R ON T.RegionID = R.RegionID
    WHERE E.Title = 'Inside Sales Coordinator'; -- Selection

OPEN AssignmentCursor;
FETCH NEXT FROM AssignmentCursor INTO 
    @v_EmployeeID, @v_TerritoryID, @v_RegionID, @v_LastName, @v_TerritoryDesc, @v_RegionDesc;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAssignmentID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data (No Transformations)
    INSERT INTO Table_EmployeeTerritoryRegionLedger (
        AssignmentID, EmployeeID, TerritoryID, RegionID, LastName, TerritoryDescription, RegionDescription
    )
    VALUES (
        @nextAssignmentID, @v_EmployeeID, @v_TerritoryID, @v_RegionID, @v_LastName, @v_TerritoryDesc, @v_RegionDesc
    );

    -- Log Triple-Source Lineage
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_EmployeeTerritoryRegionLedger', 'AssignmentID', CAST(@nextAssignmentID AS VARCHAR));
    
    -- Record source for Territories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', CAST(@v_TerritoryID AS VARCHAR), 'Table_EmployeeTerritoryRegionLedger', 'AssignmentID', CAST(@nextAssignmentID AS VARCHAR));

    -- Record source for Region
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Region', 'RegionID', CAST(@v_RegionID AS VARCHAR), 'Table_EmployeeTerritoryRegionLedger', 'AssignmentID', CAST(@nextAssignmentID AS VARCHAR));
    
    FETCH NEXT FROM AssignmentCursor INTO 
        @v_EmployeeID, @v_TerritoryID, @v_RegionID, @v_LastName, @v_TerritoryDesc, @v_RegionDesc;
END;

CLOSE AssignmentCursor; 
DEALLOCATE AssignmentCursor;
GO