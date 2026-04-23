-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling a Territory-Based Sales Coverage & Employee Management Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_TerritoryCoverageLedger', 'U') IS NOT NULL DROP TABLE Table_TerritoryCoverageLedger;
CREATE TABLE Table_TerritoryCoverageLedger (
    CoverageAuditID INT,
    EmployeeID INT,           -- Native Projection from Employees
    TerritoryID NVARCHAR(20), -- Native Projection from Territories
    LastName NVARCHAR(20),    -- Native Projection from Employees
    TerritoryDescription NCHAR(50), -- Native Projection from Territories
    RegionID INT              -- Native Projection from Territories
);
GO

DECLARE @v_EmployeeID INT,
        @v_TerritoryID NVARCHAR(20),
        @v_LastName NVARCHAR(20),
        @v_TerritoryDescription NCHAR(50),
        @v_RegionID INT,
        @nextCoverageAuditID INT;

-- Cursor using JOIN for strict projection across Employee assignments and Geographic territories.
-- Selection: Only territories within Region 3 (Northern) handled by specific staff.
-- All attributes are native; no linear transformations or string concatenations are applied.
DECLARE TerritoryCursor CURSOR FOR 
    SELECT 
        E.EmployeeID, 
        T.TerritoryID, 
        E.LastName, 
        T.TerritoryDescription,
        T.RegionID
    FROM Employees E
    INNER JOIN EmployeeTerritories ET ON E.EmployeeID = ET.EmployeeID
    INNER JOIN Territories T ON ET.TerritoryID = T.TerritoryID
    WHERE T.RegionID = 3; -- Selection

OPEN TerritoryCursor;
FETCH NEXT FROM TerritoryCursor INTO 
    @v_EmployeeID, @v_TerritoryID, @v_LastName, @v_TerritoryDescription, @v_RegionID;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextCoverageAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_TerritoryCoverageLedger (
        CoverageAuditID, EmployeeID, TerritoryID, LastName, TerritoryDescription, RegionID
    )
    VALUES (
        @nextCoverageAuditID, @v_EmployeeID, @v_TerritoryID, @v_LastName, @v_TerritoryDescription, @v_RegionID
    );

    -- Log Multi-Source Lineage
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_TerritoryCoverageLedger', 'CoverageAuditID', CAST(@nextCoverageAuditID AS VARCHAR));
    
    -- Record source for Territories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', CAST(@v_TerritoryID AS VARCHAR), 'Table_TerritoryCoverageLedger', 'CoverageAuditID', CAST(@nextCoverageAuditID AS VARCHAR));
    
    FETCH NEXT FROM TerritoryCursor INTO 
        @v_EmployeeID, @v_TerritoryID, @v_LastName, @v_TerritoryDescription, @v_RegionID;
END;

CLOSE TerritoryCursor; 
DEALLOCATE TerritoryCursor;
GO