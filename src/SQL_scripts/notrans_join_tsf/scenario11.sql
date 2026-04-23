-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Territory-Region Geographic Alignment Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_TerritoryRegionLedger', 'U') IS NOT NULL DROP TABLE Table_TerritoryRegionLedger;
CREATE TABLE Table_TerritoryRegionLedger (
    AlignmentID INT,
    TerritoryID NVARCHAR(20),      -- Native Projection from Territories
    RegionID INT,                 -- Native Projection from Region
    TerritoryDescription NCHAR(50), -- Native Projection from Territories
    RegionDescription NCHAR(50)    -- Native Projection from Region
);
GO

DECLARE @v_TerritoryID NVARCHAR(20),
        @v_RegionID INT,
        @v_TerritoryDescription NCHAR(50),
        @v_RegionDescription NCHAR(50),
        @nextAlignmentID INT;

-- Cursor using JOIN for strict projection across Geographic Definitions.
-- Selection: Only territories mapped to the 'Western' region (RegionID 2).
-- All columns are native; no string modifications or spatial transformations are performed.
DECLARE TerritoryRegionCursor CURSOR FOR 
    SELECT 
        T.TerritoryID, 
        R.RegionID, 
        T.TerritoryDescription, 
        R.RegionDescription
    FROM Territories T
    INNER JOIN Region R ON T.RegionID = R.RegionID
    WHERE R.RegionID = 2; -- Selection (Western Region)

OPEN TerritoryRegionCursor;
FETCH NEXT FROM TerritoryRegionCursor INTO 
    @v_TerritoryID, @v_RegionID, @v_TerritoryDescription, @v_RegionDescription;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAlignmentID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_TerritoryRegionLedger (
        AlignmentID, TerritoryID, RegionID, TerritoryDescription, RegionDescription
    )
    VALUES (
        @nextAlignmentID, @v_TerritoryID, @v_RegionID, @v_TerritoryDescription, @v_RegionDescription
    );

    -- Log Dual-Source Lineage
    -- Record source for Territories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', CAST(@v_TerritoryID AS VARCHAR), 'Table_TerritoryRegionLedger', 'AlignmentID', CAST(@nextAlignmentID AS VARCHAR));
    
    -- Record source for Region
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Region', 'RegionID', CAST(@v_RegionID AS VARCHAR), 'Table_TerritoryRegionLedger', 'AlignmentID', CAST(@nextAlignmentID AS VARCHAR));
    
    FETCH NEXT FROM TerritoryRegionCursor INTO 
        @v_TerritoryID, @v_RegionID, @v_TerritoryDescription, @v_RegionDescription;
END;

CLOSE TerritoryRegionCursor; 
DEALLOCATE TerritoryRegionCursor;
GO