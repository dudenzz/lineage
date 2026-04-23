-- Section 1: Create a View based on a simple SELECT
-- Scenario: Mirroring the Territories table for a regional alignment report.
-- Rule: No transformations, direct data copying only.
CREATE OR ALTER VIEW vw_TerritoryMirror AS
SELECT 
    TerritoryID, 
    TerritoryDescription, 
    RegionID
FROM Territories;
GO

-- Log Row-Level Lineage for View
DECLARE @tid NVARCHAR(20);
DECLARE ViewCursor CURSOR FOR SELECT TerritoryID FROM vw_TerritoryMirror;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @tid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', @tid, 'vw_TerritoryMirror', 'TerritoryID', @tid);
    FETCH NEXT FROM ViewCursor INTO @tid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
-- Tests: Lineage preservation during a simple filter (Region 1 only).
IF OBJECT_ID('Table_Region1_Territories', 'U') IS NOT NULL DROP TABLE Table_Region1_Territories;
CREATE TABLE Table_Region1_Territories (
    LocalID INT, 
    OriginalTerritoryID NVARCHAR(20), 
    Description NVARCHAR(50)
);
GO

DECLARE @v_tid NVARCHAR(20), @v_desc NVARCHAR(50), @nextLocalID INT;
-- Filter: Only Eastern Region (RegionID 1)
DECLARE TableCursor CURSOR FOR SELECT TerritoryID, TerritoryDescription FROM vw_TerritoryMirror WHERE RegionID = 1;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_tid, @v_desc;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLocalID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Region1_Territories (LocalID, OriginalTerritoryID, Description)
    VALUES (@nextLocalID, @v_tid, @v_desc);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_TerritoryMirror', 'TerritoryID', @v_tid, 'Table_Region1_Territories', 'LocalID', CAST(@nextLocalID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_tid, @v_desc;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeTerritoryReport AS
BEGIN
    IF OBJECT_ID('Final_TerritoryAssignmentReport', 'U') IS NOT NULL DROP TABLE Final_TerritoryAssignmentReport;
    CREATE TABLE Final_TerritoryAssignmentReport (ReportID INT, TerritoryName NVARCHAR(50), SyncStatus VARCHAR(10));

    DECLARE @t_id INT, @t_name NVARCHAR(50), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, Description FROM ##TempTerritoryBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_TerritoryAssignmentReport (ReportID, TerritoryName, SyncStatus)
        VALUES (@finalID, @t_name, 'Synced');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempTerritoryBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_TerritoryAssignmentReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessTerritoryStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempTerritoryBuffer') IS NOT NULL DROP TABLE ##TempTerritoryBuffer;
    CREATE TABLE ##TempTerritoryBuffer (TempID INT, Description NVARCHAR(50));

    DECLARE @tid INT, @desc NVARCHAR(50), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT LocalID, Description FROM Table_Region1_Territories;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @desc;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempTerritoryBuffer VALUES (@newTempID, @desc);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Region1_Territories', 'LocalID', CAST(@tid AS VARCHAR), '##TempTerritoryBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @desc;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeTerritoryReport;
END;
GO

EXEC proc_ProcessTerritoryStaging;