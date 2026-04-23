-- Section 1: Create a View based on a simple SELECT
-- Scenario: Mirroring Category data for an inventory categorization sync.
-- Rule: Direct data copying only, no transformations.
CREATE OR ALTER VIEW vw_CategoryMirror AS
SELECT 
    CategoryID, 
    CategoryName, 
    Description
FROM Categories;
GO

-- Log Row-Level Lineage for View
DECLARE @catid INT;
DECLARE ViewCursor CURSOR FOR SELECT CategoryID FROM vw_CategoryMirror;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @catid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@catid AS VARCHAR), 'vw_CategoryMirror', 'CategoryID', CAST(@catid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @catid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
-- Tests: Lineage preservation during a filter (Specific Categories only).
IF OBJECT_ID('Table_Food_Categories', 'U') IS NOT NULL DROP TABLE Table_Food_Categories;
CREATE TABLE Table_Food_Categories (
    InternalID INT, 
    OriginalCategoryID INT, 
    CategoryName NVARCHAR(15)
);
GO

DECLARE @v_catid INT, @v_name NVARCHAR(15), @nextInternalID INT;
-- Filter: Only process a subset of categories (e.g., Categories 1, 3, and 7)
DECLARE TableCursor CURSOR FOR 
    SELECT CategoryID, CategoryName 
    FROM vw_CategoryMirror 
    WHERE CategoryID IN (1, 3, 7);

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_catid, @v_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextInternalID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Food_Categories (InternalID, OriginalCategoryID, CategoryName)
    VALUES (@nextInternalID, @v_catid, @v_name);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_CategoryMirror', 'CategoryID', CAST(@v_catid AS VARCHAR), 'Table_Food_Categories', 'InternalID', CAST(@nextInternalID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_catid, @v_name;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeCategoryReport AS
BEGIN
    IF OBJECT_ID('Final_InventoryCategoryReport', 'U') IS NOT NULL DROP TABLE Final_InventoryCategoryReport;
    CREATE TABLE Final_InventoryCategoryReport (ReportID INT, Label NVARCHAR(15), SyncDate DATETIME);

    DECLARE @t_id INT, @t_label NVARCHAR(15), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CategoryName FROM ##TempCategoryBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_label;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_InventoryCategoryReport (ReportID, Label, SyncDate)
        VALUES (@finalID, @t_label, GETDATE());

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempCategoryBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_InventoryCategoryReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_label;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessCategoryStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempCategoryBuffer') IS NOT NULL DROP TABLE ##TempCategoryBuffer;
    CREATE TABLE ##TempCategoryBuffer (TempID INT, CategoryName NVARCHAR(15));

    DECLARE @tid INT, @name NVARCHAR(15), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT InternalID, CategoryName FROM Table_Food_Categories;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempCategoryBuffer VALUES (@newTempID, @name);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Food_Categories', 'InternalID', CAST(@tid AS VARCHAR), '##TempCategoryBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @name;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeCategoryReport;
END;
GO

EXEC proc_ProcessCategoryStaging;