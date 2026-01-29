-- Section 1: Create a View based on a simple SELECT
-- Tests: Column pass-through lineage
CREATE OR ALTER VIEW vw_ProductsAllStatus AS
SELECT 
    ProductID, 
    ProductName, 
    Discontinued
FROM Products;
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_ProductsAllStatus;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ProductsAllStatus', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table with a filter (WHERE Discontinued = 1)
-- Tests: Selective row lineage
IF OBJECT_ID('Table_DiscontinuedStaging', 'U') IS NOT NULL DROP TABLE Table_DiscontinuedStaging;
CREATE TABLE Table_DiscontinuedStaging (
    StageID INT, 
    OriginalProductID INT, 
    ProductLabel NVARCHAR(40)
);

DECLARE @v_pid INT, @v_name NVARCHAR(40), @nextStageID INT;
DECLARE TableCursor CURSOR FOR SELECT ProductID, ProductName FROM vw_ProductsAllStatus WHERE Discontinued = 1;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextStageID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_DiscontinuedStaging (StageID, OriginalProductID, ProductLabel)
    VALUES (@nextStageID, @v_pid, @v_name);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ProductsAllStatus', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_DiscontinuedStaging', 'StageID', CAST(@nextStageID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_name;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeDiscontinuedReport AS
BEGIN
    IF OBJECT_ID('Final_DiscontinuedReport', 'U') IS NOT NULL DROP TABLE Final_DiscontinuedReport;
    CREATE TABLE Final_DiscontinuedReport (ReportID INT, Label NVARCHAR(40), ArchiveDate DATETIME);

    DECLARE @t_id INT, @t_label NVARCHAR(40), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ProductLabel FROM ##TempDiscontinuedBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_label;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_DiscontinuedReport (ReportID, Label, ArchiveDate)
        VALUES (@finalID, @t_label, GETDATE());

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempDiscontinuedBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_DiscontinuedReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_label;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessDiscontinuedStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempDiscontinuedBuffer') IS NOT NULL DROP TABLE ##TempDiscontinuedBuffer;
    CREATE TABLE ##TempDiscontinuedBuffer (TempID INT, ProductLabel NVARCHAR(40));

    DECLARE @sid INT, @label NVARCHAR(40), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT StageID, ProductLabel FROM Table_DiscontinuedStaging;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @sid, @label;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempDiscontinuedBuffer VALUES (@newTempID, @label);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_DiscontinuedStaging', 'StageID', CAST(@sid AS VARCHAR), '##TempDiscontinuedBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @sid, @label;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeDiscontinuedReport;
END;
GO

EXEC proc_ProcessDiscontinuedStaging;