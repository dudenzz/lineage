-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring Products that have hit their reorder threshold for an active inventory management queue.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_ReorderCandidates AS
SELECT 
    ProductID, 
    ProductName, 
    SupplierID,
    CategoryID
FROM Products
WHERE UnitsInStock <= ReorderLevel AND Discontinued = 0; -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_ReorderCandidates;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ReorderCandidates', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Dairy Products only).
IF OBJECT_ID('Table_Dairy_Reorder_Queue', 'U') IS NOT NULL DROP TABLE Table_Dairy_Reorder_Queue;
CREATE TABLE Table_Dairy_Reorder_Queue (
    QueueID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    SupplierID INT
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_sid INT, @nextQueueID INT;
-- Filter: Only process reorder candidates from Category 4 (Dairy Products)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, SupplierID 
    FROM vw_ReorderCandidates 
    WHERE CategoryID = 4;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_sid;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextQueueID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Dairy_Reorder_Queue (QueueID, OriginalProductID, ProductName, SupplierID)
    VALUES (@nextQueueID, @v_pid, @v_pname, @v_sid);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ReorderCandidates', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Dairy_Reorder_Queue', 'QueueID', CAST(@nextQueueID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_sid;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeReorderReport AS
BEGIN
    IF OBJECT_ID('Final_DairyReplenishmentAction', 'U') IS NOT NULL DROP TABLE Final_DairyReplenishmentAction;
    CREATE TABLE Final_DairyReplenishmentAction (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        VendorID INT,
        QueueStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_sid INT, @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ProductName, SupplierID FROM ##TempReorderBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_sid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_DairyReplenishmentAction (ReportID, ItemName, VendorID, QueueStatus)
        VALUES (@finalID, @t_pname, @t_sid, 'PO Generated');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempReorderBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_DairyReplenishmentAction', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_sid;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessReorderStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempReorderBuffer') IS NOT NULL DROP TABLE ##TempReorderBuffer;
    CREATE TABLE ##TempReorderBuffer (
        TempID INT, 
        ProductName NVARCHAR(40),
        SupplierID INT
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @sid INT, @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT QueueID, ProductName, SupplierID FROM Table_Dairy_Reorder_Queue;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @sid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempReorderBuffer VALUES (@newTempID, @pname, @sid);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Dairy_Reorder_Queue', 'QueueID', CAST(@tid AS VARCHAR), '##TempReorderBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @sid;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeReorderReport;
END;
GO

EXEC proc_ProcessReorderStaging;