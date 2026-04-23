-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring unshipped Orders for a pending logistics queue.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_PendingOrders AS
SELECT 
    OrderID, 
    CustomerID, 
    OrderDate,
    ShipVia
FROM Orders
WHERE ShippedDate IS NULL; -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_PendingOrders;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_PendingOrders', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to a specific shipper).
IF OBJECT_ID('Table_UnitedPackage_Queue', 'U') IS NOT NULL DROP TABLE Table_UnitedPackage_Queue;
CREATE TABLE Table_UnitedPackage_Queue (
    QueueID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    OrderDate DATETIME
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_odate DATETIME, @nextQueueID INT;
-- Filter: Only process pending orders assigned to Shipper 2 (United Package)
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, OrderDate 
    FROM vw_PendingOrders 
    WHERE ShipVia = 2;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_odate;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextQueueID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnitedPackage_Queue (QueueID, OriginalOrderID, CustomerID, OrderDate)
    VALUES (@nextQueueID, @v_oid, @v_cid, @v_odate);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_PendingOrders', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_UnitedPackage_Queue', 'QueueID', CAST(@nextQueueID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_odate;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeLogisticsReport AS
BEGIN
    IF OBJECT_ID('Final_LogisticsActionReport', 'U') IS NOT NULL DROP TABLE Final_LogisticsActionReport;
    CREATE TABLE Final_LogisticsActionReport (
        ReportID INT, 
        CustomerID NCHAR(5), 
        DateLogged DATETIME,
        DispatchStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_odate DATETIME, @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CustomerID, OrderDate FROM ##TempQueueBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_odate;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_LogisticsActionReport (ReportID, CustomerID, DateLogged, DispatchStatus)
        VALUES (@finalID, @t_cid, @t_odate, 'Awaiting Dispatch');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempQueueBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_LogisticsActionReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_odate;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessLogisticsStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempQueueBuffer') IS NOT NULL DROP TABLE ##TempQueueBuffer;
    CREATE TABLE ##TempQueueBuffer (
        TempID INT, 
        CustomerID NCHAR(5),
        OrderDate DATETIME
    );

    DECLARE @tid INT, @cid NCHAR(5), @odate DATETIME, @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT QueueID, CustomerID, OrderDate FROM Table_UnitedPackage_Queue;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @odate;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempQueueBuffer VALUES (@newTempID, @cid, @odate);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_UnitedPackage_Queue', 'QueueID', CAST(@tid AS VARCHAR), '##TempQueueBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @odate;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeLogisticsReport;
END;
GO

EXEC proc_ProcessLogisticsStaging;