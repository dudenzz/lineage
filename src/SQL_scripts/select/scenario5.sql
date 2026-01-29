-- Section 1: Create a View based on a simple SELECT
-- Tests: Selection of specific columns from a high-volume transactional table.
CREATE OR ALTER VIEW vw_OrderFreightDetails AS
SELECT 
    OrderID, 
    OrderDate, 
    Freight, 
    ShipName
FROM Orders;
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_OrderFreightDetails;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_OrderFreightDetails', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table with a numeric filter
-- Tests: Lineage through a threshold filter (Freight > 100.00)
IF OBJECT_ID('Table_HighFreightStaging', 'U') IS NOT NULL DROP TABLE Table_HighFreightStaging;
CREATE TABLE Table_HighFreightStaging (
    StageID INT, 
    SourceOrderID INT, 
    ShippingLabel NVARCHAR(40)
);

DECLARE @v_oid INT, @v_ship NVARCHAR(40), @nextStageID INT;
DECLARE TableCursor CURSOR FOR SELECT OrderID, ShipName FROM vw_OrderFreightDetails WHERE Freight > 100.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_ship;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextStageID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_HighFreightStaging (StageID, SourceOrderID, ShippingLabel)
    VALUES (@nextStageID, @v_oid, @v_ship);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_OrderFreightDetails', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_HighFreightStaging', 'StageID', CAST(@nextStageID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_ship;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures for Final Priority Audit
CREATE OR ALTER PROCEDURE proc_FinalizePriorityReport AS
BEGIN
    IF OBJECT_ID('Final_PriorityShipmentReport', 'U') IS NOT NULL DROP TABLE Final_PriorityShipmentReport;
    CREATE TABLE Final_PriorityShipmentReport (ReportID INT, FinalShipName NVARCHAR(40), PriorityStatus VARCHAR(10));

    DECLARE @t_id INT, @t_ship NVARCHAR(40), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ShippingLabel FROM ##TempFreightBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_ship;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_PriorityShipmentReport (ReportID, FinalShipName, PriorityStatus)
        VALUES (@finalID, @t_ship, 'URGENT');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempFreightBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_PriorityShipmentReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_ship;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StagingHighFreightOrders AS
BEGIN
    IF OBJECT_ID('tempdb..##TempFreightBuffer') IS NOT NULL DROP TABLE ##TempFreightBuffer;
    CREATE TABLE ##TempFreightBuffer (TempID INT, ShippingLabel NVARCHAR(40));

    DECLARE @sid INT, @sLabel NVARCHAR(40), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT StageID, ShippingLabel FROM Table_HighFreightStaging;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @sid, @sLabel;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempFreightBuffer VALUES (@newTempID, @sLabel);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_HighFreightStaging', 'StageID', CAST(@sid AS VARCHAR), '##TempFreightBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @sid, @sLabel;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizePriorityReport;
END;
GO

EXEC proc_StagingHighFreightOrders;