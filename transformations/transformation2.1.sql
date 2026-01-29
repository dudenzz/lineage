    IF OBJECT_ID('dbo.DataLineage', 'U') IS NOT NULL DROP TABLE dbo.DataLineage; 
CREATE TABLE DataLineage (
    SourceName VARCHAR(255), SourcePKName VARCHAR(255), SourceID VARCHAR(255), 
    TargetName VARCHAR(255), TargetPKName VARCHAR(255), TargetID VARCHAR(255)
);

IF EXISTS (SELECT * FROM sys.sequences WHERE name = 'GlobalIDSequence')
    DROP SEQUENCE GlobalIDSequence;
CREATE SEQUENCE GlobalIDSequence START WITH 1 INCREMENT BY 1;

-- Transformation: Order Details -> vw_OrderTotals
DECLARE @orderID INT, @gross DECIMAL(18,2), @nextID INT;

DECLARE LineageCursor CURSOR FOR 
SELECT OrderID, SUM(UnitPrice * Quantity) FROM [Order Details] GROUP BY OrderID;

OPEN LineageCursor;
FETCH NEXT FROM LineageCursor INTO @orderID, @gross;
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Log lineage from original source to the logical view record
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('[Order Details]', 'OrderID', CAST(@orderID AS VARCHAR), 'vw_OrderTotals', 'OrderID', CAST(@orderID AS VARCHAR));
    FETCH NEXT FROM LineageCursor INTO @orderID, @gross;
END;
CLOSE LineageCursor; DEALLOCATE LineageCursor;
GO

IF OBJECT_ID('dbo.Table_OrderCategories', 'U') IS NOT NULL DROP TABLE dbo.Table_OrderCategories;
CREATE TABLE Table_OrderCategories (OrderID INT, GrossAmount DECIMAL(18,2), Category VARCHAR(10), LineageID INT);

DECLARE @orderID INT, @gross DECIMAL(18,2), @nextID INT;
DECLARE TableCursor CURSOR FOR SELECT OrderID, GrossAmount FROM vw_OrderTotals;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @orderID, @gross;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_OrderCategories (OrderID, GrossAmount, Category, LineageID)
    VALUES (@orderID, @gross, CASE WHEN @gross > 1000 THEN 'Tier1' ELSE 'Tier2' END, @nextID);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_OrderTotals', 'OrderID', CAST(@orderID AS VARCHAR), 'Table_OrderCategories', 'LineageID', CAST(@nextID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @orderID, @gross;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

CREATE OR ALTER PROCEDURE proc_FinalizeHighValueReport AS
BEGIN
    DECLARE @oid INT, @amt DECIMAL(18,2), @lid INT;
    DECLARE FinalCursor CURSOR FOR SELECT OrderID, GrossAmount, LineageID FROM ##TempHighValue;

    IF OBJECT_ID('dbo.Final_HighValueReport', 'U') IS NOT NULL DROP TABLE dbo.Final_HighValueReport;
    CREATE TABLE Final_HighValueReport (OrderID INT, FinalAmount DECIMAL(18,2), ParentLineageID INT);

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @oid, @amt, @lid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        INSERT INTO Final_HighValueReport (OrderID, FinalAmount, ParentLineageID)
        VALUES (@oid, @amt * 0.9, @lid);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempHighValue', 'LineageID', CAST(@lid AS VARCHAR), 'Final_HighValueReport', 'OrderID', CAST(@oid AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @oid, @amt, @lid;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessHighValueOrders AS
BEGIN
    IF OBJECT_ID('tempdb..##TempHighValue') IS NOT NULL DROP TABLE ##TempHighValue;
    CREATE TABLE ##TempHighValue (OrderID INT, GrossAmount DECIMAL(18,2), LineageID INT);

    DECLARE @oid INT, @amt DECIMAL(18,2), @lid INT, @newID INT;
    DECLARE ProcCursor CURSOR FOR 
    SELECT OrderID, GrossAmount, LineageID FROM Table_OrderCategories WHERE Category = 'Tier1';

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @oid, @amt, @lid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempHighValue VALUES (@oid, @amt, @newID);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_OrderCategories', 'LineageID', CAST(@lid AS VARCHAR), '##TempHighValue', 'LineageID', CAST(@newID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @oid, @amt, @lid;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeHighValueReport;
END;
GO

-- Execution
EXEC proc_ProcessHighValueOrders;