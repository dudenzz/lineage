-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring historical Orders from 1997 for an annual corporate data archiving initiative.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_HistoricalOrders_1997 AS
SELECT 
    OrderID, 
    CustomerID, 
    EmployeeID,
    OrderDate,
    ShipCountry
FROM Orders
WHERE OrderDate >= '1997-01-01' AND OrderDate < '1998-01-01'; -- Selection applied here (Year filter)
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_HistoricalOrders_1997;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_HistoricalOrders_1997', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to France only).
IF OBJECT_ID('Table_France_1997_Archive', 'U') IS NOT NULL DROP TABLE Table_France_1997_Archive;
CREATE TABLE Table_France_1997_Archive (
    ArchiveID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    OrderDate DATETIME
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_odate DATETIME, @nextArchiveID INT;
-- Filter: Only process historical 1997 orders shipped to France
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, OrderDate 
    FROM vw_HistoricalOrders_1997 
    WHERE ShipCountry = 'France';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_odate;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextArchiveID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_France_1997_Archive (ArchiveID, OriginalOrderID, CustomerID, OrderDate)
    VALUES (@nextArchiveID, @v_oid, @v_cid, @v_odate);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_HistoricalOrders_1997', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_France_1997_Archive', 'ArchiveID', CAST(@nextArchiveID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_odate;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeArchiveReport AS
BEGIN
    IF OBJECT_ID('Final_FrenchHistoricalArchive', 'U') IS NOT NULL DROP TABLE Final_FrenchHistoricalArchive;
    CREATE TABLE Final_FrenchHistoricalArchive (
        ReportID INT, 
        CustomerID NCHAR(5), 
        TransactionDate DATETIME,
        ArchiveStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_odate DATETIME, @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CustomerID, OrderDate FROM ##TempArchiveBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_odate;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_FrenchHistoricalArchive (ReportID, CustomerID, TransactionDate, ArchiveStatus)
        VALUES (@finalID, @t_cid, @t_odate, 'Stored Offline');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempArchiveBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_FrenchHistoricalArchive', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_odate;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessArchiveStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempArchiveBuffer') IS NOT NULL DROP TABLE ##TempArchiveBuffer;
    CREATE TABLE ##TempArchiveBuffer (
        TempID INT, 
        CustomerID NCHAR(5),
        OrderDate DATETIME
    );

    DECLARE @tid INT, @cid NCHAR(5), @odate DATETIME, @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT ArchiveID, CustomerID, OrderDate FROM Table_France_1997_Archive;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @odate;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempArchiveBuffer VALUES (@newTempID, @cid, @odate);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_France_1997_Archive', 'ArchiveID', CAST(@tid AS VARCHAR), '##TempArchiveBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @odate;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeArchiveReport;
END;
GO

EXEC proc_ProcessArchiveStaging;