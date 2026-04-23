-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring high-freight Orders for an international logistics audit.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_HighFreightOrders AS
SELECT 
    OrderID, 
    CustomerID, 
    ShipCountry,
    Freight
FROM Orders
WHERE Freight > 100.00; -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_HighFreightOrders;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_HighFreightOrders', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Brazil only).
IF OBJECT_ID('Table_Brazil_HighFreight', 'U') IS NOT NULL DROP TABLE Table_Brazil_HighFreight;
CREATE TABLE Table_Brazil_HighFreight (
    LocalOrderID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    Freight DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_freight DECIMAL(10,2), @nextLocalID INT;
-- Filter: Only process high-freight orders shipped specifically to Brazil
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, Freight 
    FROM vw_HighFreightOrders 
    WHERE ShipCountry = 'Brazil';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLocalID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Brazil_HighFreight (LocalOrderID, OriginalOrderID, CustomerID, Freight)
    VALUES (@nextLocalID, @v_oid, @v_cid, @v_freight);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_HighFreightOrders', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_Brazil_HighFreight', 'LocalOrderID', CAST(@nextLocalID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeOrderReport AS
BEGIN
    IF OBJECT_ID('Final_BrazilLogisticsAudit', 'U') IS NOT NULL DROP TABLE Final_BrazilLogisticsAudit;
    CREATE TABLE Final_BrazilLogisticsAudit (
        ReportID INT, 
        CustomerID NCHAR(5), 
        RecordedFreight DECIMAL(10,2),
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_freight DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CustomerID, Freight FROM ##TempOrderBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_freight;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_BrazilLogisticsAudit (ReportID, CustomerID, RecordedFreight, AuditStatus)
        VALUES (@finalID, @t_cid, @t_freight, 'Flagged for Review');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempOrderBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_BrazilLogisticsAudit', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_freight;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessOrderStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempOrderBuffer') IS NOT NULL DROP TABLE ##TempOrderBuffer;
    CREATE TABLE ##TempOrderBuffer (
        TempID INT, 
        CustomerID NCHAR(5),
        Freight DECIMAL(10,2)
    );

    DECLARE @tid INT, @cid NCHAR(5), @freight DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT LocalOrderID, CustomerID, Freight FROM Table_Brazil_HighFreight;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @freight;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempOrderBuffer VALUES (@newTempID, @cid, @freight);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Brazil_HighFreight', 'LocalOrderID', CAST(@tid AS VARCHAR), '##TempOrderBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @freight;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeOrderReport;
END;
GO

EXEC proc_ProcessOrderStaging;