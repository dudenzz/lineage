-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring late shipments for a carrier Service Level Agreement (SLA) compliance audit.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_LateShipments AS
SELECT 
    OrderID, 
    CustomerID, 
    RequiredDate,
    ShippedDate,
    ShipVia
FROM Orders
WHERE ShippedDate > RequiredDate; -- Selection applied here (Late Deliveries)
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_LateShipments;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_LateShipments', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Federal Shipping only).
IF OBJECT_ID('Table_FederalShipping_Violations', 'U') IS NOT NULL DROP TABLE Table_FederalShipping_Violations;
CREATE TABLE Table_FederalShipping_Violations (
    ViolationID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    DaysLate INT -- We will just store a copied placeholder if no transformations allowed, wait, strict rule: NO transformations. Let's stick to dates.
);
-- Correction: Dropping DaysLate to strictly avoid DATEDIFF transformation.
DROP TABLE Table_FederalShipping_Violations;
CREATE TABLE Table_FederalShipping_Violations (
    ViolationID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    RequiredDate DATETIME,
    ShippedDate DATETIME
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_req DATETIME, @v_ship DATETIME, @nextViolationID INT;
-- Filter: Only process late shipments handled by Shipper 3 (Federal Shipping)
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, RequiredDate, ShippedDate 
    FROM vw_LateShipments 
    WHERE ShipVia = 3;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_req, @v_ship;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextViolationID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_FederalShipping_Violations (ViolationID, OriginalOrderID, CustomerID, RequiredDate, ShippedDate)
    VALUES (@nextViolationID, @v_oid, @v_cid, @v_req, @v_ship);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_LateShipments', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_FederalShipping_Violations', 'ViolationID', CAST(@nextViolationID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_req, @v_ship;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeSLAReport AS
BEGIN
    IF OBJECT_ID('Final_CarrierSLAPenalty', 'U') IS NOT NULL DROP TABLE Final_CarrierSLAPenalty;
    CREATE TABLE Final_CarrierSLAPenalty (
        ReportID INT, 
        AffectedCustomer NCHAR(5), 
        TargetDelivery DATETIME,
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_req DATETIME, @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CustomerID, RequiredDate FROM ##TempSLABuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_req;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_CarrierSLAPenalty (ReportID, AffectedCustomer, TargetDelivery, AuditStatus)
        VALUES (@finalID, @t_cid, @t_req, 'Penalty Logged');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempSLABuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_CarrierSLAPenalty', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_req;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessSLAStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempSLABuffer') IS NOT NULL DROP TABLE ##TempSLABuffer;
    CREATE TABLE ##TempSLABuffer (
        TempID INT, 
        CustomerID NCHAR(5),
        RequiredDate DATETIME
    );

    DECLARE @tid INT, @cid NCHAR(5), @req DATETIME, @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT ViolationID, CustomerID, RequiredDate FROM Table_FederalShipping_Violations;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @req;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempSLABuffer VALUES (@newTempID, @cid, @req);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_FederalShipping_Violations', 'ViolationID', CAST(@tid AS VARCHAR), '##TempSLABuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @req;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeSLAReport;
END;
GO

EXEC proc_ProcessSLAStaging;