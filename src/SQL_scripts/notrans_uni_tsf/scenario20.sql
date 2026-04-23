-- Section: Create a Physical Table combining native timestamp columns using UNION with Strict Projection
-- Scenario: Compiling a System-Wide Temporal Audit Ledger for compliance tracking of all historical timestamps.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection. No Joins, zero data manipulation (native INT and DATETIME alignment).
IF OBJECT_ID('Table_TemporalAuditLedger', 'U') IS NOT NULL DROP TABLE Table_TemporalAuditLedger;
CREATE TABLE Table_TemporalAuditLedger (
    AuditLineID INT,
    TemporalContext VARCHAR(30),
    SystemRecordID INT,         -- Natively matches both OrderID and EmployeeID
    LoggedTimestamp DATETIME    -- Natively matches OrderDate, ShippedDate, HireDate, and BirthDate
);
GO

DECLARE @v_TemporalContext VARCHAR(30),
        @v_SystemRecordID INT,
        @v_LoggedTimestamp DATETIME,
        @nextAuditLineID INT;

-- Cursor using UNION ALL for strict projection across multiple operational domains.
-- Because OrderID/EmployeeID are natively INT, and the dates are natively DATETIME, 
-- this query extracts the exact chronological data with absolutely zero data type casting.
DECLARE TemporalLedgerCursor CURSOR FOR 
    SELECT 
        'OrderPlacement' AS TemporalContext, 
        OrderID AS SystemRecordID, 
        OrderDate AS LoggedTimestamp 
    FROM Orders
    UNION ALL
    SELECT 
        'OrderFulfillment' AS TemporalContext, 
        OrderID AS SystemRecordID, 
        ShippedDate AS LoggedTimestamp 
    FROM Orders
    UNION ALL
    SELECT 
        'EmployeeOnboarding' AS TemporalContext, 
        EmployeeID AS SystemRecordID, 
        HireDate AS LoggedTimestamp 
    FROM Employees
    UNION ALL
    SELECT 
        'EmployeeBirth' AS TemporalContext, 
        EmployeeID AS SystemRecordID, 
        BirthDate AS LoggedTimestamp 
    FROM Employees;

OPEN TemporalLedgerCursor;
FETCH NEXT FROM TemporalLedgerCursor INTO @v_TemporalContext, @v_SystemRecordID, @v_LoggedTimestamp;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextAuditLineID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected temporal record
    -- Note: NULL dates (like an unshipped order) map cleanly without any modification
    INSERT INTO Table_TemporalAuditLedger (AuditLineID, TemporalContext, SystemRecordID, LoggedTimestamp)
    VALUES (@nextAuditLineID, @v_TemporalContext, @v_SystemRecordID, @v_LoggedTimestamp);

    -- Conditionally log Row-Level Lineage based on which table the timestamp originated from
    -- (CAST is used here exclusively to meet the standardized DataLineage tracking table requirements, 
    -- keeping the actual chronological payload passing into the new table completely unmanipulated)
    IF @v_TemporalContext = 'OrderPlacement' OR @v_TemporalContext = 'OrderFulfillment'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', CAST(@v_SystemRecordID AS VARCHAR), 'Table_TemporalAuditLedger', 'AuditLineID', CAST(@nextAuditLineID AS VARCHAR));
    END
    ELSE IF @v_TemporalContext = 'EmployeeOnboarding' OR @v_TemporalContext = 'EmployeeBirth'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', CAST(@v_SystemRecordID AS VARCHAR), 'Table_TemporalAuditLedger', 'AuditLineID', CAST(@nextAuditLineID AS VARCHAR));
    END
    
    FETCH NEXT FROM TemporalLedgerCursor INTO @v_TemporalContext, @v_SystemRecordID, @v_LoggedTimestamp;
END;

CLOSE TemporalLedgerCursor; 
DEALLOCATE TemporalLedgerCursor;
GO