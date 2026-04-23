-- Section: Create a Physical Table combining multiple date columns using UNION with Strict Projection
-- Scenario: Building a Universal Chronological Event Ledger to plot all enterprise activities on a single timeline.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no data formatting or date math.
IF OBJECT_ID('Table_UniversalEventLedger', 'U') IS NOT NULL DROP TABLE Table_UniversalEventLedger;
CREATE TABLE Table_UniversalEventLedger (
    EventID INT,
    SourceDomain VARCHAR(20),
    OriginalRecordID NVARCHAR(15),
    EventType VARCHAR(30),
    EventDate DATETIME
);
GO

DECLARE @v_SourceDomain VARCHAR(20),
        @v_OriginalRecordID NVARCHAR(15),
        @v_EventType VARCHAR(30),
        @v_EventDate DATETIME,
        @nextEventID INT;

-- Cursor using UNION ALL for strict projection across multiple tables and columns.
-- We are extracting unedited date columns and assigning them a literal string 'EventType'.
-- CAST is used strictly for structural data-type alignment across the UNION.
DECLARE EventLedgerCursor CURSOR FOR 
    SELECT 
        'Sales' AS SourceDomain, 
        CAST(OrderID AS NVARCHAR(15)) AS OriginalRecordID, 
        'Order Placed' AS EventType, 
        OrderDate AS EventDate 
    FROM Orders
    UNION ALL
    SELECT 
        'Logistics' AS SourceDomain, 
        CAST(OrderID AS NVARCHAR(15)) AS OriginalRecordID, 
        'Order Shipped' AS EventType, 
        ShippedDate AS EventDate 
    FROM Orders
    UNION ALL
    SELECT 
        'HumanResources' AS SourceDomain, 
        CAST(EmployeeID AS NVARCHAR(15)) AS OriginalRecordID, 
        'Employee Hired' AS EventType, 
        HireDate AS EventDate 
    FROM Employees
    UNION ALL
    SELECT 
        'HumanResources' AS SourceDomain, 
        CAST(EmployeeID AS NVARCHAR(15)) AS OriginalRecordID, 
        'Employee Born' AS EventType, 
        BirthDate AS EventDate 
    FROM Employees;

OPEN EventLedgerCursor;
FETCH NEXT FROM EventLedgerCursor INTO @v_SourceDomain, @v_OriginalRecordID, @v_EventType, @v_EventDate;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextEventID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected event record
    -- Note: NULL dates (e.g., unshipped orders) are inserted exactly as they are, preserving raw data integrity
    INSERT INTO Table_UniversalEventLedger (EventID, SourceDomain, OriginalRecordID, EventType, EventDate)
    VALUES (@nextEventID, @v_SourceDomain, @v_OriginalRecordID, @v_EventType, @v_EventDate);

    -- Conditionally log Row-Level Lineage based on which table the date originated from
    IF @v_SourceDomain = 'Sales' OR @v_SourceDomain = 'Logistics'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', @v_OriginalRecordID, 'Table_UniversalEventLedger', 'EventID', CAST(@nextEventID AS VARCHAR));
    END
    ELSE IF @v_SourceDomain = 'HumanResources'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_OriginalRecordID, 'Table_UniversalEventLedger', 'EventID', CAST(@nextEventID AS VARCHAR));
    END
    
    FETCH NEXT FROM EventLedgerCursor INTO @v_SourceDomain, @v_OriginalRecordID, @v_EventType, @v_EventDate;
END;

CLOSE EventLedgerCursor; 
DEALLOCATE EventLedgerCursor;
GO