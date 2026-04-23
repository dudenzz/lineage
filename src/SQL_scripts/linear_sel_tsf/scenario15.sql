-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Priority Shipping Upgrades and Weekend Delivery Surcharges.
CREATE OR ALTER VIEW vw_CanadianShippingSurcharges AS
SELECT 
    OrderID,
    -- Copied Columns
    CustomerID,
    ShipCity,
    Freight,
    -- Linear Transformation 1: Priority Upgrade Cost ($) f(x) = 0.30x + 20.00
    CAST((Freight * 0.30) + 20.00 AS DECIMAL(10,2)) AS PriorityCost,
    -- Linear Transformation 2: Weekend Delivery Surcharge ($) f(x) = 0.15x + 12.50
    CAST((Freight * 0.15) + 12.50 AS DECIMAL(10,2)) AS WeekendSurcharge
FROM Orders
WHERE ShipCountry = 'Canada'; -- Filter: Only calculate for shipments to Canada
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_CanadianShippingSurcharges;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_CanadianShippingSurcharges', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_CanadaExpedited', 'U') IS NOT NULL DROP TABLE Table_CanadaExpedited;
CREATE TABLE Table_CanadaExpedited (
    ExpeditedID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    Freight DECIMAL(10,2),
    PriorityCost DECIMAL(10,2),
    WeekendSurcharge DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_freight DECIMAL(10,2), @v_priority DECIMAL(10,2), @v_weekend DECIMAL(10,2), @nextExpeditedID INT;

-- Filter: Only process expedited options for heavier freight (e.g., Freight > 25.00)
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, Freight, PriorityCost, WeekendSurcharge 
    FROM vw_CanadianShippingSurcharges 
    WHERE Freight > 25.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight, @v_priority, @v_weekend;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextExpeditedID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_CanadaExpedited (ExpeditedID, OriginalOrderID, CustomerID, Freight, PriorityCost, WeekendSurcharge)
    VALUES (@nextExpeditedID, @v_oid, @v_cid, @v_freight, @v_priority, @v_weekend);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_CanadianShippingSurcharges', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_CanadaExpedited', 'ExpeditedID', CAST(@nextExpeditedID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight, @v_priority, @v_weekend;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeExpeditedReport AS
BEGIN
    IF OBJECT_ID('Final_ExpeditedShippingReport', 'U') IS NOT NULL DROP TABLE Final_ExpeditedShippingReport;
    CREATE TABLE Final_ExpeditedShippingReport (
        ReportID INT, 
        CustomerID NCHAR(5), 
        TotalPriorityCost DECIMAL(10,2), 
        TotalWeekendSurcharge DECIMAL(10,2),
        ApprovalStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_priority DECIMAL(10,2), @t_weekend DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, CustomerID, PriorityCost, WeekendSurcharge 
        FROM ##TempExpeditedBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_priority, @t_weekend;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ExpeditedShippingReport (ReportID, CustomerID, TotalPriorityCost, TotalWeekendSurcharge, ApprovalStatus)
        VALUES (@finalID, @t_cid, @t_priority, @t_weekend, 'Authorized');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempExpeditedBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ExpeditedShippingReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_priority, @t_weekend;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageExpeditedMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempExpeditedBuffer') IS NOT NULL DROP TABLE ##TempExpeditedBuffer;
    CREATE TABLE ##TempExpeditedBuffer (
        TempID INT, 
        CustomerID NCHAR(5), 
        PriorityCost DECIMAL(10,2),
        WeekendSurcharge DECIMAL(10,2)
    );

    DECLARE @tid INT, @cid NCHAR(5), @priority DECIMAL(10,2), @weekend DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT ExpeditedID, CustomerID, PriorityCost, WeekendSurcharge 
        FROM Table_CanadaExpedited;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @priority, @weekend;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempExpeditedBuffer VALUES (@newTempID, @cid, @priority, @weekend);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_CanadaExpedited', 'ExpeditedID', CAST(@tid AS VARCHAR), '##TempExpeditedBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @priority, @weekend;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeExpeditedReport;
END;
GO

EXEC proc_StageExpeditedMetrics;