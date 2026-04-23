-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Packaging Material Costs and Shipping Insurance based on Freight.
CREATE OR ALTER VIEW vw_OrderFulfillmentCosts AS
SELECT 
    OrderID,
    -- Copied Columns
    CustomerID,
    EmployeeID,
    ShipCountry,
    Freight,
    -- Linear Transformation 1: Packaging Material Cost ($) f(x) = 0.5x + 3.00
    CAST((Freight * 0.5) + 3.00 AS DECIMAL(10,2)) AS PackagingCost,
    -- Linear Transformation 2: Shipping Insurance ($) f(x) = 0.05x + 10.00
    CAST((Freight * 0.05) + 10.00 AS DECIMAL(10,2)) AS ShippingInsurance
FROM Orders
WHERE Freight > 10.00; -- Filter: Only calculate for orders with baseline freight costs
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_OrderFulfillmentCosts;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_OrderFulfillmentCosts', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_USFulfillment', 'U') IS NOT NULL DROP TABLE Table_USFulfillment;
CREATE TABLE Table_USFulfillment (
    FulfillmentID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    EmployeeID INT,
    PackagingCost DECIMAL(10,2),
    ShippingInsurance DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_eid INT, @v_pkg DECIMAL(10,2), @v_ins DECIMAL(10,2), @nextFulfillID INT;

-- Filter: Only process fulfillment costs for orders shipped to the USA
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, EmployeeID, PackagingCost, ShippingInsurance 
    FROM vw_OrderFulfillmentCosts 
    WHERE ShipCountry = 'USA';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_eid, @v_pkg, @v_ins;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextFulfillID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_USFulfillment (FulfillmentID, OriginalOrderID, CustomerID, EmployeeID, PackagingCost, ShippingInsurance)
    VALUES (@nextFulfillID, @v_oid, @v_cid, @v_eid, @v_pkg, @v_ins);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_OrderFulfillmentCosts', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_USFulfillment', 'FulfillmentID', CAST(@nextFulfillID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_eid, @v_pkg, @v_ins;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeFulfillmentAudit AS
BEGIN
    IF OBJECT_ID('Final_FulfillmentAudit', 'U') IS NOT NULL DROP TABLE Final_FulfillmentAudit;
    CREATE TABLE Final_FulfillmentAudit (
        AuditID INT, 
        CustomerID NCHAR(5), 
        TotalPackagingCost DECIMAL(10,2), 
        TotalInsurance DECIMAL(10,2),
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_pkg DECIMAL(10,2), @t_ins DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, CustomerID, PackagingCost, ShippingInsurance 
        FROM ##TempFulfillmentBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_pkg, @t_ins;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_FulfillmentAudit (AuditID, CustomerID, TotalPackagingCost, TotalInsurance, AuditStatus)
        VALUES (@finalID, @t_cid, @t_pkg, @t_ins, 'Verified');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempFulfillmentBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_FulfillmentAudit', 'AuditID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_pkg, @t_ins;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageFulfillmentMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempFulfillmentBuffer') IS NOT NULL DROP TABLE ##TempFulfillmentBuffer;
    CREATE TABLE ##TempFulfillmentBuffer (
        TempID INT, 
        CustomerID NCHAR(5), 
        PackagingCost DECIMAL(10,2),
        ShippingInsurance DECIMAL(10,2)
    );

    DECLARE @tid INT, @cid NCHAR(5), @pkg DECIMAL(10,2), @ins DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT FulfillmentID, CustomerID, PackagingCost, ShippingInsurance 
        FROM Table_USFulfillment;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @pkg, @ins;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempFulfillmentBuffer VALUES (@newTempID, @cid, @pkg, @ins);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_USFulfillment', 'FulfillmentID', CAST(@tid AS VARCHAR), '##TempFulfillmentBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @pkg, @ins;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeFulfillmentAudit;
END;
GO

EXEC proc_StageFulfillmentMetrics;