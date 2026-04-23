-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Delivery Driver Compensation and Route Mileage based on Order Freight.
CREATE OR ALTER VIEW vw_DriverCompensation AS
SELECT 
    OrderID,
    -- Copied Columns
    CustomerID,
    ShipCity,
    Freight,
    -- Linear Transformation 1: Driver Base Pay ($) f(x) = 0.30x + 15.00
    CAST((Freight * 0.30) + 15.00 AS DECIMAL(10,2)) AS DriverBasePay,
    -- Linear Transformation 2: Estimated Route Mileage f(x) = 1.2x + 5.0
    CAST((Freight * 1.2) + 5.0 AS DECIMAL(10,2)) AS EstimatedMileage
FROM Orders
WHERE ShipVia = 1; -- Filter: Only orders shipped via Speedy Express (ShipVia = 1)
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_DriverCompensation;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_DriverCompensation', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_DispatchLog', 'U') IS NOT NULL DROP TABLE Table_DispatchLog;
CREATE TABLE Table_DispatchLog (
    DispatchID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    ShipCity NVARCHAR(15),
    DriverBasePay DECIMAL(10,2),
    EstimatedMileage DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_city NVARCHAR(15), @v_pay DECIMAL(10,2), @v_miles DECIMAL(10,2), @nextDispatchID INT;

-- Filter: Only process long-distance deliveries (Estimated Mileage > 20.0 miles)
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, ShipCity, DriverBasePay, EstimatedMileage 
    FROM vw_DriverCompensation 
    WHERE EstimatedMileage > 20.0;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_city, @v_pay, @v_miles;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextDispatchID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_DispatchLog (DispatchID, OriginalOrderID, CustomerID, ShipCity, DriverBasePay, EstimatedMileage)
    VALUES (@nextDispatchID, @v_oid, @v_cid, @v_city, @v_pay, @v_miles);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_DriverCompensation', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_DispatchLog', 'DispatchID', CAST(@nextDispatchID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_city, @v_pay, @v_miles;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizePayrollReport AS
BEGIN
    IF OBJECT_ID('Final_DriverPayroll', 'U') IS NOT NULL DROP TABLE Final_DriverPayroll;
    CREATE TABLE Final_DriverPayroll (
        ReportID INT, 
        TargetCity NVARCHAR(15), 
        TotalPayout DECIMAL(10,2), 
        LoggedMileage DECIMAL(10,2),
        PaymentStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_city NVARCHAR(15), @t_pay DECIMAL(10,2), @t_miles DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ShipCity, DriverBasePay, EstimatedMileage 
        FROM ##TempDispatchBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_city, @t_pay, @t_miles;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_DriverPayroll (ReportID, TargetCity, TotalPayout, LoggedMileage, PaymentStatus)
        VALUES (@finalID, @t_city, @t_pay, @t_miles, 'Cleared');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempDispatchBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_DriverPayroll', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_city, @t_pay, @t_miles;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageDispatchMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempDispatchBuffer') IS NOT NULL DROP TABLE ##TempDispatchBuffer;
    CREATE TABLE ##TempDispatchBuffer (
        TempID INT, 
        ShipCity NVARCHAR(15), 
        DriverBasePay DECIMAL(10,2),
        EstimatedMileage DECIMAL(10,2)
    );

    DECLARE @tid INT, @city NVARCHAR(15), @pay DECIMAL(10,2), @miles DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT DispatchID, ShipCity, DriverBasePay, EstimatedMileage 
        FROM Table_DispatchLog;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @city, @pay, @miles;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempDispatchBuffer VALUES (@newTempID, @city, @pay, @miles);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_DispatchLog', 'DispatchID', CAST(@tid AS VARCHAR), '##TempDispatchBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @city, @pay, @miles;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizePayrollReport;
END;
GO

EXEC proc_StageDispatchMetrics;