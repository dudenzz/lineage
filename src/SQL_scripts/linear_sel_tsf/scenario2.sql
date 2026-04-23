-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating UK Logistics metrics (Fuel Surcharge and Loyalty Points) from Orders.
CREATE OR ALTER VIEW vw_UKOrderMetrics AS
SELECT 
    OrderID,
    -- Copied Columns
    CustomerID,
    OrderDate,
    -- Linear Transformation 1: Fuel Surcharge f(x) = 0.15x + 2.50
    CAST((Freight * 0.15) + 2.50 AS DECIMAL(10,2)) AS FuelSurcharge,
    -- Linear Transformation 2: Loyalty Points f(x) = 2.0x + 10.0
    CAST((Freight * 2.0) + 10.0 AS INT) AS LoyaltyPoints
FROM Orders
WHERE ShipCountry = 'UK'; -- Filter: Only UK Orders
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_UKOrderMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_UKOrderMetrics', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_UKLogistics', 'U') IS NOT NULL DROP TABLE Table_UKLogistics;
CREATE TABLE Table_UKLogistics (
    UKLogisticsID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    OrderDate DATETIME,
    FuelSurcharge DECIMAL(10,2),
    LoyaltyPoints INT
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_odate DATETIME, @v_fuel DECIMAL(10,2), @v_pts INT, @nextLogisticsID INT;

-- Filter: Only process orders where the calculated Fuel Surcharge is greater than $5.00
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, OrderDate, FuelSurcharge, LoyaltyPoints 
    FROM vw_UKOrderMetrics 
    WHERE FuelSurcharge > 5.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_odate, @v_fuel, @v_pts;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLogisticsID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UKLogistics (UKLogisticsID, OriginalOrderID, CustomerID, OrderDate, FuelSurcharge, LoyaltyPoints)
    VALUES (@nextLogisticsID, @v_oid, @v_cid, @v_odate, @v_fuel, @v_pts);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_UKOrderMetrics', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_UKLogistics', 'UKLogisticsID', CAST(@nextLogisticsID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_odate, @v_fuel, @v_pts;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeUKMetrics AS
BEGIN
    IF OBJECT_ID('Final_UKMetricsReport', 'U') IS NOT NULL DROP TABLE Final_UKMetricsReport;
    CREATE TABLE Final_UKMetricsReport (
        ReportID INT, 
        CustomerID NCHAR(5), 
        ProcessingDate DATETIME,
        FinalFuelSurcharge DECIMAL(10,2), 
        FinalLoyaltyPoints INT,
        AuditStatus VARCHAR(15)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_odate DATETIME, @t_fuel DECIMAL(10,2), @t_pts INT, @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, CustomerID, OrderDate, FuelSurcharge, LoyaltyPoints 
        FROM ##TempUKLogisticsBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_odate, @t_fuel, @t_pts;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_UKMetricsReport (ReportID, CustomerID, ProcessingDate, FinalFuelSurcharge, FinalLoyaltyPoints, AuditStatus)
        VALUES (@finalID, @t_cid, @t_odate, @t_fuel, @t_pts, 'Verified');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempUKLogisticsBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_UKMetricsReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_odate, @t_fuel, @t_pts;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageUKMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempUKLogisticsBuffer') IS NOT NULL DROP TABLE ##TempUKLogisticsBuffer;
    CREATE TABLE ##TempUKLogisticsBuffer (
        TempID INT, 
        CustomerID NCHAR(5), 
        OrderDate DATETIME,
        FuelSurcharge DECIMAL(10,2),
        LoyaltyPoints INT
    );

    DECLARE @tid INT, @cid NCHAR(5), @odate DATETIME, @fuel DECIMAL(10,2), @pts INT, @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT UKLogisticsID, CustomerID, OrderDate, FuelSurcharge, LoyaltyPoints 
        FROM Table_UKLogistics;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @odate, @fuel, @pts;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempUKLogisticsBuffer VALUES (@newTempID, @cid, @odate, @fuel, @pts);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_UKLogistics', 'UKLogisticsID', CAST(@tid AS VARCHAR), '##TempUKLogisticsBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @odate, @fuel, @pts;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeUKMetrics;
END;
GO

EXEC proc_StageUKMetrics;