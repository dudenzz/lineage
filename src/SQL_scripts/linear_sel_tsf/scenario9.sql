-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Packaging Weights and Handling Surcharges for Wholesale Fulfillment.
CREATE OR ALTER VIEW vw_PackagingMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitsInStock,
    -- Linear Transformation 1: Estimated Packaging Weight (lbs) f(x) = 0.05x + 1.50
    CAST((UnitsInStock * 0.05) + 1.50 AS DECIMAL(10,2)) AS EstimatedWeightLbs,
    -- Linear Transformation 2: Handling Surcharge ($) f(x) = 0.10x + 2.00
    CAST((UnitPrice * 0.10) + 2.00 AS DECIMAL(10,2)) AS HandlingSurcharge
FROM Products
WHERE UnitsInStock > 20; -- Filter: Only calculate for bulk items with significant stock
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_PackagingMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_PackagingMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_PackagingLogistics', 'U') IS NOT NULL DROP TABLE Table_PackagingLogistics;
CREATE TABLE Table_PackagingLogistics (
    LogisticsID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitsInStock SMALLINT,
    EstimatedWeightLbs DECIMAL(10,2),
    HandlingSurcharge DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_uis SMALLINT, @v_weight DECIMAL(10,2), @v_surcharge DECIMAL(10,2), @nextLogisticsID INT;

-- Filter: Only process logistics for a specific category (e.g., Category 3 - Confections)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitsInStock, EstimatedWeightLbs, HandlingSurcharge 
    FROM vw_PackagingMetrics 
    WHERE CategoryID = 3;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_weight, @v_surcharge;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLogisticsID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_PackagingLogistics (LogisticsID, OriginalProductID, ProductName, UnitsInStock, EstimatedWeightLbs, HandlingSurcharge)
    VALUES (@nextLogisticsID, @v_pid, @v_pname, @v_uis, @v_weight, @v_surcharge);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_PackagingMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_PackagingLogistics', 'LogisticsID', CAST(@nextLogisticsID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_weight, @v_surcharge;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizePackagingReport AS
BEGIN
    IF OBJECT_ID('Final_FulfillmentReport', 'U') IS NOT NULL DROP TABLE Final_FulfillmentReport;
    CREATE TABLE Final_FulfillmentReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalWeight DECIMAL(10,2), 
        AppliedSurcharge DECIMAL(10,2),
        FulfillmentStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_weight DECIMAL(10,2), @t_surcharge DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, EstimatedWeightLbs, HandlingSurcharge 
        FROM ##TempPackagingBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_weight, @t_surcharge;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_FulfillmentReport (ReportID, ProductName, TotalWeight, AppliedSurcharge, FulfillmentStatus)
        VALUES (@finalID, @t_pname, @t_weight, @t_surcharge, 'Ready to Ship');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempPackagingBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_FulfillmentReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_weight, @t_surcharge;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StagePackagingMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempPackagingBuffer') IS NOT NULL DROP TABLE ##TempPackagingBuffer;
    CREATE TABLE ##TempPackagingBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        EstimatedWeightLbs DECIMAL(10,2),
        HandlingSurcharge DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @weight DECIMAL(10,2), @surcharge DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT LogisticsID, ProductName, EstimatedWeightLbs, HandlingSurcharge 
        FROM Table_PackagingLogistics;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @weight, @surcharge;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempPackagingBuffer VALUES (@newTempID, @pname, @weight, @surcharge);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_PackagingLogistics', 'LogisticsID', CAST(@tid AS VARCHAR), '##TempPackagingBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @weight, @surcharge;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizePackagingReport;
END;
GO

EXEC proc_StagePackagingMetrics;