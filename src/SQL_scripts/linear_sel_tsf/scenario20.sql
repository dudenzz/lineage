-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Liquidation Pricing and Bundle Assembly Costs for Overstocked Items.
CREATE OR ALTER VIEW vw_OverstockLiquidation AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitsInStock,
    UnitPrice,
    -- Linear Transformation 1: Liquidation Base Price ($) f(x) = 0.40x + 1.50
    CAST((UnitPrice * 0.40) + 1.50 AS DECIMAL(10,2)) AS LiquidationPrice,
    -- Linear Transformation 2: Bundle Assembly Cost ($) f(x) = 0.10x + 5.00
    CAST((UnitsInStock * 0.10) + 5.00 AS DECIMAL(10,2)) AS BundleAssemblyCost
FROM Products
WHERE UnitsInStock >= 50; -- Filter: Only calculate for products with significant overstock
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_OverstockLiquidation;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_OverstockLiquidation', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_BeverageLiquidation', 'U') IS NOT NULL DROP TABLE Table_BeverageLiquidation;
CREATE TABLE Table_BeverageLiquidation (
    LiquidationID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitsInStock SMALLINT,
    LiquidationPrice DECIMAL(10,2),
    BundleAssemblyCost DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_uis SMALLINT, @v_liq_price DECIMAL(10,2), @v_bundle_cost DECIMAL(10,2), @nextLiqID INT;

-- Filter: Only process liquidation metrics for a specific category (e.g., Category 1 - Beverages)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitsInStock, LiquidationPrice, BundleAssemblyCost 
    FROM vw_OverstockLiquidation 
    WHERE CategoryID = 1;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_liq_price, @v_bundle_cost;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLiqID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_BeverageLiquidation (LiquidationID, OriginalProductID, ProductName, UnitsInStock, LiquidationPrice, BundleAssemblyCost)
    VALUES (@nextLiqID, @v_pid, @v_pname, @v_uis, @v_liq_price, @v_bundle_cost);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_OverstockLiquidation', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_BeverageLiquidation', 'LiquidationID', CAST(@nextLiqID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_liq_price, @v_bundle_cost;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeLiquidationReport AS
BEGIN
    IF OBJECT_ID('Final_ClearanceStrategy', 'U') IS NOT NULL DROP TABLE Final_ClearanceStrategy;
    CREATE TABLE Final_ClearanceStrategy (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TargetClearancePrice DECIMAL(10,2), 
        EstimatedPrepCost DECIMAL(10,2),
        MarketStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_liq_price DECIMAL(10,2), @t_bundle_cost DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, LiquidationPrice, BundleAssemblyCost 
        FROM ##TempLiquidationBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_liq_price, @t_bundle_cost;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ClearanceStrategy (ReportID, ProductName, TargetClearancePrice, EstimatedPrepCost, MarketStatus)
        VALUES (@finalID, @t_pname, @t_liq_price, @t_bundle_cost, 'Ready for Clearance');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempLiquidationBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ClearanceStrategy', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_liq_price, @t_bundle_cost;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageLiquidationMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempLiquidationBuffer') IS NOT NULL DROP TABLE ##TempLiquidationBuffer;
    CREATE TABLE ##TempLiquidationBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        LiquidationPrice DECIMAL(10,2),
        BundleAssemblyCost DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @liq_price DECIMAL(10,2), @bundle_cost DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT LiquidationID, ProductName, LiquidationPrice, BundleAssemblyCost 
        FROM Table_BeverageLiquidation;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @liq_price, @bundle_cost;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempLiquidationBuffer VALUES (@newTempID, @pname, @liq_price, @bundle_cost);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_BeverageLiquidation', 'LiquidationID', CAST(@tid AS VARCHAR), '##TempLiquidationBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @liq_price, @bundle_cost;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeLiquidationReport;
END;
GO

EXEC proc_StageLiquidationMetrics;