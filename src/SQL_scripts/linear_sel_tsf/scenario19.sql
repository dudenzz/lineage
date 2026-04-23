-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Suggested Retail Price (SRP) and Max Discount Thresholds for Catalogs.
CREATE OR ALTER VIEW vw_CatalogPricingStrategy AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitPrice,
    -- Linear Transformation 1: Suggested Retail Price ($) f(x) = 1.40x + 5.00
    CAST((UnitPrice * 1.40) + 5.00 AS DECIMAL(10,2)) AS SuggestedRetailPrice,
    -- Linear Transformation 2: Max Discount Allowance ($) f(x) = 0.15x + 1.00
    CAST((UnitPrice * 0.15) + 1.00 AS DECIMAL(10,2)) AS MaxDiscountAmount
FROM Products
WHERE UnitPrice > 0; -- Filter: Only calculate for products with a valid base price
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_CatalogPricingStrategy;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_CatalogPricingStrategy', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_GrainsCatalogPricing', 'U') IS NOT NULL DROP TABLE Table_GrainsCatalogPricing;
CREATE TABLE Table_GrainsCatalogPricing (
    CatalogID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitPrice DECIMAL(10,2),
    SuggestedRetailPrice DECIMAL(10,2),
    MaxDiscountAmount DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_price DECIMAL(10,2), @v_srp DECIMAL(10,2), @v_discount DECIMAL(10,2), @nextCatalogID INT;

-- Filter: Only process pricing for a specific category (e.g., Category 5 - Grains/Cereals)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitPrice, SuggestedRetailPrice, MaxDiscountAmount 
    FROM vw_CatalogPricingStrategy 
    WHERE CategoryID = 5;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price, @v_srp, @v_discount;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextCatalogID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_GrainsCatalogPricing (CatalogID, OriginalProductID, ProductName, UnitPrice, SuggestedRetailPrice, MaxDiscountAmount)
    VALUES (@nextCatalogID, @v_pid, @v_pname, @v_price, @v_srp, @v_discount);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_CatalogPricingStrategy', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_GrainsCatalogPricing', 'CatalogID', CAST(@nextCatalogID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price, @v_srp, @v_discount;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizePricingReport AS
BEGIN
    IF OBJECT_ID('Final_RetailStrategyReport', 'U') IS NOT NULL DROP TABLE Final_RetailStrategyReport;
    CREATE TABLE Final_RetailStrategyReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TargetRetailPrice DECIMAL(10,2), 
        ApprovedDiscount DECIMAL(10,2),
        StrategyStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_srp DECIMAL(10,2), @t_discount DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, SuggestedRetailPrice, MaxDiscountAmount 
        FROM ##TempPricingBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_srp, @t_discount;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_RetailStrategyReport (ReportID, ProductName, TargetRetailPrice, ApprovedDiscount, StrategyStatus)
        VALUES (@finalID, @t_pname, @t_srp, @t_discount, 'Published');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempPricingBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_RetailStrategyReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_srp, @t_discount;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StagePricingMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempPricingBuffer') IS NOT NULL DROP TABLE ##TempPricingBuffer;
    CREATE TABLE ##TempPricingBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        SuggestedRetailPrice DECIMAL(10,2),
        MaxDiscountAmount DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @srp DECIMAL(10,2), @discount DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT CatalogID, ProductName, SuggestedRetailPrice, MaxDiscountAmount 
        FROM Table_GrainsCatalogPricing;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @srp, @discount;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempPricingBuffer VALUES (@newTempID, @pname, @srp, @discount);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_GrainsCatalogPricing', 'CatalogID', CAST(@tid AS VARCHAR), '##TempPricingBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @srp, @discount;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizePricingReport;
END;
GO

EXEC proc_StagePricingMetrics;