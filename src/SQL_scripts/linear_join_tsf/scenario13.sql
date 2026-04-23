-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Category-Based Inventory Valuation & Financial Forecasting Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_CategoryInventoryForecastLedger', 'U') IS NOT NULL DROP TABLE Table_CategoryInventoryForecastLedger;
CREATE TABLE Table_CategoryInventoryForecastLedger (
    ForecastAuditID INT,
    ProductID INT,           -- Native Projection from Products
    CategoryID INT,          -- Native Projection from Categories
    ProductName NVARCHAR(40), -- Native Projection from Products
    CategoryName NVARCHAR(15),-- Native Projection from Categories
    MarkedUpPrice MONEY,      -- Linearly transformed column (Products.UnitPrice)
    ProjectedHoldingCost MONEY, -- Linearly transformed column (Products.UnitPrice)
    StockCriticalityIndex DECIMAL(18,2) -- Linearly transformed column (Products.ReorderLevel)
);
GO

DECLARE @v_ProductID INT,
        @v_CategoryID INT,
        @v_ProductName NVARCHAR(40),
        @v_CategoryName NVARCHAR(15),
        @v_MarkedUpPrice MONEY,
        @v_ProjectedHoldingCost MONEY,
        @v_StockCriticalityIndex DECIMAL(18,2),
        @nextForecastAuditID INT;

-- Linear Transformation Constants:
-- 1. Marked Up Price: Standard retail markup over wholesale unit price (y = 1.45 * UnitPrice + 10.00)
-- 2. Projected Holding Cost: Annualized storage cost per unit based on value (y = 0.12 * UnitPrice + 2.50)
-- 3. Stock Criticality Index: Scaled priority score derived from reorder thresholds (y = 3.20 * ReorderLevel + 15.00)
DECLARE @MarkupScalar DECIMAL(10,2) = 1.45;
DECLARE @MarkupBase MONEY = 10.00;
DECLARE @HoldingScalar DECIMAL(10,2) = 0.12;
DECLARE @HoldingBase MONEY = 2.50;
DECLARE @CriticalityScalar DECIMAL(10,2) = 3.20;
DECLARE @CriticalityBase DECIMAL(10,2) = 15.00;

-- Cursor using JOIN to integrate Product inventory thresholds with Category classifications.
-- Selection: Only active products (Discontinued = 0) within the 'Beverages' or 'Condiments' categories.
DECLARE ForecastCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        C.CategoryID, 
        P.ProductName, 
        C.CategoryName, 
        (P.UnitPrice * @MarkupScalar) + @MarkupBase AS MarkedUpPrice,
        (P.UnitPrice * @HoldingScalar) + @HoldingBase AS ProjectedHoldingCost,
        (CAST(P.ReorderLevel AS DECIMAL(18,2)) * @CriticalityScalar) + @CriticalityBase AS StockCriticalityIndex
    FROM Products P
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    WHERE P.Discontinued = 0 AND C.CategoryName IN ('Beverages', 'Condiments'); -- Selection

OPEN ForecastCursor;
FETCH NEXT FROM ForecastCursor INTO 
    @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_MarkedUpPrice, @v_ProjectedHoldingCost, @v_StockCriticalityIndex;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextForecastAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_CategoryInventoryForecastLedger (
        ForecastAuditID, ProductID, CategoryID, ProductName, CategoryName, MarkedUpPrice, ProjectedHoldingCost, StockCriticalityIndex
    )
    VALUES (
        @nextForecastAuditID, @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_MarkedUpPrice, @v_ProjectedHoldingCost, @v_StockCriticalityIndex
    );

    -- Log Dual-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_CategoryInventoryForecastLedger', 'ForecastAuditID', CAST(@nextForecastAuditID AS VARCHAR));
    
    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_CategoryInventoryForecastLedger', 'ForecastAuditID', CAST(@nextForecastAuditID AS VARCHAR));
    
    FETCH NEXT FROM ForecastCursor INTO 
        @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_MarkedUpPrice, @v_ProjectedHoldingCost, @v_StockCriticalityIndex;
END;

CLOSE ForecastCursor; 
DEALLOCATE ForecastCursor;
GO