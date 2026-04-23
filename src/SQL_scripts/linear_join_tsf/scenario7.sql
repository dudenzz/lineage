-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Product-Category Stock Replenishment & Financial Buffer Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_StockReplenishmentLedger', 'U') IS NOT NULL DROP TABLE Table_StockReplenishmentLedger;
CREATE TABLE Table_StockReplenishmentLedger (
    ReplenishmentAuditID INT,
    ProductID INT,           -- Native Projection from Products
    CategoryID INT,          -- Native Projection from Categories
    ProductName NVARCHAR(40), -- Native Projection from Products
    CategoryName NVARCHAR(15),-- Native Projection from Categories
    AdjustedUnitCost MONEY,   -- Linearly transformed column (Products.UnitPrice)
    SafetyBufferLevel DECIMAL(18,2), -- Linearly transformed column (Products.ReorderLevel)
    WarehouseStorageIndex DECIMAL(18,2) -- Linearly transformed column (Products.UnitsInStock)
);
GO

DECLARE @v_ProductID INT,
        @v_CategoryID INT,
        @v_ProductName NVARCHAR(40),
        @v_CategoryName NVARCHAR(15),
        @v_AdjustedUnitCost MONEY,
        @v_SafetyBufferLevel DECIMAL(18,2),
        @v_WarehouseStorageIndex DECIMAL(18,2),
        @nextReplenishmentAuditID INT;

-- Linear Transformation Constants:
-- 1. Adjusted Unit Cost: Incorporates a flat logistics surcharge and regional pricing factor (y = 1.08 * UnitPrice + 3.75)
-- 2. Safety Buffer Level: Scales the reorder point to account for supply chain lag (y = 1.35 * ReorderLevel + 10.00)
-- 3. Warehouse Storage Index: Calculates spatial utility based on current stock (y = 0.65 * UnitsInStock + 20.00)
DECLARE @CostScalar DECIMAL(10,2) = 1.08;
DECLARE @CostBase MONEY = 3.75;
DECLARE @BufferScalar DECIMAL(10,2) = 1.35;
DECLARE @BufferBase DECIMAL(10,2) = 10.00;
DECLARE @StorageScalar DECIMAL(10,2) = 0.65;
DECLARE @StorageBase DECIMAL(10,2) = 20.00;

-- Cursor using JOIN to integrate Product inventory states with Category classifications.
-- Selection: Only products in 'Beverages', 'Dairy Products', or 'Meat/Poultry' that are not discontinued.
DECLARE ReplenishmentCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        C.CategoryID, 
        P.ProductName, 
        C.CategoryName, 
        (P.UnitPrice * @CostScalar) + @CostBase AS AdjustedUnitCost,
        (CAST(P.ReorderLevel AS DECIMAL(18,2)) * @BufferScalar) + @BufferBase AS SafetyBufferLevel,
        (CAST(P.UnitsInStock AS DECIMAL(18,2)) * @StorageScalar) + @StorageBase AS WarehouseStorageIndex
    FROM Products P
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    WHERE P.Discontinued = 0 
      AND C.CategoryName IN ('Beverages', 'Dairy Products', 'Meat/Poultry'); -- Selection

OPEN ReplenishmentCursor;
FETCH NEXT FROM ReplenishmentCursor INTO 
    @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_AdjustedUnitCost, @v_SafetyBufferLevel, @v_WarehouseStorageIndex;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextReplenishmentAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_StockReplenishmentLedger (
        ReplenishmentAuditID, ProductID, CategoryID, ProductName, CategoryName, AdjustedUnitCost, SafetyBufferLevel, WarehouseStorageIndex
    )
    VALUES (
        @nextReplenishmentAuditID, @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_AdjustedUnitCost, @v_SafetyBufferLevel, @v_WarehouseStorageIndex
    );

    -- Log Dual-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_StockReplenishmentLedger', 'ReplenishmentAuditID', CAST(@nextReplenishmentAuditID AS VARCHAR));
    
    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_StockReplenishmentLedger', 'ReplenishmentAuditID', CAST(@nextReplenishmentAuditID AS VARCHAR));
    
    FETCH NEXT FROM ReplenishmentCursor INTO 
        @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_AdjustedUnitCost, @v_SafetyBufferLevel, @v_WarehouseStorageIndex;
END;

CLOSE ReplenishmentCursor; 
DEALLOCATE ReplenishmentCursor;
GO