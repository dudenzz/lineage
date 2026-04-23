-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Category-Based Inventory Valuation & Stock Management Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_CategoryInventoryValueLedger', 'U') IS NOT NULL DROP TABLE Table_CategoryInventoryValueLedger;
CREATE TABLE Table_CategoryInventoryValueLedger (
    ValueAuditID INT,
    ProductID INT,           -- Native Projection from Products
    CategoryID INT,          -- Native Projection from Categories
    ProductName NVARCHAR(40), -- Native Projection from Products
    CategoryName NVARCHAR(15),-- Native Projection from Categories
    AssessedValue MONEY,      -- Linearly transformed column (Products.UnitPrice)
    SafetyStockBuffer DECIMAL(18,2) -- Linearly transformed column (Products.ReorderLevel)
);
GO

DECLARE @v_ProductID INT,
        @v_CategoryID INT,
        @v_ProductName NVARCHAR(40),
        @v_CategoryName NVARCHAR(15),
        @v_AssessedValue MONEY,
        @v_SafetyStockBuffer DECIMAL(18,2),
        @nextValueAuditID INT;

-- Linear Transformation Constants:
-- 1. Assessed Value: Adjusted price reflecting a flat overhead and bulk markup (y = 1.25 * UnitPrice + 10.00)
-- 2. Safety Stock Buffer: Enhanced reorder threshold based on a volatility scalar (y = 1.50 * ReorderLevel + 2.00)
DECLARE @ValueScalar DECIMAL(10,2) = 1.25;
DECLARE @ValueBase MONEY = 10.00;
DECLARE @BufferScalar DECIMAL(10,2) = 1.50;
DECLARE @BufferBase DECIMAL(10,2) = 2.00;

-- Cursor using JOIN to integrate Product stock metrics with Category classifications.
-- Selection: Only active products (Discontinued = 0) in the 'Meat/Poultry' or 'Produce' categories.
DECLARE InventoryValueCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        C.CategoryID, 
        P.ProductName, 
        C.CategoryName, 
        (P.UnitPrice * @ValueScalar) + @ValueBase AS AssessedValue,
        (CAST(P.ReorderLevel AS DECIMAL(18,2)) * @BufferScalar) + @BufferBase AS SafetyStockBuffer
    FROM Products P
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    WHERE P.Discontinued = 0 AND C.CategoryName IN ('Meat/Poultry', 'Produce'); -- Selection

OPEN InventoryValueCursor;
FETCH NEXT FROM InventoryValueCursor INTO 
    @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_AssessedValue, @v_SafetyStockBuffer;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextValueAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_CategoryInventoryValueLedger (
        ValueAuditID, ProductID, CategoryID, ProductName, CategoryName, AssessedValue, SafetyStockBuffer
    )
    VALUES (
        @nextValueAuditID, @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_AssessedValue, @v_SafetyStockBuffer
    );

    -- Log Dual-Source Lineage for the Joined Records
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_CategoryInventoryValueLedger', 'ValueAuditID', CAST(@nextValueAuditID AS VARCHAR));
    
    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_CategoryInventoryValueLedger', 'ValueAuditID', CAST(@nextValueAuditID AS VARCHAR));
    
    FETCH NEXT FROM InventoryValueCursor INTO 
        @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_AssessedValue, @v_SafetyStockBuffer;
END;

CLOSE InventoryValueCursor; 
DEALLOCATE InventoryValueCursor;
GO