-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Building a Global Inventory Value Ledger by combining disparate product sources.
-- Rule: UNION ALL between DIFFERENT tables, Selection (WHERE), and Linear Transformations (y = cx + d).
-- Lineage: Tracks specific source table and primary key for every record in the union.

IF OBJECT_ID('Table_GlobalInventoryValue', 'U') IS NOT NULL DROP TABLE Table_GlobalInventoryValue;
CREATE TABLE Table_GlobalInventoryValue (
    LedgerID INT,
    SourceSystem VARCHAR(20),
    OriginalPKID INT,
    EntityName NVARCHAR(40),
    RawStockCount INT,
    EstimatedValue MONEY -- Linearly transformed column
);
GO

DECLARE @v_SourceSystem VARCHAR(20),
        @v_OriginalPKID INT,
        @v_EntityName NVARCHAR(40),
        @v_RawStock INT,
        @v_EstimatedValue MONEY,
        @nextLedgerID INT;

-- Define constants for Linear Transformations
-- For Products: Estimated value is UnitPrice * constant (Stock Level) + offset
-- For Suppliers: Since we don't have stock counts, we use a constant projection 
-- for audit value based on their region.
DECLARE @ProductValuationScalar MONEY = 15.50; -- Constant weight for product valuation
DECLARE @BaseHandlingPremium MONEY = 50.00;   -- Constant d in y = cx + d

-- Cursor combining DIFFERENT tables (Products and Suppliers) via UNION ALL
DECLARE GlobalInventoryCursor CURSOR FOR 
    -- Branch 1: Products table
    -- Selection: Only currently available products
    -- Transformation: Linear scaling of UnitsInStock (y = 15.50 * UnitsInStock + 50.00)
    SELECT 
        'ProductStore' AS SourceSystem, 
        ProductID AS OriginalPKID, 
        ProductName AS EntityName, 
        CAST(UnitsInStock AS INT) AS RawStock, 
        (CAST(UnitsInStock AS MONEY) * @ProductValuationScalar) + @BaseHandlingPremium AS EstimatedValue 
    FROM Products 
    WHERE Discontinued = 0 -- Selection
    
    UNION ALL

    -- Branch 2: Suppliers table
    -- Selection: Only Suppliers from specific high-value regions
    -- Transformation: Constant linear projection (y = 0 * x + 500.00) for fixed-asset audits
    SELECT 
        'SupplierAsset' AS SourceSystem, 
        SupplierID AS OriginalPKID, 
        CompanyName AS EntityName, 
        0 AS RawStock, 
        500.00 AS EstimatedValue 
    FROM Suppliers
    WHERE Region IN ('WA', 'OR', 'BC'); -- Selection

OPEN GlobalInventoryCursor;
FETCH NEXT FROM GlobalInventoryCursor INTO @v_SourceSystem, @v_OriginalPKID, @v_EntityName, @v_RawStock, @v_EstimatedValue;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLedgerID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_GlobalInventoryValue (LedgerID, SourceSystem, OriginalPKID, EntityName, RawStockCount, EstimatedValue)
    VALUES (@nextLedgerID, @v_SourceSystem, @v_OriginalPKID, @v_EntityName, @v_RawStock, @v_EstimatedValue);

    -- Route Lineage tracking to the correct source table based on the Union branch
    IF @v_SourceSystem = 'ProductStore'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_OriginalPKID AS VARCHAR), 'Table_GlobalInventoryValue', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    ELSE IF @v_SourceSystem = 'SupplierAsset'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', CAST(@v_OriginalPKID AS VARCHAR), 'Table_GlobalInventoryValue', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    
    FETCH NEXT FROM GlobalInventoryCursor INTO @v_SourceSystem, @v_OriginalPKID, @v_EntityName, @v_RawStock, @v_EstimatedValue;
END;

CLOSE GlobalInventoryCursor; 
DEALLOCATE GlobalInventoryCursor;
GO