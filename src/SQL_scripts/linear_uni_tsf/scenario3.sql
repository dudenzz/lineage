-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Building a Global Insurance Value Registry for physical and transitional assets.
-- Rule: UNION ALL between different tables (Products and Orders), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating a standardized "Insurance Value" for inventory on shelves and goods currently in transit.

IF OBJECT_ID('Table_AssetInsuranceRegistry', 'U') IS NOT NULL DROP TABLE Table_AssetInsuranceRegistry;
CREATE TABLE Table_AssetInsuranceRegistry (
    InsuranceID INT,
    AssetCategory VARCHAR(25),
    SourcePKID INT,
    AssetDescriptor NVARCHAR(40),
    CoverageValue MONEY -- Linearly transformed column
);
GO

DECLARE @v_AssetCategory VARCHAR(25),
        @v_SourcePKID INT,
        @v_AssetDescriptor NVARCHAR(40),
        @v_CoverageValue MONEY,
        @nextInsuranceID INT;

-- Linear Transformation Constants (Insurance Risk Models):
-- 1. Inventory (Products): Value is based on UnitPrice with a 15% luxury tax adjustment (y = 1.15x + 0.00)
-- 2. Transit (Orders): Value is based on Freight with a flat $100 base liability coverage (y = 1.00x + 100.00)
DECLARE @InventoryRiskScalar MONEY = 1.15;
DECLARE @TransitLiabilityOffset MONEY = 100.00;

-- Cursor combining DIFFERENT tables (Products and Orders) via UNION ALL
DECLARE InsuranceCursor CURSOR FOR 
    -- Branch 1: Products (In-Stock Inventory)
    -- Selection: High-value items only (UnitPrice > 20)
    -- Transformation: Linear scale for insurance premium (y = 1.15 * UnitPrice)
    SELECT 
        'InStockInventory' AS AssetCategory, 
        ProductID AS SourcePKID, 
        ProductName AS AssetDescriptor, 
        (UnitPrice * @InventoryRiskScalar) AS CoverageValue 
    FROM Products 
    WHERE UnitPrice > 20 -- Selection (High-value items)
    
    UNION ALL

    -- Branch 2: Orders (Goods in Transit)
    -- Selection: Orders shipped via 'Speedy Express' (ShipVia = 1)
    -- Transformation: Linear base coverage plus flat fee (y = 1.0 * Freight + 100.00)
    SELECT 
        'InTransitGoods' AS AssetCategory, 
        OrderID AS SourcePKID, 
        ShipName AS AssetDescriptor, 
        (Freight + @TransitLiabilityOffset) AS CoverageValue 
    FROM Orders
    WHERE ShipVia = 1; -- Selection (Specific carrier risk)

OPEN InsuranceCursor;
FETCH NEXT FROM InsuranceCursor INTO @v_AssetCategory, @v_SourcePKID, @v_AssetDescriptor, @v_CoverageValue;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextInsuranceID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the projected, filtered, and transformed record
    INSERT INTO Table_AssetInsuranceRegistry (InsuranceID, AssetCategory, SourcePKID, AssetDescriptor, CoverageValue)
    VALUES (@nextInsuranceID, @v_AssetCategory, @v_SourcePKID, @v_AssetDescriptor, @v_CoverageValue);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AssetCategory = 'InStockInventory'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_SourcePKID AS VARCHAR), 'Table_AssetInsuranceRegistry', 'InsuranceID', CAST(@nextInsuranceID AS VARCHAR));
    END
    ELSE IF @v_AssetCategory = 'InTransitGoods'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', CAST(@v_SourcePKID AS VARCHAR), 'Table_AssetInsuranceRegistry', 'InsuranceID', CAST(@nextInsuranceID AS VARCHAR));
    END
    
    FETCH NEXT FROM InsuranceCursor INTO @v_AssetCategory, @v_SourcePKID, @v_AssetDescriptor, @v_CoverageValue;
END;

CLOSE InsuranceCursor; 
DEALLOCATE InsuranceCursor;
GO