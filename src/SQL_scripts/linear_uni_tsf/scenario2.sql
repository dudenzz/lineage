-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Building a Global Shipping and Storage Capacity ledger.
-- Rule: UNION ALL between different tables (Orders and Products), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Consolidating shipping volumes (Orders) and warehouse capacity (Products) into a unified unit-count index.

IF OBJECT_ID('Table_UnifiedCapacityLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedCapacityLedger;
CREATE TABLE Table_UnifiedCapacityLedger (
    LedgerID INT,
    AssetType VARCHAR(20),
    OriginalPKID INT,
    ReferenceName NVARCHAR(40),
    NormalizedUnits DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_AssetType VARCHAR(20),
        @v_OriginalPKID INT,
        @v_ReferenceName NVARCHAR(40),
        @v_NormalizedUnits DECIMAL(18, 2),
        @nextLedgerID INT;

-- Linear Transformation Constants:
-- 1. For Orders: We assume a flat palletization factor (y = 1.0 * x + 0.5 for handling overhead).
-- 2. For Products: We convert 'UnitsInStock' to 'CrateVolume' (y = 2.25 * x + 10 for safety buffer).
DECLARE @OrderScale DECIMAL(10,2) = 1.0;
DECLARE @OrderOffset DECIMAL(10,2) = 0.5;
DECLARE @StockScale DECIMAL(10,2) = 2.25;
DECLARE @StockOffset DECIMAL(10,2) = 10.0;

-- Cursor combining DIFFERENT tables (Orders and Products) via UNION ALL
DECLARE CapacityCursor CURSOR FOR 
    -- Branch 1: Orders (Shipping Demand)
    -- Selection: Only orders requiring international shipping (ShipCountry != 'USA')
    -- Transformation: Linear normalization of Freight costs to "Space Units"
    SELECT 
        'ShippingDemand' AS AssetType, 
        OrderID AS OriginalPKID, 
        ShipName AS ReferenceName, 
        (CAST(Freight AS DECIMAL(18,2)) * @OrderScale) + @OrderOffset AS NormalizedUnits 
    FROM Orders 
    WHERE ShipCountry <> 'USA' -- Selection
    
    UNION ALL

    -- Branch 2: Products (Warehouse Supply)
    -- Selection: Only products with stock levels above 0
    -- Transformation: Linear conversion of stock count to "Occupancy Units"
    SELECT 
        'WarehouseSupply' AS AssetType, 
        ProductID AS OriginalPKID, 
        ProductName AS ReferenceName, 
        (CAST(UnitsInStock AS DECIMAL(18,2)) * @StockScale) + @StockOffset AS NormalizedUnits 
    FROM Products
    WHERE UnitsInStock > 0; -- Selection

OPEN CapacityCursor;
FETCH NEXT FROM CapacityCursor INTO @v_AssetType, @v_OriginalPKID, @v_ReferenceName, @v_NormalizedUnits;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLedgerID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedCapacityLedger (LedgerID, AssetType, OriginalPKID, ReferenceName, NormalizedUnits)
    VALUES (@nextLedgerID, @v_AssetType, @v_OriginalPKID, @v_ReferenceName, @v_NormalizedUnits);

    -- Tracking Row-Level Lineage based on the Union origin
    IF @v_AssetType = 'ShippingDemand'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', CAST(@v_OriginalPKID AS VARCHAR), 'Table_UnifiedCapacityLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    ELSE IF @v_AssetType = 'WarehouseSupply'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_OriginalPKID AS VARCHAR), 'Table_UnifiedCapacityLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    
    FETCH NEXT FROM CapacityCursor INTO @v_AssetType, @v_OriginalPKID, @v_ReferenceName, @v_NormalizedUnits;
END;

CLOSE CapacityCursor; 
DEALLOCATE CapacityCursor;
GO