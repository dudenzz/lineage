-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Asset Disposal & Recovery Ledger.
-- Rule: UNION ALL between different tables (Orders and Products), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Depreciated Recovery Value" for historical order assets and stagnant inventory.

IF OBJECT_ID('Table_AssetRecoveryLedger', 'U') IS NOT NULL DROP TABLE Table_AssetRecoveryLedger;
CREATE TABLE Table_AssetRecoveryLedger (
    RecoveryID INT,
    AssetType VARCHAR(25),
    SourcePKID INT,
    EntityLabel NVARCHAR(40),
    RecoveryValue MONEY -- Linearly transformed column
);
GO

DECLARE @v_AssetType VARCHAR(25),
        @v_SourcePKID INT,
        @v_EntityLabel NVARCHAR(40),
        @v_RecoveryValue MONEY,
        @nextRecoveryID INT;

-- Linear Transformation Constants:
-- 1. Order Assets (Orders): Recovery value is a fraction of the Freight cost minus a flat processing fee (y = 0.40 * Freight - 5.00)
-- 2. Stagnant Stock (Products): Recovery value is a flat liquidation rate per unit (y = 0 * x + 15.00)
DECLARE @FreightRecoveryScalar MONEY = 0.40;
DECLARE @FreightProcessingFee MONEY = 5.00;
DECLARE @StockLiquidationRate MONEY = 15.00;

-- Cursor combining DIFFERENT tables (Orders and Products) via UNION ALL
DECLARE AssetRecoveryCursor CURSOR FOR 
    -- Branch 1: Orders (Historical Shipping Assets)
    -- Selection: Only orders from the year 1996 (Legacy assets)
    -- Transformation: Linear depreciation of original freight costs (y = 0.40 * x - 5.00)
    SELECT 
        'LegacyOrderAsset' AS AssetType, 
        OrderID AS SourcePKID, 
        ShipName AS EntityLabel, 
        (Freight * @FreightRecoveryScalar) - @FreightProcessingFee AS RecoveryValue 
    FROM Orders 
    WHERE OrderDate < '1997-01-01' -- Selection (Legacy data)
    
    UNION ALL

    -- Branch 2: Products (Stagnant Inventory)
    -- Selection: Products that need reordering (UnitsOnOrder > 0) but have no stock (UnitsInStock = 0)
    -- Transformation: Flat linear recovery projection (y = 0 * x + 15.00)
    SELECT 
        'StagnantInventory' AS AssetType, 
        ProductID AS SourcePKID, 
        ProductName AS EntityLabel, 
        @StockLiquidationRate AS RecoveryValue 
    FROM Products
    WHERE UnitsInStock = 0 AND UnitsOnOrder > 0; -- Selection

OPEN AssetRecoveryCursor;
FETCH NEXT FROM AssetRecoveryCursor INTO @v_AssetType, @v_SourcePKID, @v_EntityLabel, @v_RecoveryValue;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextRecoveryID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_AssetRecoveryLedger (RecoveryID, AssetType, SourcePKID, EntityLabel, RecoveryValue)
    VALUES (@nextRecoveryID, @v_AssetType, @v_SourcePKID, @v_EntityLabel, @v_RecoveryValue);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AssetType = 'LegacyOrderAsset'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', CAST(@v_SourcePKID AS VARCHAR), 'Table_AssetRecoveryLedger', 'RecoveryID', CAST(@nextRecoveryID AS VARCHAR));
    END
    ELSE IF @v_AssetType = 'StagnantInventory'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_SourcePKID AS VARCHAR), 'Table_AssetRecoveryLedger', 'RecoveryID', CAST(@nextRecoveryID AS VARCHAR));
    END
    
    FETCH NEXT FROM AssetRecoveryCursor INTO @v_AssetType, @v_SourcePKID, @v_EntityLabel, @v_RecoveryValue;
END;

CLOSE AssetRecoveryCursor; 
DEALLOCATE AssetRecoveryCursor;
GO