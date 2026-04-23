-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Facility Maintenance & Shipping Insurance Ledger.
-- Rule: UNION ALL between different tables (Suppliers and Orders), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Facility Upkeep Allowances" for domestic suppliers and "Transit Protection Costs" for international shipments.

IF OBJECT_ID('Table_FacilityTransitLedger', 'U') IS NOT NULL DROP TABLE Table_FacilityTransitLedger;
CREATE TABLE Table_FacilityTransitLedger (
    LedgerID INT,
    AssetContext VARCHAR(25),
    SourcePKID NVARCHAR(15),
    AssetLabel NVARCHAR(40),
    CalculatedFee MONEY -- Linearly transformed column
);
GO

DECLARE @v_AssetContext VARCHAR(25),
        @v_SourcePKID NVARCHAR(15),
        @v_AssetLabel NVARCHAR(40),
        @v_CalculatedFee MONEY,
        @nextLedgerID INT;

-- Linear Transformation Constants:
-- 1. Facility Upkeep (Suppliers): Allowance based on SupplierID (as a regional density proxy) plus a flat base (y = 15.00 * SupplierID + 200.00)
-- 2. Transit Protection (Orders): Insurance fee based on Freight weight plus a flat documentation fee (y = 0.25 * Freight + 50.00)
DECLARE @FacilityScalar MONEY = 15.00;
DECLARE @FacilityBaseAllowance MONEY = 200.00;
DECLARE @TransitScalar MONEY = 0.25;
DECLARE @TransitFlatFee MONEY = 50.00;

-- Cursor combining DIFFERENT tables (Suppliers and Orders) via UNION ALL
DECLARE FacilityTransitCursor CURSOR FOR 
    -- Branch 1: Suppliers (Facility Maintenance)
    -- Selection: Only suppliers from the UK (Specific regional budget)
    -- Transformation: Linear maintenance allowance (y = 15.00 * x + 200.00)
    SELECT 
        'FacilityUpkeep' AS AssetContext, 
        CAST(SupplierID AS NVARCHAR(15)) AS SourcePKID, 
        CompanyName AS AssetLabel, 
        (CAST(SupplierID AS MONEY) * @FacilityScalar) + @FacilityBaseAllowance AS CalculatedFee 
    FROM Suppliers 
    WHERE Country = 'UK' -- Selection
    
    UNION ALL

    -- Branch 2: Orders (Shipping Protection)
    -- Selection: Only international orders (ShipCountry != 'USA')
    -- Transformation: Linear protection cost scaling (y = 0.25 * x + 50.00)
    SELECT 
        'TransitProtection' AS AssetContext, 
        CAST(OrderID AS NVARCHAR(15)) AS SourcePKID, 
        ShipName AS AssetLabel, 
        (Freight * @TransitScalar) + @TransitFlatFee AS CalculatedFee 
    FROM Orders
    WHERE ShipCountry <> 'USA'; -- Selection

OPEN FacilityTransitCursor;
FETCH NEXT FROM FacilityTransitCursor INTO @v_AssetContext, @v_SourcePKID, @v_AssetLabel, @v_CalculatedFee;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLedgerID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_FacilityTransitLedger (LedgerID, AssetContext, SourcePKID, AssetLabel, CalculatedFee)
    VALUES (@nextLedgerID, @v_AssetContext, @v_SourcePKID, @v_AssetLabel, @v_CalculatedFee);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AssetContext = 'FacilityUpkeep'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_SourcePKID, 'Table_FacilityTransitLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    ELSE IF @v_AssetContext = 'TransitProtection'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', @v_SourcePKID, 'Table_FacilityTransitLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    
    FETCH NEXT FROM FacilityTransitCursor INTO @v_AssetContext, @v_SourcePKID, @v_AssetLabel, @v_CalculatedFee;
END;

CLOSE FacilityTransitCursor; 
DEALLOCATE FacilityTransitCursor;
GO