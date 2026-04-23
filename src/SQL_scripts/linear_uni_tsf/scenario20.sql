-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Asset Maintenance & Fleet Operational Readiness Ledger.
-- Rule: UNION ALL between different tables (Suppliers and Shippers), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Facility Reinvestment Credits" for domestic suppliers and "Fleet Modernization Fees" for logistics partners.

IF OBJECT_ID('Table_InfrastructureReadinessLedger', 'U') IS NOT NULL DROP TABLE Table_InfrastructureReadinessLedger;
CREATE TABLE Table_InfrastructureReadinessLedger (
    ReadinessID INT,
    AssetSector VARCHAR(25),
    SourcePKID INT,
    AssetReference NVARCHAR(40),
    AdjustmentValue MONEY -- Linearly transformed column
);
GO

DECLARE @v_AssetSector VARCHAR(25),
        @v_SourcePKID INT,
        @v_AssetReference NVARCHAR(40),
        @v_AdjustmentValue MONEY,
        @nextReadinessID INT;

-- Linear Transformation Constants:
-- 1. Suppliers: Reinvestment credit based on ID (legacy node proxy) plus a flat regional bonus (y = 18.25 * SupplierID + 350.00)
-- 2. Shippers: Modernization fee based on a flat baseline for essential logistics (y = 0 * x + 750.00)
DECLARE @SupplierReinvestmentScalar MONEY = 18.25;
DECLARE @SupplierRegionalBonus MONEY = 350.00;
DECLARE @ShipperModernizationBase MONEY = 750.00;

-- Cursor combining DIFFERENT tables (Suppliers and Shippers) via UNION ALL
DECLARE ReadinessCursor CURSOR FOR 
    -- Branch 1: Suppliers (Facility Maintenance/Reinvestment)
    -- Selection: Only suppliers from North America (USA and Canada)
    -- Transformation: Linear reinvestment scaling (y = 18.25 * x + 350.00)
    SELECT 
        'FacilityMaintenance' AS AssetSector, 
        SupplierID AS SourcePKID, 
        CompanyName AS AssetReference, 
        (CAST(SupplierID AS MONEY) * @SupplierReinvestmentScalar) + @SupplierRegionalBonus AS AdjustmentValue 
    FROM Suppliers 
    WHERE Country IN ('USA', 'Canada') -- Selection
    
    UNION ALL

    -- Branch 2: Shippers (Fleet Operations/Modernization)
    -- Selection: All registered shippers (Core logistics infrastructure)
    -- Transformation: Constant linear modernization projection (y = 0 * x + 750.00)
    SELECT 
        'FleetModernization' AS AssetSector, 
        ShipperID AS SourcePKID, 
        CompanyName AS AssetReference, 
        @ShipperModernizationBase AS AdjustmentValue 
    FROM Shippers;

OPEN ReadinessCursor;
FETCH NEXT FROM ReadinessCursor INTO @v_AssetSector, @v_SourcePKID, @v_AssetReference, @v_AdjustmentValue;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextReadinessID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_InfrastructureReadinessLedger (ReadinessID, AssetSector, SourcePKID, AssetReference, AdjustmentValue)
    VALUES (@nextReadinessID, @v_AssetSector, @v_SourcePKID, @v_AssetReference, @v_AdjustmentValue);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AssetSector = 'FacilityMaintenance'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', CAST(@v_SourcePKID AS VARCHAR), 'Table_InfrastructureReadinessLedger', 'ReadinessID', CAST(@nextReadinessID AS VARCHAR));
    END
    ELSE IF @v_AssetSector = 'FleetModernization'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', CAST(@v_SourcePKID AS VARCHAR), 'Table_InfrastructureReadinessLedger', 'ReadinessID', CAST(@nextReadinessID AS VARCHAR));
    END
    
    FETCH NEXT FROM ReadinessCursor INTO @v_AssetSector, @v_SourcePKID, @v_AssetReference, @v_AdjustmentValue;
END;

CLOSE ReadinessCursor; 
DEALLOCATE ReadinessCursor;
GO