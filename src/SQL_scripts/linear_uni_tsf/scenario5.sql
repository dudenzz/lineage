-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Building a Global Infrastructure Maintenance Ledger for facility and shipping assets.
-- Rule: UNION ALL between different tables (Suppliers and Shippers), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating a standardized "Annual Maintenance Budget" for supplier facilities and shipping fleet assets.

IF OBJECT_ID('Table_InfrastructureMaintenanceLedger', 'U') IS NOT NULL DROP TABLE Table_InfrastructureMaintenanceLedger;
CREATE TABLE Table_InfrastructureMaintenanceLedger (
    MaintenanceID INT,
    AssetClass VARCHAR(25),
    SourcePKID INT,
    AssetName NVARCHAR(40),
    AllocatedBudget MONEY -- Linearly transformed column
);
GO

DECLARE @v_AssetClass VARCHAR(25),
        @v_SourcePKID INT,
        @v_AssetName NVARCHAR(40),
        @v_AllocatedBudget MONEY,
        @nextMaintenanceID INT;

-- Linear Transformation Constants (Maintenance Models):
-- 1. Supplier Facilities: Budget is based on ID (proxy for site age) with a base renovation fee (y = 25.50 * SupplierID + 500.00)
-- 2. Shipping Fleet: Budget is a flat fee per entity plus a legacy equipment adjustment (y = 0 * x + 1200.00)
DECLARE @FacilityAgeScalar MONEY = 25.50;
DECLARE @FacilityBaseRenovation MONEY = 500.00;
DECLARE @FleetFixedBudget MONEY = 1200.00;

-- Cursor combining DIFFERENT tables (Suppliers and Shippers) via UNION ALL
DECLARE MaintenanceCursor CURSOR FOR 
    -- Branch 1: Suppliers (Production Facilities)
    -- Selection: Only suppliers located in the USA (Domestic infrastructure focus)
    -- Transformation: Linear scale based on ID plus base maintenance fee (y = 25.50 * x + 500.00)
    SELECT 
        'ProductionFacility' AS AssetClass, 
        SupplierID AS SourcePKID, 
        CompanyName AS AssetName, 
        (CAST(SupplierID AS MONEY) * @FacilityAgeScalar) + @FacilityBaseRenovation AS AllocatedBudget 
    FROM Suppliers 
    WHERE Country = 'USA' -- Selection
    
    UNION ALL

    -- Branch 2: Shippers (Logistics Fleet)
    -- Selection: All shippers (Essential infrastructure)
    -- Transformation: Flat linear projection (y = 0 * x + 1200.00)
    SELECT 
        'LogisticsFleet' AS AssetClass, 
        ShipperID AS SourcePKID, 
        CompanyName AS AssetName, 
        @FleetFixedBudget AS AllocatedBudget 
    FROM Shippers;

OPEN MaintenanceCursor;
FETCH NEXT FROM MaintenanceCursor INTO @v_AssetClass, @v_SourcePKID, @v_AssetName, @v_AllocatedBudget;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextMaintenanceID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the projected, filtered, and linearly transformed record
    INSERT INTO Table_InfrastructureMaintenanceLedger (MaintenanceID, AssetClass, SourcePKID, AssetName, AllocatedBudget)
    VALUES (@nextMaintenanceID, @v_AssetClass, @v_SourcePKID, @v_AssetName, @v_AllocatedBudget);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AssetClass = 'ProductionFacility'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', CAST(@v_SourcePKID AS VARCHAR), 'Table_InfrastructureMaintenanceLedger', 'MaintenanceID', CAST(@nextMaintenanceID AS VARCHAR));
    END
    ELSE IF @v_AssetClass = 'LogisticsFleet'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', CAST(@v_SourcePKID AS VARCHAR), 'Table_InfrastructureMaintenanceLedger', 'MaintenanceID', CAST(@nextMaintenanceID AS VARCHAR));
    END
    
    FETCH NEXT FROM MaintenanceCursor INTO @v_AssetClass, @v_SourcePKID, @v_AssetName, @v_AllocatedBudget;
END;

CLOSE MaintenanceCursor; 
DEALLOCATE MaintenanceCursor;
GO