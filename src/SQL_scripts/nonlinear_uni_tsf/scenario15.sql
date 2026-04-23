-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Asset-Resilience" & Strategic Maintenance Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Product-Inventory and Employee-Resource domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to create normalized lifecycle and durability metrics.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedAssetResilienceLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedAssetResilienceLedger;
CREATE TABLE Table_UnifiedAssetResilienceLedger (
    ResilienceAuditID INT,
    AssetKey NVARCHAR(20),       -- ProductID or EmployeeID
    AssetClass NVARCHAR(20),      -- Discriminator: 'INVENTORY_STOCK' or 'HUMAN_RESOURCE'
    AssetDescriptor NVARCHAR(60), -- ProductName or LastName
    DurabilityMass FLOAT,         -- Non-Linear Transformation: (ID * 1.7) / LOG(ID + 4.0)
    FrictionCurvature FLOAT,      -- Non-Linear Transformation: SQRT(CAST(ID AS FLOAT) * 95.0)
    MaintenanceExponent FLOAT     -- Non-Linear Transformation: POWER(CAST(ID AS FLOAT), 0.82)
);
GO

DECLARE @v_AssetKey NVARCHAR(20),
        @v_AssetClass NVARCHAR(20),
        @v_AssetDescriptor NVARCHAR(60),
        @v_DurabilityMass FLOAT,
        @v_FrictionCurvature FLOAT,
        @v_MaintenanceExponent FLOAT,
        @nextResilienceAuditID INT;

-- Cursor using UNION ALL to integrate physical inventory assets and human resource assets into a single reliability model.
-- Selection: Products that are currently discontinued (Legacy Assets) and Employees with Title 'Sales Representative'.
-- Transformations (Non-Linear):
-- 1. Durability Mass: Models the "hardiness" of the asset node relative to its ID weight (A' = A * B / log(A)).
-- 2. Friction Curvature: Square root scaling to proxy the operational resistance or maintenance cost of the asset.
-- 3. Maintenance Exponent: A power function (x^0.82) to estimate the complexity of sustaining the asset over time.
DECLARE ResilienceUnionCursor CURSOR FOR 
    -- Segment A: Inventory Stock Domain (Discontinued Products)
    SELECT 
        CAST(ProductID AS NVARCHAR(20)) AS AssetKey,
        'INVENTORY_STOCK' AS AssetClass,
        ProductName AS AssetDescriptor,
        (CAST(ProductID AS FLOAT) * 1.7) / LOG(CAST(ProductID AS FLOAT) + 4.0) AS DurabilityMass,
        SQRT(CAST(ProductID AS FLOAT) * 95.0) AS FrictionCurvature,
        POWER(CAST(ProductID AS FLOAT), 0.82) AS MaintenanceExponent
    FROM Products
    WHERE Discontinued = 1
    
    UNION ALL

    -- Segment B: Human Resource Domain (Staff)
    SELECT 
        CAST(EmployeeID AS NVARCHAR(20)) AS AssetKey,
        'HUMAN_RESOURCE' AS AssetClass,
        LastName AS AssetDescriptor,
        (CAST(EmployeeID AS FLOAT) * 1.7) / LOG(CAST(EmployeeID AS FLOAT) + 4.0) AS DurabilityMass,
        SQRT(CAST(EmployeeID AS FLOAT) * 95.0) AS FrictionCurvature,
        POWER(CAST(EmployeeID AS FLOAT), 0.82) AS MaintenanceExponent
    FROM Employees
    WHERE Title = 'Sales Representative';

OPEN ResilienceUnionCursor;
FETCH NEXT FROM ResilienceUnionCursor INTO 
    @v_AssetKey, @v_AssetClass, @v_AssetDescriptor, @v_DurabilityMass, @v_FrictionCurvature, @v_MaintenanceExponent;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextResilienceAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedAssetResilienceLedger (
        ResilienceAuditID, AssetKey, AssetClass, AssetDescriptor, 
        DurabilityMass, FrictionCurvature, MaintenanceExponent
    )
    VALUES (
        @nextResilienceAuditID, @v_AssetKey, @v_AssetClass, @v_AssetDescriptor, 
        @v_DurabilityMass, @v_FrictionCurvature, @v_MaintenanceExponent
    );

    -- Log Single-Source Lineage based on the AssetClass discriminator
    IF @v_AssetClass = 'INVENTORY_STOCK'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', @v_AssetKey, 'Table_UnifiedAssetResilienceLedger', 'ResilienceAuditID', CAST(@nextResilienceAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_AssetKey, 'Table_UnifiedAssetResilienceLedger', 'ResilienceAuditID', CAST(@nextResilienceAuditID AS VARCHAR));
    
    FETCH NEXT FROM ResilienceUnionCursor INTO 
        @v_AssetKey, @v_AssetClass, @v_AssetDescriptor, @v_DurabilityMass, @v_FrictionCurvature, @v_MaintenanceExponent;
END;

CLOSE ResilienceUnionCursor; 
DEALLOCATE ResilienceUnionCursor;
GO