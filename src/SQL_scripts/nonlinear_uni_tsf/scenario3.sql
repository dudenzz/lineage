-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Strategic Asset" Valuation & Risk Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Product Inventory and Logistics Territory domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to harmonize valuation across disparate data types.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedAssetValuationLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedAssetValuationLedger;
CREATE TABLE Table_UnifiedAssetValuationLedger (
    AssetAuditID INT,
    SourceKey NVARCHAR(20),    -- ProductID or TerritoryID
    AssetCategory NVARCHAR(20),-- Discriminator: 'PHYSICAL_GOOD' or 'MARKET_REGION'
    AssetIdentifier NVARCHAR(60), -- ProductName or TerritoryDescription
    CalculatedAssetMass MONEY, -- Non-Linear Transformation: (UnitPrice * UnitsInStock) OR (Length(Name) * 500)
    ComplexityGradient FLOAT,  -- Non-Linear Transformation: SQRT(Value * 1.5)
    StrategicImpactScore FLOAT -- Non-Linear Transformation: LOG(Value + 10.0) / 2.1
);
GO

DECLARE @v_SourceKey NVARCHAR(20),
        @v_AssetCategory NVARCHAR(20),
        @v_AssetIdentifier NVARCHAR(60),
        @v_CalculatedAssetMass MONEY,
        @v_ComplexityGradient FLOAT,
        @v_StrategicImpactScore FLOAT,
        @nextAssetAuditID INT;

-- Cursor using UNION ALL to consolidate physical inventory and geographic territories into a single asset ledger.
-- Selection: Products with substantial stock (>100) and Territories in high-priority Regions (RegionID 1 or 4).
-- Transformations (Non-Linear):
-- 1. Calculated Asset Mass: For products (A' = Price * Stock); For territories (A' = NameLength * 500 proxy).
-- 2. Complexity Gradient: Square root of the mass to model diminishing returns on resource management complexity.
-- 3. Strategic Impact Score: Logarithmic normalization of value to identify core infrastructure assets.
DECLARE AssetUnionCursor CURSOR FOR 
    -- Segment A: Physical Product Assets
    SELECT 
        CAST(ProductID AS NVARCHAR(20)) AS SourceKey,
        'PHYSICAL_GOOD' AS AssetCategory,
        ProductName AS AssetIdentifier,
        CAST((UnitPrice * UnitsInStock) AS MONEY) AS CalculatedAssetMass,
        SQRT(CAST((UnitPrice * UnitsInStock) AS FLOAT) * 1.5) AS ComplexityGradient,
        LOG(CAST((UnitPrice * UnitsInStock) AS FLOAT) + 10.0) / 2.1 AS StrategicImpactScore
    FROM Products
    WHERE UnitsInStock > 100
    
    UNION ALL

    -- Segment B: Geographic Territory Assets
    SELECT 
        TerritoryID AS SourceKey,
        'MARKET_REGION' AS AssetCategory,
        TerritoryDescription AS AssetIdentifier,
        CAST((LEN(TerritoryDescription) * 500.0) AS MONEY) AS CalculatedAssetMass,
        SQRT(CAST((LEN(TerritoryDescription) * 500.0) AS FLOAT) * 1.5) AS ComplexityGradient,
        LOG(CAST((LEN(TerritoryDescription) * 500.0) AS FLOAT) + 10.0) / 2.1 AS StrategicImpactScore
    FROM Territories
    WHERE RegionID IN (1, 4);

OPEN AssetUnionCursor;
FETCH NEXT FROM AssetUnionCursor INTO 
    @v_SourceKey, @v_AssetCategory, @v_AssetIdentifier, @v_CalculatedAssetMass, @v_ComplexityGradient, @v_StrategicImpactScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAssetAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedAssetValuationLedger (
        AssetAuditID, SourceKey, AssetCategory, AssetIdentifier, 
        CalculatedAssetMass, ComplexityGradient, StrategicImpactScore
    )
    VALUES (
        @nextAssetAuditID, @v_SourceKey, @v_AssetCategory, @v_AssetIdentifier, 
        @v_CalculatedAssetMass, @v_ComplexityGradient, @v_StrategicImpactScore
    );

    -- Log Single-Source Lineage based on the AssetCategory discriminator
    IF @v_AssetCategory = 'PHYSICAL_GOOD'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', @v_SourceKey, 'Table_UnifiedAssetValuationLedger', 'AssetAuditID', CAST(@nextAssetAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Territories', 'TerritoryID', @v_SourceKey, 'Table_UnifiedAssetValuationLedger', 'AssetAuditID', CAST(@nextAssetAuditID AS VARCHAR));
    
    FETCH NEXT FROM AssetUnionCursor INTO 
        @v_SourceKey, @v_AssetCategory, @v_AssetIdentifier, @v_CalculatedAssetMass, @v_ComplexityGradient, @v_StrategicImpactScore;
END;

CLOSE AssetUnionCursor; 
DEALLOCATE AssetUnionCursor;
GO