-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Geographic-Economic" Node & Risk Exposure Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Territory-Region and Supplier-Country domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to create normalized risk and potential metrics.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedGeographicEntropyLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedGeographicEntropyLedger;
CREATE TABLE Table_UnifiedGeographicEntropyLedger (
    GeoAuditID INT,
    ReferenceKey NVARCHAR(20),  -- TerritoryID or SupplierID
    NodeSource NVARCHAR(20),    -- Discriminator: 'TERRITORY_NODE' or 'SUPPLIER_NODE'
    LocationLabel NVARCHAR(60), -- TerritoryDescription or CompanyName
    EconomicDensity FLOAT,      -- Non-Linear Transformation: (CAST(ID AS FLOAT) * 0.8) / LOG(CAST(ID AS FLOAT) + 2.0)
    ComplexityFriction FLOAT,   -- Non-Linear Transformation: SQRT(CAST(ID AS FLOAT) * 45.0)
    GrowthPotentialExponent FLOAT -- Non-Linear Transformation: POWER(CAST(ID AS FLOAT), 0.72)
);
GO

DECLARE @v_ReferenceKey NVARCHAR(20),
        @v_NodeSource NVARCHAR(20),
        @v_LocationLabel NVARCHAR(60),
        @v_EconomicDensity FLOAT,
        @v_ComplexityFriction FLOAT,
        @v_GrowthPotentialExponent FLOAT,
        @nextGeoAuditID INT;

-- Cursor using UNION ALL to integrate geographic territories and global supplier nodes into a single risk map.
-- Selection: Territories in Region 2 (Central) and Suppliers from 'Germany', 'France', or 'Italy'.
-- Transformations (Non-Linear):
-- 1. Economic Density: A ratio of identifier weight to logarithmic growth to proxy market maturity.
-- 2. Complexity Friction: Square root scaling of the identifier to model administrative overhead.
-- 3. Growth Potential: A power function to estimate the "explosive" potential of the node.
DECLARE GeoUnionCursor CURSOR FOR 
    -- Segment A: Geographic Territory Nodes
    SELECT 
        TerritoryID AS ReferenceKey,
        'TERRITORY_NODE' AS NodeSource,
        TerritoryDescription AS LocationLabel,
        (CAST(ABS(CHECKSUM(TerritoryID) % 100) AS FLOAT) * 0.8) / LOG(CAST(ABS(CHECKSUM(TerritoryID) % 100) AS FLOAT) + 2.0) AS EconomicDensity,
        SQRT(CAST(ABS(CHECKSUM(TerritoryID) % 100) AS FLOAT) * 45.0) AS ComplexityFriction,
        POWER(CAST(ABS(CHECKSUM(TerritoryID) % 100) AS FLOAT), 0.72) AS GrowthPotentialExponent
    FROM Territories
    WHERE RegionID = 2
    
    UNION ALL

    -- Segment B: Global Supplier Nodes
    SELECT 
        CAST(SupplierID AS NVARCHAR(20)) AS ReferenceKey,
        'SUPPLIER_NODE' AS NodeSource,
        CompanyName AS LocationLabel,
        (CAST(SupplierID AS FLOAT) * 0.8) / LOG(CAST(SupplierID AS FLOAT) + 2.0) AS EconomicDensity,
        SQRT(CAST(SupplierID AS FLOAT) * 45.0) AS ComplexityFriction,
        POWER(CAST(SupplierID AS FLOAT), 0.72) AS GrowthPotentialExponent
    FROM Suppliers
    WHERE Country IN ('Germany', 'France', 'Italy');

OPEN GeoUnionCursor;
FETCH NEXT FROM GeoUnionCursor INTO 
    @v_ReferenceKey, @v_NodeSource, @v_LocationLabel, @v_EconomicDensity, @v_ComplexityFriction, @v_GrowthPotentialExponent;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextGeoAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedGeographicEntropyLedger (
        GeoAuditID, ReferenceKey, NodeSource, LocationLabel, 
        EconomicDensity, ComplexityFriction, GrowthPotentialExponent
    )
    VALUES (
        @nextGeoAuditID, @v_ReferenceKey, @v_NodeSource, @v_LocationLabel, 
        @v_EconomicDensity, @v_ComplexityFriction, @v_GrowthPotentialExponent
    );

    -- Log Single-Source Lineage based on the NodeSource discriminator
    IF @v_NodeSource = 'TERRITORY_NODE'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Territories', 'TerritoryID', @v_ReferenceKey, 'Table_UnifiedGeographicEntropyLedger', 'GeoAuditID', CAST(@nextGeoAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_ReferenceKey, 'Table_UnifiedGeographicEntropyLedger', 'GeoAuditID', CAST(@nextGeoAuditID AS VARCHAR));
    
    FETCH NEXT FROM GeoUnionCursor INTO 
        @v_ReferenceKey, @v_NodeSource, @v_LocationLabel, @v_EconomicDensity, @v_ComplexityFriction, @v_GrowthPotentialExponent;
END;

CLOSE GeoUnionCursor; 
DEALLOCATE GeoUnionCursor;
GO