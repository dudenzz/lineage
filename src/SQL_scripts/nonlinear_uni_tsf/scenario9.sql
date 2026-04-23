-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Transaction-Volume" & Fiscal Momentum Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Order-Logistics and Product-Inventory domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to create normalized throughput and mass metrics.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedVolumeMomentumLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedVolumeMomentumLedger;
CREATE TABLE Table_UnifiedVolumeMomentumLedger (
    VolumeAuditID INT,
    SourceIdentifier NVARCHAR(20), -- OrderID or ProductID
    ActivityType NVARCHAR(20),      -- Discriminator: 'LOGISTICS_FLOW' or 'INVENTORY_HOLD'
    PrimaryLabel NVARCHAR(60),      -- ShipName or ProductName
    KineticFiscalMass MONEY,        -- Non-Linear Transformation: (Freight * 5.0) OR (UnitPrice * UnitsInStock)
    OperationalCurvature FLOAT,     -- Non-Linear Transformation: SQRT(Value * 1.8)
    ThroughputLogarithm FLOAT       -- Non-Linear Transformation: LOG(Value + 4.0)
);
GO

DECLARE @v_SourceIdentifier NVARCHAR(20),
        @v_ActivityType NVARCHAR(20),
        @v_PrimaryLabel NVARCHAR(60),
        @v_KineticFiscalMass MONEY,
        @v_OperationalCurvature FLOAT,
        @v_ThroughputLogarithm FLOAT,
        @nextVolumeAuditID INT;

-- Cursor using UNION ALL to harmonize active logistics transit and static inventory capital.
-- Selection: Orders with Freight > 75 and Products with UnitPrice > 30 and UnitsInStock > 0.
-- Transformations (Non-Linear):
-- 1. Kinetic Fiscal Mass: For orders, a 5x scaling of freight (A' = A * B). For products, total stock value.
-- 2. Operational Curvature: Square root of the mass to model the diminishing efficiency of managing larger volumes.
-- 3. Throughput Logarithm: Logarithmic dampening to identify core performance nodes across domains.
DECLARE VolumeUnionCursor CURSOR FOR 
    -- Segment A: Active Logistics Flow (Orders)
    SELECT 
        CAST(OrderID AS NVARCHAR(20)) AS SourceIdentifier,
        'LOGISTICS_FLOW' AS ActivityType,
        ShipName AS PrimaryLabel,
        CAST((Freight * 5.0) AS MONEY) AS KineticFiscalMass,
        SQRT(CAST(Freight AS FLOAT) * 5.0 * 1.8) AS OperationalCurvature,
        LOG(CAST(Freight AS FLOAT) * 5.0 + 4.0) AS ThroughputLogarithm
    FROM Orders
    WHERE Freight > 75.00
    
    UNION ALL

    -- Segment B: Static Inventory Hold (Products)
    SELECT 
        CAST(ProductID AS NVARCHAR(20)) AS SourceIdentifier,
        'INVENTORY_HOLD' AS ActivityType,
        ProductName AS PrimaryLabel,
        CAST((UnitPrice * UnitsInStock) AS MONEY) AS KineticFiscalMass,
        SQRT(CAST((UnitPrice * UnitsInStock) AS FLOAT) * 1.8) AS OperationalCurvature,
        LOG(CAST((UnitPrice * UnitsInStock) AS FLOAT) + 4.0) AS ThroughputLogarithm
    FROM Products
    WHERE UnitPrice > 30.00 AND UnitsInStock > 0;

OPEN VolumeUnionCursor;
FETCH NEXT FROM VolumeUnionCursor INTO 
    @v_SourceIdentifier, @v_ActivityType, @v_PrimaryLabel, @v_KineticFiscalMass, @v_OperationalCurvature, @v_ThroughputLogarithm;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextVolumeAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedVolumeMomentumLedger (
        VolumeAuditID, SourceIdentifier, ActivityType, PrimaryLabel, 
        KineticFiscalMass, OperationalCurvature, ThroughputLogarithm
    )
    VALUES (
        @nextVolumeAuditID, @v_SourceIdentifier, @v_ActivityType, @v_PrimaryLabel, 
        @v_KineticFiscalMass, @v_OperationalCurvature, @v_ThroughputLogarithm
    );

    -- Log Single-Source Lineage based on the ActivityType discriminator
    IF @v_ActivityType = 'LOGISTICS_FLOW'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', @v_SourceIdentifier, 'Table_UnifiedVolumeMomentumLedger', 'VolumeAuditID', CAST(@nextVolumeAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', @v_SourceIdentifier, 'Table_UnifiedVolumeMomentumLedger', 'VolumeAuditID', CAST(@nextVolumeAuditID AS VARCHAR));
    
    FETCH NEXT FROM VolumeUnionCursor INTO 
        @v_SourceIdentifier, @v_ActivityType, @v_PrimaryLabel, @v_KineticFiscalMass, @v_OperationalCurvature, @v_ThroughputLogarithm;
END;

CLOSE VolumeUnionCursor; 
DEALLOCATE VolumeUnionCursor;
GO