-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified Cross-Domain Transactional "Super-Ledger".
-- Rule: Combine inputs via UNION ALL (Merging Product-Inventory and Order-Logistics domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to create normalized metrics across disparate sources.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedMarketActivityLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedMarketActivityLedger;
CREATE TABLE Table_UnifiedMarketActivityLedger (
    UnifiedAuditID INT,
    SourceEntityID NVARCHAR(20), -- Can hold ProductID or OrderID
    EntityType NVARCHAR(20),     -- Discriminator: 'PRODUCT' or 'ORDER'
    EntityName NVARCHAR(60),     -- ProductName or ShipName
    EconomicMass MONEY,          -- Non-Linear Transformation: (UnitPrice * UnitsInStock) OR (Freight * 10.0)
    ComplexityEntropy FLOAT,     -- Non-Linear Transformation: SQRT(ID * Value)
    NormalizedMomentum FLOAT     -- Non-Linear Transformation: LOG(Value + 2.0)
);
GO

DECLARE @v_SourceEntityID NVARCHAR(20),
        @v_EntityType NVARCHAR(20),
        @v_EntityName NVARCHAR(60),
        @v_EconomicMass MONEY,
        @v_ComplexityEntropy FLOAT,
        @v_NormalizedMomentum FLOAT,
        @nextUnifiedAuditID INT;

-- Cursor using UNION ALL to stack records from Products and Orders into a single stream.
-- Selection: Products with price > 20 and Orders with Freight > 50.
-- Transformations (Non-Linear):
-- 1. Economic Mass: For products (Price * Stock); For orders (Freight scaled by 10).
-- 2. Complexity Entropy: Square root of the identifier multiplied by the financial value.
-- 3. Normalized Momentum: Logarithmic scaling of the core financial metric.
DECLARE UnifiedCursor CURSOR FOR 
    -- Segment A: Product Inventory Domain
    SELECT 
        CAST(ProductID AS NVARCHAR(20)) AS SourceEntityID,
        'PRODUCT' AS EntityType,
        ProductName AS EntityName,
        (UnitPrice * UnitsInStock) AS EconomicMass, -- A' = A * B
        SQRT(CAST(ProductID AS FLOAT) * CAST(UnitPrice AS FLOAT)) AS ComplexityEntropy,
        LOG(CAST(UnitPrice AS FLOAT) + 2.0) AS NormalizedMomentum
    FROM Products
    WHERE UnitPrice > 20.00
    
    UNION ALL

    -- Segment B: Order Logistics Domain
    SELECT 
        CAST(OrderID AS NVARCHAR(20)) AS SourceEntityID,
        'ORDER' AS EntityType,
        ShipName AS EntityName,
        (Freight * 10.0) AS EconomicMass, -- Scaled linear-to-non-linear proxy
        SQRT(CAST(OrderID AS FLOAT) * CAST(Freight AS FLOAT)) AS ComplexityEntropy,
        LOG(CAST(Freight AS FLOAT) + 2.0) AS NormalizedMomentum
    FROM Orders
    WHERE Freight > 50.00;

OPEN UnifiedCursor;
FETCH NEXT FROM UnifiedCursor INTO 
    @v_SourceEntityID, @v_EntityType, @v_EntityName, @v_EconomicMass, @v_ComplexityEntropy, @v_NormalizedMomentum;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextUnifiedAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedMarketActivityLedger (
        UnifiedAuditID, SourceEntityID, EntityType, EntityName, 
        EconomicMass, ComplexityEntropy, NormalizedMomentum
    )
    VALUES (
        @nextUnifiedAuditID, @v_SourceEntityID, @v_EntityType, @v_EntityName, 
        @v_EconomicMass, @v_ComplexityEntropy, @v_NormalizedMomentum
    );

    -- Log Single-Source Lineage based on the EntityType discriminator
    IF @v_EntityType = 'PRODUCT'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', @v_SourceEntityID, 'Table_UnifiedMarketActivityLedger', 'UnifiedAuditID', CAST(@nextUnifiedAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', @v_SourceEntityID, 'Table_UnifiedMarketActivityLedger', 'UnifiedAuditID', CAST(@nextUnifiedAuditID AS VARCHAR));
    
    FETCH NEXT FROM UnifiedCursor INTO 
        @v_SourceEntityID, @v_EntityType, @v_EntityName, @v_EconomicMass, @v_ComplexityEntropy, @v_NormalizedMomentum;
END;

CLOSE UnifiedCursor; 
DEALLOCATE UnifiedCursor;
GO