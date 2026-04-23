-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Asset-Resilience" & Structural Durability Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Product-Inventory and Employee-Legacy domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to create normalized lifecycle and durability metrics.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedLifecycleResilienceLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedLifecycleResilienceLedger;
CREATE TABLE Table_UnifiedLifecycleResilienceLedger (
    ResilienceAuditID INT,
    ReferenceKey NVARCHAR(20),  -- ProductID or EmployeeID
    AssetClass NVARCHAR(20),     -- Discriminator: 'PRODUCT_ASSET' or 'PERSONNEL_ASSET'
    AssetLabel NVARCHAR(60),     -- ProductName or LastName
    DurabilityMass FLOAT,        -- Non-Linear Transformation: (ID * 1.85) / LOG(ID + 3.0)
    FrictionCurvature FLOAT,    -- Non-Linear Transformation: SQRT(CAST(ID AS FLOAT) * 130.0)
    SustainmentExponent FLOAT    -- Non-Linear Transformation: POWER(CAST(ID AS FLOAT), 0.72)
);
GO

DECLARE @v_ReferenceKey NVARCHAR(20),
        @v_AssetClass NVARCHAR(20),
        @v_AssetLabel NVARCHAR(60),
        @v_DurabilityMass FLOAT,
        @v_FrictionCurvature FLOAT,
        @v_SustainmentExponent FLOAT,
        @nextResilienceAuditID INT;

-- Cursor using UNION ALL to harmonize physical hardware assets and human capital into a unified durability model.
-- Selection: Products with UnitsInStock > 50 and Employees hired before 1993 (Legacy Staff).
-- Transformations (Non-Linear):
-- 1. Durability Mass: Models node "hardiness" relative to its ID weight (A' = A * B / log(A)).
-- 2. Friction Curvature: Square root scaling to proxy the operational "drag" or maintenance cost of the asset.
-- 3. Sustainment Exponent: A power function (x^0.72) to estimate the complexity of maintaining the node.
DECLARE ResilienceUnionCursor CURSOR FOR 
    -- Segment A: Product Asset Domain (High-Stock Inventory)
    SELECT 
        CAST(ProductID AS NVARCHAR(20)) AS ReferenceKey,
        'PRODUCT_ASSET' AS AssetClass,
        ProductName AS AssetLabel,
        (CAST(ProductID AS FLOAT) * 1.85) / LOG(CAST(ProductID AS FLOAT) + 3.0) AS DurabilityMass,
        SQRT(CAST(ProductID AS FLOAT) * 130.0) AS FrictionCurvature,
        POWER(CAST(ProductID AS FLOAT), 0.72) AS SustainmentExponent
    FROM Products
    WHERE UnitsInStock > 50
    
    UNION ALL

    -- Segment B: Personnel Asset Domain (Legacy Employee Base)
    SELECT 
        CAST(EmployeeID AS NVARCHAR(20)) AS ReferenceKey,
        'PERSONNEL_ASSET' AS AssetClass,
        LastName AS AssetLabel,
        (CAST(EmployeeID AS FLOAT) * 1.85) / LOG(CAST(EmployeeID AS FLOAT) + 3.0) AS DurabilityMass,
        SQRT(CAST(EmployeeID AS FLOAT) * 130.0) AS FrictionCurvature,
        POWER(CAST(EmployeeID AS FLOAT), 0.72) AS SustainmentExponent
    FROM Employees
    WHERE HireDate < '1993-01-01';

OPEN ResilienceUnionCursor;
FETCH NEXT FROM ResilienceUnionCursor INTO 
    @v_ReferenceKey, @v_AssetClass, @v_AssetLabel, @v_DurabilityMass, @v_FrictionCurvature, @v_SustainmentExponent;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextResilienceAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedLifecycleResilienceLedger (
        ResilienceAuditID, ReferenceKey, AssetClass, AssetLabel, 
        DurabilityMass, FrictionCurvature, SustainmentExponent
    )
    VALUES (
        @nextResilienceAuditID, @v_ReferenceKey, @v_AssetClass, @v_AssetLabel, 
        @v_DurabilityMass, @v_FrictionCurvature, @v_SustainmentExponent
    );

    -- Log Single-Source Lineage based on the AssetClass discriminator
    IF @v_AssetClass = 'PRODUCT_ASSET'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', @v_ReferenceKey, 'Table_UnifiedLifecycleResilienceLedger', 'ResilienceAuditID', CAST(@nextResilienceAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_ReferenceKey, 'Table_UnifiedLifecycleResilienceLedger', 'ResilienceAuditID', CAST(@nextResilienceAuditID AS VARCHAR));
    
    FETCH NEXT FROM ResilienceUnionCursor INTO 
        @v_ReferenceKey, @v_AssetClass, @v_AssetLabel, @v_DurabilityMass, @v_FrictionCurvature, @v_SustainmentExponent;
END;

CLOSE ResilienceUnionCursor; 
DEALLOCATE ResilienceUnionCursor;
GO