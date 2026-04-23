-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Asset-Resilience" & Structural Durability Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Product-Inventory and Employee-Legacy domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to create normalized lifecycle and durability metrics.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedStructuralDurabilityLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedStructuralDurabilityLedger;
CREATE TABLE Table_UnifiedStructuralDurabilityLedger (
    DurabilityAuditID INT,
    ReferenceKey NVARCHAR(20),  -- ProductID or EmployeeID
    AssetClass NVARCHAR(20),     -- Discriminator: 'HARDWARE_STOCK' or 'HUMAN_CAPITAL'
    AssetLabel NVARCHAR(60),     -- ProductName or LastName
    ResilienceMass FLOAT,        -- Non-Linear Transformation: (ID * 1.4) / LOG(ID + 2.5)
    StructuralFriction FLOAT,    -- Non-Linear Transformation: SQRT(CAST(ID AS FLOAT) * 115.0)
    SustainmentExponent FLOAT    -- Non-Linear Transformation: POWER(CAST(ID AS FLOAT), 0.74)
);
GO

DECLARE @v_ReferenceKey NVARCHAR(20),
        @v_AssetClass NVARCHAR(20),
        @v_AssetLabel NVARCHAR(60),
        @v_ResilienceMass FLOAT,
        @v_StructuralFriction FLOAT,
        @v_SustainmentExponent FLOAT,
        @nextDurabilityAuditID INT;

-- Cursor using UNION ALL to integrate physical stock resilience and organizational human capital into a unified durability model.
-- Selection: Products with UnitPrice between 10 and 50 (Mid-tier) and Employees with Title 'Sales Representative'.
-- Transformations (Non-Linear):
-- 1. Resilience Mass: Models the "hardiness" of the asset node relative to its ID weight (A' = A * B / log(A)).
-- 2. Structural Friction: Square root scaling to proxy the operational "drag" or maintenance cost of the asset.
-- 3. Sustainment Exponent: A power function (x^0.74) to estimate the complexity of maintaining the node in the long term.
DECLARE DurabilityUnionCursor CURSOR FOR 
    -- Segment A: Hardware Stock Domain (Inventory)
    SELECT 
        CAST(ProductID AS NVARCHAR(20)) AS ReferenceKey,
        'HARDWARE_STOCK' AS AssetClass,
        ProductName AS AssetLabel,
        (CAST(ProductID AS FLOAT) * 1.4) / LOG(CAST(ProductID AS FLOAT) + 2.5) AS ResilienceMass,
        SQRT(CAST(ProductID AS FLOAT) * 115.0) AS StructuralFriction,
        POWER(CAST(ProductID AS FLOAT), 0.74) AS SustainmentExponent
    FROM Products
    WHERE UnitPrice BETWEEN 10.00 AND 50.00
    
    UNION ALL

    -- Segment B: Human Capital Domain (Staff)
    SELECT 
        CAST(EmployeeID AS NVARCHAR(20)) AS ReferenceKey,
        'HUMAN_CAPITAL' AS AssetClass,
        LastName AS AssetLabel,
        (CAST(EmployeeID AS FLOAT) * 1.4) / LOG(CAST(EmployeeID AS FLOAT) + 2.5) AS ResilienceMass,
        SQRT(CAST(EmployeeID AS FLOAT) * 115.0) AS StructuralFriction,
        POWER(CAST(EmployeeID AS FLOAT), 0.74) AS SustainmentExponent
    FROM Employees
    WHERE Title = 'Sales Representative';

OPEN DurabilityUnionCursor;
FETCH NEXT FROM DurabilityUnionCursor INTO 
    @v_ReferenceKey, @v_AssetClass, @v_AssetLabel, @v_ResilienceMass, @v_StructuralFriction, @v_SustainmentExponent;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextDurabilityAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedStructuralDurabilityLedger (
        DurabilityAuditID, ReferenceKey, AssetClass, AssetLabel, 
        ResilienceMass, StructuralFriction, SustainmentExponent
    )
    VALUES (
        @nextDurabilityAuditID, @v_ReferenceKey, @v_AssetClass, @v_AssetLabel, 
        @v_ResilienceMass, @v_StructuralFriction, @v_SustainmentExponent
    );

    -- Log Single-Source Lineage based on the AssetClass discriminator
    IF @v_AssetClass = 'HARDWARE_STOCK'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', @v_ReferenceKey, 'Table_UnifiedStructuralDurabilityLedger', 'DurabilityAuditID', CAST(@nextDurabilityAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_ReferenceKey, 'Table_UnifiedStructuralDurabilityLedger', 'DurabilityAuditID', CAST(@nextDurabilityAuditID AS VARCHAR));
    
    FETCH NEXT FROM DurabilityUnionCursor INTO 
        @v_ReferenceKey, @v_AssetClass, @v_AssetLabel, @v_ResilienceMass, @v_StructuralFriction, @v_SustainmentExponent;
END;

CLOSE DurabilityUnionCursor; 
DEALLOCATE DurabilityUnionCursor;
GO