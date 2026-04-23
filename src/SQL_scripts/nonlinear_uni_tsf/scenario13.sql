-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Sourcing-Taxonomy" Structural Complexity Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Supplier-Vendor and Category-Taxonomy domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to create normalized structural and risk metrics.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedStructuralEntropyLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedStructuralEntropyLedger;
CREATE TABLE Table_UnifiedStructuralEntropyLedger (
    EntropyAuditID INT,
    SourceNodeKey NVARCHAR(20),  -- SupplierID or CategoryID
    NodeHierarchy NVARCHAR(20),  -- Discriminator: 'SUPPLY_ORIGIN' or 'MARKET_CLASS'
    NodeIdentifier NVARCHAR(60), -- CompanyName or CategoryName
    OrganizationalMass FLOAT,    -- Non-Linear Transformation: (ID * 3.1) / LOG(ID + 2.0)
    NetworkComplexity FLOAT,     -- Non-Linear Transformation: SQRT(CAST(ID AS FLOAT) * 85.0)
    FragilityExponent FLOAT      -- Non-Linear Transformation: POWER(CAST(ID AS FLOAT), 0.62)
);
GO

DECLARE @v_SourceNodeKey NVARCHAR(20),
        @v_NodeHierarchy NVARCHAR(20),
        @v_NodeIdentifier NVARCHAR(60),
        @v_OrganizationalMass FLOAT,
        @v_NetworkComplexity FLOAT,
        @v_FragilityExponent FLOAT,
        @nextEntropyAuditID INT;

-- Cursor using UNION ALL to integrate supplier sourcing origins and market category classifications.
-- Selection: Suppliers from 'Canada' or 'USA' and Categories 1 through 5.
-- Transformations (Non-Linear):
-- 1. Organizational Mass: Models the "weight" of the node relative to logarithmic growth (A' = A * B / log(A)).
-- 2. Network Complexity: Square root scaling of the identifier to proxy the relational density of the node.
-- 3. Fragility Exponent: A power function (x^0.62) to estimate the vulnerability of the node to systemic shocks.
DECLARE EntropyUnionCursor CURSOR FOR 
    -- Segment A: Supply Origin Domain (Suppliers)
    SELECT 
        CAST(SupplierID AS NVARCHAR(20)) AS SourceNodeKey,
        'SUPPLY_ORIGIN' AS NodeHierarchy,
        CompanyName AS NodeIdentifier,
        (CAST(SupplierID AS FLOAT) * 3.1) / LOG(CAST(SupplierID AS FLOAT) + 2.0) AS OrganizationalMass,
        SQRT(CAST(SupplierID AS FLOAT) * 85.0) AS NetworkComplexity,
        POWER(CAST(SupplierID AS FLOAT), 0.62) AS FragilityExponent
    FROM Suppliers
    WHERE Country IN ('Canada', 'USA')
    
    UNION ALL

    -- Segment B: Market Classification Domain (Categories)
    SELECT 
        CAST(CategoryID AS NVARCHAR(20)) AS SourceNodeKey,
        'MARKET_CLASS' AS NodeHierarchy,
        CategoryName AS NodeIdentifier,
        (CAST(CategoryID AS FLOAT) * 3.1) / LOG(CAST(CategoryID AS FLOAT) + 2.0) AS OrganizationalMass,
        SQRT(CAST(CategoryID AS FLOAT) * 85.0) AS NetworkComplexity,
        POWER(CAST(CategoryID AS FLOAT), 0.62) AS FragilityExponent
    FROM Categories
    WHERE CategoryID <= 5;

OPEN EntropyUnionCursor;
FETCH NEXT FROM EntropyUnionCursor INTO 
    @v_SourceNodeKey, @v_NodeHierarchy, @v_NodeIdentifier, @v_OrganizationalMass, @v_NetworkComplexity, @v_FragilityExponent;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextEntropyAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedStructuralEntropyLedger (
        EntropyAuditID, SourceNodeKey, NodeHierarchy, NodeIdentifier, 
        OrganizationalMass, NetworkComplexity, FragilityExponent
    )
    VALUES (
        @nextEntropyAuditID, @v_SourceNodeKey, @v_NodeHierarchy, @v_NodeIdentifier, 
        @v_OrganizationalMass, @v_NetworkComplexity, @v_FragilityExponent
    );

    -- Log Single-Source Lineage based on the NodeHierarchy discriminator
    IF @v_NodeHierarchy = 'SUPPLY_ORIGIN'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_SourceNodeKey, 'Table_UnifiedStructuralEntropyLedger', 'EntropyAuditID', CAST(@nextEntropyAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Categories', 'CategoryID', @v_SourceNodeKey, 'Table_UnifiedStructuralEntropyLedger', 'EntropyAuditID', CAST(@nextEntropyAuditID AS VARCHAR));
    
    FETCH NEXT FROM EntropyUnionCursor INTO 
        @v_SourceNodeKey, @v_NodeHierarchy, @v_NodeIdentifier, @v_OrganizationalMass, @v_NetworkComplexity, @v_FragilityExponent;
END;

CLOSE EntropyUnionCursor; 
DEALLOCATE EntropyUnionCursor;
GO