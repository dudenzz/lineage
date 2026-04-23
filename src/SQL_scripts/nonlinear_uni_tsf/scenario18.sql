-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Market-Node" Performance & Growth Potential Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Product-Category segments and Geographic-Territory nodes).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to create normalized growth and scale metrics.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedMarketNodeLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedMarketNodeLedger;
CREATE TABLE Table_UnifiedMarketNodeLedger (
    NodeAuditID INT,
    NodeSourceKey NVARCHAR(20),  -- CategoryID or TerritoryID
    NodeClass NVARCHAR(20),      -- Discriminator: 'TAXONOMY_SEGMENT' or 'GEOGRAPHIC_NODE'
    NodeDescriptor NVARCHAR(60), -- CategoryName or TerritoryDescription
    OperationalVolume FLOAT,     -- Non-Linear Transformation: POWER(ID_Weight, 1.25)
    StructuralScalingScore FLOAT, -- Non-Linear Transformation: SQRT(ID_Weight * 25.0)
    ExpansionLogarithm FLOAT     -- Non-Linear Transformation: LOG(ID_Weight + 5.0)
);
GO

DECLARE @v_NodeSourceKey NVARCHAR(20),
        @v_NodeClass NVARCHAR(20),
        @v_NodeDescriptor NVARCHAR(60),
        @v_OperationalVolume FLOAT,
        @v_StructuralScalingScore FLOAT,
        @v_ExpansionLogarithm FLOAT,
        @nextNodeAuditID INT;

-- Cursor using UNION ALL to integrate market category segments and geographic territories into one strategic graph.
-- Selection: Categories 1-8 and Territories in Region 2 or 3.
-- Transformations (Non-Linear):
-- 1. Operational Volume: Exponential growth scaling based on the ID-weight to proxy segment maturity (A' = A^1.25).
-- 2. Structural Scaling: Square root interaction to model organizational footprint expansion.
-- 3. Expansion Logarithm: Logarithmic normalization to project future scalability.
DECLARE NodeUnionCursor CURSOR FOR 
    -- Segment A: Product Category Taxonomy
    SELECT 
        CAST(CategoryID AS NVARCHAR(20)) AS NodeSourceKey,
        'TAXONOMY_SEGMENT' AS NodeClass,
        CategoryName AS NodeDescriptor,
        POWER(CAST(CategoryID AS FLOAT), 1.25) AS OperationalVolume,
        SQRT(CAST(CategoryID AS FLOAT) * 25.0) AS StructuralScalingScore,
        LOG(CAST(CategoryID AS FLOAT) + 5.0) AS ExpansionLogarithm
    FROM Categories
    WHERE CategoryID <= 8
    
    UNION ALL

    -- Segment B: Geographic Territory Nodes
    -- Using the length of the TerritoryID as a numerical weight for the non-linear calc
    SELECT 
        TerritoryID AS NodeSourceKey,
        'GEOGRAPHIC_NODE' AS NodeClass,
        TerritoryDescription AS NodeDescriptor,
        POWER(CAST(LEN(TerritoryID) AS FLOAT), 1.25) AS OperationalVolume,
        SQRT(CAST(LEN(TerritoryID) AS FLOAT) * 25.0) AS StructuralScalingScore,
        LOG(CAST(LEN(TerritoryID) AS FLOAT) + 5.0) AS ExpansionLogarithm
    FROM Territories
    WHERE RegionID IN (2, 3);

OPEN NodeUnionCursor;
FETCH NEXT FROM NodeUnionCursor INTO 
    @v_NodeSourceKey, @v_NodeClass, @v_NodeDescriptor, @v_OperationalVolume, @v_StructuralScalingScore, @v_ExpansionLogarithm;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextNodeAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedMarketNodeLedger (
        NodeAuditID, NodeSourceKey, NodeClass, NodeDescriptor, 
        OperationalVolume, StructuralScalingScore, ExpansionLogarithm
    )
    VALUES (
        @nextNodeAuditID, @v_NodeSourceKey, @v_NodeClass, @v_NodeDescriptor, 
        @v_OperationalVolume, @v_StructuralScalingScore, @v_ExpansionLogarithm
    );

    -- Log Single-Source Lineage based on the NodeClass discriminator
    IF @v_NodeClass = 'TAXONOMY_SEGMENT'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Categories', 'CategoryID', @v_NodeSourceKey, 'Table_UnifiedMarketNodeLedger', 'NodeAuditID', CAST(@nextNodeAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Territories', 'TerritoryID', @v_NodeSourceKey, 'Table_UnifiedMarketNodeLedger', 'NodeAuditID', CAST(@nextNodeAuditID AS VARCHAR));
    
    FETCH NEXT FROM NodeUnionCursor INTO 
        @v_NodeSourceKey, @v_NodeClass, @v_NodeDescriptor, @v_OperationalVolume, @v_StructuralScalingScore, @v_ExpansionLogarithm;
END;

CLOSE NodeUnionCursor; 
DEALLOCATE NodeUnionCursor;
GO