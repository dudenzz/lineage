 -- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Sourcing-Fulfillment" Efficiency & Capacity Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Supplier-Sourcing and Shipper-Logistics domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to harmonize node capacity and network influence.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedCapacityEntropyLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedCapacityEntropyLedger;
CREATE TABLE Table_UnifiedCapacityEntropyLedger (
    CapacityAuditID INT,
    NodeReferenceID NVARCHAR(20), -- SupplierID or ShipperID
    NodeDomain NVARCHAR(20),      -- Discriminator: 'SUPPLY_ORIGIN' or 'LOGISTICS_HUB'
    NodeDescriptor NVARCHAR(60),  -- CompanyName
    StructuralInertia FLOAT,      -- Non-Linear Transformation: (CAST(ID AS FLOAT) * 2.2) / LOG(CAST(ID AS FLOAT) + 3.0)
    NetworkCurvature FLOAT,       -- Non-Linear Transformation: SQRT(CAST(ID AS FLOAT) * 75.0)
    SystemicResilienceScore FLOAT -- Non-Linear Transformation: POWER(CAST(ID AS FLOAT), 0.65)
);
GO

DECLARE @v_NodeReferenceID NVARCHAR(20),
        @v_NodeDomain NVARCHAR(20),
        @v_NodeDescriptor NVARCHAR(60),
        @v_StructuralInertia FLOAT,
        @v_NetworkCurvature FLOAT,
        @v_SystemicResilienceScore FLOAT,
        @nextCapacityAuditID INT;

-- Cursor using UNION ALL to harmonize external supply origins and logistics fulfillment hubs into a unified capacity map.
-- Selection: Suppliers from 'Pacific Rim' (Japan, Australia, Singapore) and Shippers 1 through 3.
-- Transformations (Non-Linear):
-- 1. Structural Inertia: Models the "weight" of a node based on its ID relative to a logarithmic growth cap.
-- 2. Network Curvature: Square root scaling of the node ID to proxy the geographic reach and complexity.
-- 3. Systemic Resilience: A power function (A' = A ^ 0.65) to estimate the node's ability to absorb shock.
DECLARE CapacityUnionCursor CURSOR FOR 
    -- Segment A: Supply Origin Domain (Suppliers)
    SELECT 
        CAST(SupplierID AS NVARCHAR(20)) AS NodeReferenceID,
        'SUPPLY_ORIGIN' AS NodeDomain,
        CompanyName AS NodeDescriptor,
        (CAST(SupplierID AS FLOAT) * 2.2) / LOG(CAST(SupplierID AS FLOAT) + 3.0) AS StructuralInertia,
        SQRT(CAST(SupplierID AS FLOAT) * 75.0) AS NetworkCurvature,
        POWER(CAST(SupplierID AS FLOAT), 0.65) AS SystemicResilienceScore
    FROM Suppliers
    WHERE Country IN ('Japan', 'Australia', 'Singapore')
    
    UNION ALL

    -- Segment B: Logistics Hub Domain (Shippers)
    SELECT 
        CAST(ShipperID AS NVARCHAR(20)) AS NodeReferenceID,
        'LOGISTICS_HUB' AS NodeDomain,
        CompanyName AS NodeDescriptor,
        (CAST(ShipperID AS FLOAT) * 2.2) / LOG(CAST(ShipperID AS FLOAT) + 3.0) AS StructuralInertia,
        SQRT(CAST(ShipperID AS FLOAT) * 75.0) AS NetworkCurvature,
        POWER(CAST(ShipperID AS FLOAT), 0.65) AS SystemicResilienceScore
    FROM Shippers;

OPEN CapacityUnionCursor;
FETCH NEXT FROM CapacityUnionCursor INTO 
    @v_NodeReferenceID, @v_NodeDomain, @v_NodeDescriptor, @v_StructuralInertia, @v_NetworkCurvature, @v_SystemicResilienceScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextCapacityAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedCapacityEntropyLedger (
        CapacityAuditID, NodeReferenceID, NodeDomain, NodeDescriptor, 
        StructuralInertia, NetworkCurvature, SystemicResilienceScore
    )
    VALUES (
        @nextCapacityAuditID, @v_NodeReferenceID, @v_NodeDomain, @v_NodeDescriptor, 
        @v_StructuralInertia, @v_NetworkCurvature, @v_SystemicResilienceScore
    );

    -- Log Single-Source Lineage based on the NodeDomain discriminator
    IF @v_NodeDomain = 'SUPPLY_ORIGIN'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_NodeReferenceID, 'Table_UnifiedCapacityEntropyLedger', 'CapacityAuditID', CAST(@nextCapacityAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', @v_NodeReferenceID, 'Table_UnifiedCapacityEntropyLedger', 'CapacityAuditID', CAST(@nextCapacityAuditID AS VARCHAR));
    
    FETCH NEXT FROM CapacityUnionCursor INTO 
        @v_NodeReferenceID, @v_NodeDomain, @v_NodeDescriptor, @v_StructuralInertia, @v_NetworkCurvature, @v_SystemicResilienceScore;
END;

CLOSE CapacityUnionCursor; 
DEALLOCATE CapacityUnionCursor;
GO