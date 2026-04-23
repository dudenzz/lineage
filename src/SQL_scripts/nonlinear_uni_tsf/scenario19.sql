-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Channel-Impact" & Growth Acceleration Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Customer-Market and Employee-Sales domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to harmonize engagement and momentum scores.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedGrowthVelocityLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedGrowthVelocityLedger;
CREATE TABLE Table_UnifiedGrowthVelocityLedger (
    VelocityAuditID INT,
    SourceInstanceID NVARCHAR(20), -- CustomerID or EmployeeID
    NodeDomain NVARCHAR(20),       -- Discriminator: 'EXTERNAL_CLIENT' or 'INTERNAL_AGENT'
    NodeIdentity NVARCHAR(60),     -- CompanyName or LastName
    PotentialMass FLOAT,           -- Non-Linear Transformation: (ID_Weight * 2.15)
    StructuralComplexity FLOAT,    -- Non-Linear Transformation: SQRT(ID_Weight * 120.0)
    MomentumLogarithm FLOAT        -- Non-Linear Transformation: LOG(ID_Weight + 3.5)
);
GO

DECLARE @v_SourceInstanceID NVARCHAR(20),
        @v_NodeDomain NVARCHAR(20),
        @v_NodeIdentity NVARCHAR(60),
        @v_PotentialMass FLOAT,
        @v_StructuralComplexity FLOAT,
        @v_MomentumLogarithm FLOAT,
        @nextVelocityAuditID INT;

-- Cursor using UNION ALL to bridge external customer demand and internal employee sales capacity.
-- Selection: Customers from 'Brazil' or 'Venezuela' and Employees with Title 'Sales Representative'.
-- Transformations (Non-Linear):
-- 1. Potential Mass: Models the growth "weight" of the node relative to its internal ID or length (A' = A * B).
-- 2. Structural Complexity: Square root scaling to proxy the administrative "drag" associated with the node.
-- 3. Momentum Logarithm: Logarithmic normalization to identify high-velocity nodes across domains.
DECLARE VelocityUnionCursor CURSOR FOR 
    -- Segment A: External Client Domain
    SELECT 
        CAST(CustomerID AS NVARCHAR(20)) AS SourceInstanceID,
        'EXTERNAL_CLIENT' AS NodeDomain,
        CompanyName AS NodeIdentity,
        (CAST(LEN(CustomerID) AS FLOAT) * 2.15) AS PotentialMass,
        SQRT(CAST(LEN(CustomerID) AS FLOAT) * 120.0) AS StructuralComplexity,
        LOG(CAST(LEN(CustomerID) AS FLOAT) + 3.5) AS MomentumLogarithm
    FROM Customers
    WHERE Country IN ('Brazil', 'Venezuela')
    
    UNION ALL

    -- Segment B: Internal Sales Agent Domain
    SELECT 
        CAST(EmployeeID AS NVARCHAR(20)) AS SourceInstanceID,
        'INTERNAL_AGENT' AS NodeDomain,
        LastName AS NodeIdentity,
        (CAST(EmployeeID AS FLOAT) * 2.15) AS PotentialMass,
        SQRT(CAST(EmployeeID AS FLOAT) * 120.0) AS StructuralComplexity,
        LOG(CAST(EmployeeID AS FLOAT) + 3.5) AS MomentumLogarithm
    FROM Employees
    WHERE Title = 'Sales Representative';

OPEN VelocityUnionCursor;
FETCH NEXT FROM VelocityUnionCursor INTO 
    @v_SourceInstanceID, @v_NodeDomain, @v_NodeIdentity, @v_PotentialMass, @v_StructuralComplexity, @v_MomentumLogarithm;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextVelocityAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedGrowthVelocityLedger (
        VelocityAuditID, SourceInstanceID, NodeDomain, NodeIdentity, 
        PotentialMass, StructuralComplexity, MomentumLogarithm
    )
    VALUES (
        @nextVelocityAuditID, @v_SourceInstanceID, @v_NodeDomain, @v_NodeIdentity, 
        @v_PotentialMass, @v_StructuralComplexity, @v_MomentumLogarithm
    );

    -- Log Single-Source Lineage based on the NodeDomain discriminator
    IF @v_NodeDomain = 'EXTERNAL_CLIENT'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_SourceInstanceID, 'Table_UnifiedGrowthVelocityLedger', 'VelocityAuditID', CAST(@nextVelocityAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_SourceInstanceID, 'Table_UnifiedGrowthVelocityLedger', 'VelocityAuditID', CAST(@nextVelocityAuditID AS VARCHAR));
    
    FETCH NEXT FROM VelocityUnionCursor INTO 
        @v_SourceInstanceID, @v_NodeDomain, @v_NodeIdentity, @v_PotentialMass, @v_StructuralComplexity, @v_MomentumLogarithm;
END;

CLOSE VelocityUnionCursor; 
DEALLOCATE VelocityUnionCursor;
GO