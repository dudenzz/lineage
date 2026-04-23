-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Channel-Impact" & Growth Acceleration Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Customer-Market and Employee-Sales domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to harmonize engagement and momentum scores.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedChannelImpactLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedChannelImpactLedger;
CREATE TABLE Table_UnifiedChannelImpactLedger (
    ImpactAuditID INT,
    ChannelSourceKey NVARCHAR(20), -- CustomerID or EmployeeID
    ChannelType NVARCHAR(20),      -- Discriminator: 'EXTERNAL_CLIENT' or 'INTERNAL_AGENT'
    ChannelLabel NVARCHAR(60),     -- CompanyName or LastName
    EngagementMass FLOAT,          -- Non-Linear Transformation: SQRT(LEN(ID) * 50.0)
    MomentumCurvature FLOAT,       -- Non-Linear Transformation: POWER(CAST(LEN(Name) AS FLOAT), 1.3)
    StabilityLogarithm FLOAT       -- Non-Linear Transformation: LOG(CAST(LEN(Name) AS FLOAT) + 5.0)
);
GO

DECLARE @v_ChannelSourceKey NVARCHAR(20),
        @v_ChannelType NVARCHAR(20),
        @v_ChannelLabel NVARCHAR(60),
        @v_EngagementMass FLOAT,
        @v_MomentumCurvature FLOAT,
        @v_StabilityLogarithm FLOAT,
        @nextImpactAuditID INT;

-- Cursor using UNION ALL to bridge external customer demand and internal employee sales capacity.
-- Selection: Customers from 'Brazil' or 'Venezuela' (South American Market) and Employees with Title 'Sales Representative'.
-- Transformations (Non-Linear):
-- 1. Engagement Mass: Square root of the ID length to proxy the structural weight of the channel.
-- 2. Momentum Curvature: Exponential scaling based on name length to model communicative reach.
-- 3. Stability Logarithm: Logarithmic normalization to assess the consistency of the channel node.
DECLARE ChannelUnionCursor CURSOR FOR 
    -- Segment A: External Client Domain
    SELECT 
        CAST(CustomerID AS NVARCHAR(20)) AS ChannelSourceKey,
        'EXTERNAL_CLIENT' AS ChannelType,
        CompanyName AS ChannelLabel,
        SQRT(CAST(LEN(CustomerID) AS FLOAT) * 50.0) AS EngagementMass,
        POWER(CAST(LEN(CompanyName) AS FLOAT), 1.3) AS MomentumCurvature,
        LOG(CAST(LEN(CompanyName) AS FLOAT) + 5.0) AS StabilityLogarithm
    FROM Customers
    WHERE Country IN ('Brazil', 'Venezuela')
    
    UNION ALL

    -- Segment B: Internal Sales Agent Domain
    SELECT 
        CAST(EmployeeID AS NVARCHAR(20)) AS ChannelSourceKey,
        'INTERNAL_AGENT' AS ChannelType,
        LastName AS ChannelLabel,
        SQRT(CAST(LEN(CAST(EmployeeID AS NVARCHAR)) AS FLOAT) * 50.0) AS EngagementMass,
        POWER(CAST(LEN(LastName) AS FLOAT), 1.3) AS MomentumCurvature,
        LOG(CAST(LEN(LastName) AS FLOAT) + 5.0) AS StabilityLogarithm
    FROM Employees
    WHERE Title = 'Sales Representative';

OPEN ChannelUnionCursor;
FETCH NEXT FROM ChannelUnionCursor INTO 
    @v_ChannelSourceKey, @v_ChannelType, @v_ChannelLabel, @v_EngagementMass, @v_MomentumCurvature, @v_StabilityLogarithm;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextImpactAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedChannelImpactLedger (
        ImpactAuditID, ChannelSourceKey, ChannelType, ChannelLabel, 
        EngagementMass, MomentumCurvature, StabilityLogarithm
    )
    VALUES (
        @nextImpactAuditID, @v_ChannelSourceKey, @v_ChannelType, @v_ChannelLabel, 
        @v_EngagementMass, @v_MomentumCurvature, @v_StabilityLogarithm
    );

    -- Log Single-Source Lineage based on the ChannelType discriminator
    IF @v_ChannelType = 'EXTERNAL_CLIENT'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_ChannelSourceKey, 'Table_UnifiedChannelImpactLedger', 'ImpactAuditID', CAST(@nextImpactAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_ChannelSourceKey, 'Table_UnifiedChannelImpactLedger', 'ImpactAuditID', CAST(@nextImpactAuditID AS VARCHAR));
    
    FETCH NEXT FROM ChannelUnionCursor INTO 
        @v_ChannelSourceKey, @v_ChannelType, @v_ChannelLabel, @v_EngagementMass, @v_MomentumCurvature, @v_StabilityLogarithm;
END;

CLOSE ChannelUnionCursor; 
DEALLOCATE ChannelUnionCursor;
GO