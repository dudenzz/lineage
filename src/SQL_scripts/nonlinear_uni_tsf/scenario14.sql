-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Economic-Region" Influence & Market Gravity Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Geographic Regions and Customer Demographics).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to create normalized influence and density metrics.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedMarketGravityLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedMarketGravityLedger;
CREATE TABLE Table_UnifiedMarketGravityLedger (
    GravityAuditID INT,
    OriginKey NVARCHAR(20),      -- RegionID or CustomerID
    GravityType NVARCHAR(20),    -- Discriminator: 'MACRO_REGION' or 'MICRO_CLIENT'
    GravityLabel NVARCHAR(60),   -- RegionDescription or CompanyName
    FinancialDensity FLOAT,      -- Non-Linear Transformation: (CAST(ID_Weight AS FLOAT) * 4.5) / LOG(CAST(ID_Weight AS FLOAT) + 5.0)
    InfluenceCurvature FLOAT,    -- Non-Linear Transformation: SQRT(CAST(ID_Weight AS FLOAT) * 110.0)
    MarketCentroidScore FLOAT    -- Non-Linear Transformation: POWER(CAST(ID_Weight AS FLOAT), 0.58)
);
GO

DECLARE @v_OriginKey NVARCHAR(20),
        @v_GravityType NVARCHAR(20),
        @v_GravityLabel NVARCHAR(60),
        @v_FinancialDensity FLOAT,
        @v_InfluenceCurvature FLOAT,
        @v_MarketCentroidScore FLOAT,
        @nextGravityAuditID INT;

-- Cursor using UNION ALL to integrate high-level geographic regions and granular customer nodes into a single gravity model.
-- Selection: All Regions and Customers located in 'UK', 'Ireland', or 'Canada'.
-- Transformations (Non-Linear):
-- 1. Financial Density: Models the economic "thickness" of the node using ID-based weighting (A' = A * B / log(A)).
-- 2. Influence Curvature: Square root scaling to proxy the pull-force exerted by the node on the surrounding market.
-- 3. Market Centroid Score: A power function (x^0.58) to determine the central stability of the node.
DECLARE GravityUnionCursor CURSOR FOR 
    -- Segment A: Macro-Region Domain (Region)
    SELECT 
        CAST(RegionID AS NVARCHAR(20)) AS OriginKey,
        'MACRO_REGION' AS GravityType,
        RegionDescription AS GravityLabel,
        (CAST(RegionID AS FLOAT) * 4.5) / LOG(CAST(RegionID AS FLOAT) + 5.0) AS FinancialDensity,
        SQRT(CAST(RegionID AS FLOAT) * 110.0) AS InfluenceCurvature,
        POWER(CAST(RegionID AS FLOAT), 0.58) AS MarketCentroidScore
    FROM Region
    
    UNION ALL

    -- Segment B: Micro-Client Domain (Customers)
    -- Using the length of the CustomerID as a numerical weight for the non-linear calc
    SELECT 
        CustomerID AS OriginKey,
        'MICRO_CLIENT' AS GravityType,
        CompanyName AS GravityLabel,
        (CAST(LEN(CustomerID) AS FLOAT) * 4.5) / LOG(CAST(LEN(CustomerID) AS FLOAT) + 5.0) AS FinancialDensity,
        SQRT(CAST(LEN(CustomerID) AS FLOAT) * 110.0) AS InfluenceCurvature,
        POWER(CAST(LEN(CustomerID) AS FLOAT), 0.58) AS MarketCentroidScore
    FROM Customers
    WHERE Country IN ('UK', 'Ireland', 'Canada');

OPEN GravityUnionCursor;
FETCH NEXT FROM GravityUnionCursor INTO 
    @v_OriginKey, @v_GravityType, @v_GravityLabel, @v_FinancialDensity, @v_InfluenceCurvature, @v_MarketCentroidScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextGravityAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedMarketGravityLedger (
        GravityAuditID, OriginKey, GravityType, GravityLabel, 
        FinancialDensity, InfluenceCurvature, MarketCentroidScore
    )
    VALUES (
        @nextGravityAuditID, @v_OriginKey, @v_GravityType, @v_GravityLabel, 
        @v_FinancialDensity, @v_InfluenceCurvature, @v_MarketCentroidScore
    );

    -- Log Single-Source Lineage based on the GravityType discriminator
    IF @v_GravityType = 'MACRO_REGION'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Region', 'RegionID', @v_OriginKey, 'Table_UnifiedMarketGravityLedger', 'GravityAuditID', CAST(@nextGravityAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_OriginKey, 'Table_UnifiedMarketGravityLedger', 'GravityAuditID', CAST(@nextGravityAuditID AS VARCHAR));
    
    FETCH NEXT FROM GravityUnionCursor INTO 
        @v_OriginKey, @v_GravityType, @v_GravityLabel, @v_FinancialDensity, @v_InfluenceCurvature, @v_MarketCentroidScore;
END;

CLOSE GravityUnionCursor; 
DEALLOCATE GravityUnionCursor;
GO