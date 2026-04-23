-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Risk Assessment Ledger for financial and operational exposure.
-- Rule: UNION ALL between different tables (Customers and Products), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating a "Risk Exposure Score" based on customer credit standing and product inventory volatility.

IF OBJECT_ID('Table_EnterpriseRiskLedger', 'U') IS NOT NULL DROP TABLE Table_EnterpriseRiskLedger;
CREATE TABLE Table_EnterpriseRiskLedger (
    RiskID INT,
    ExposureDomain VARCHAR(25),
    SourcePKID NVARCHAR(15), -- Fits both CustomerID (NCHAR) and ProductID (INT)
    EntityName NVARCHAR(40),
    CalculatedRiskScore DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_ExposureDomain VARCHAR(25),
        @v_SourcePKID NVARCHAR(15),
        @v_EntityName NVARCHAR(40),
        @v_RiskScore DECIMAL(18, 2),
        @nextRiskID INT;

-- Linear Transformation Constants (Risk Models):
-- 1. Revenue Risk (Customers): Risk increases linearly with empty region fields (y = 0 * x + 75.00 for target group)
-- 2. Supply Risk (Products): Risk is an inverse linear function of units in stock (y = -0.5 * UnitsInStock + 100.00)
DECLARE @SupplyVolatilityScalar DECIMAL(10,2) = -0.5;
DECLARE @SupplyBaseRisk DECIMAL(10,2) = 100.00;
DECLARE @CreditBaseRisk DECIMAL(10,2) = 75.00;

-- Cursor combining DIFFERENT tables (Customers and Products) via UNION ALL
DECLARE RiskCursor CURSOR FOR 
    -- Branch 1: Customers (Credit/Market Exposure)
    -- Selection: Only customers without a specified Region (High uncertainty/data gap)
    -- Transformation: Flat linear projection for high-uncertainty accounts (y = 0 * x + 75.00)
    SELECT 
        'MarketExposure' AS ExposureDomain, 
        CAST(CustomerID AS NVARCHAR(15)) AS SourcePKID, 
        CompanyName AS EntityName, 
        @CreditBaseRisk AS CalculatedRiskScore 
    FROM Customers 
    WHERE Region IS NULL -- Selection
    
    UNION ALL

    -- Branch 2: Products (Operational/Supply Exposure)
    -- Selection: Only products with low stock (UnitsInStock < 10)
    -- Transformation: Linear risk scaling—lower stock equals higher risk (y = -0.5 * x + 100.00)
    SELECT 
        'SupplyExposure' AS ExposureDomain, 
        CAST(ProductID AS NVARCHAR(15)) AS SourcePKID, 
        ProductName AS EntityName, 
        (CAST(UnitsInStock AS DECIMAL(18,2)) * @SupplyVolatilityScalar) + @SupplyBaseRisk AS CalculatedRiskScore 
    FROM Products
    WHERE UnitsInStock < 10; -- Selection

OPEN RiskCursor;
FETCH NEXT FROM RiskCursor INTO @v_ExposureDomain, @v_SourcePKID, @v_EntityName, @v_RiskScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextRiskID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the projected, filtered, and linearly transformed record
    INSERT INTO Table_EnterpriseRiskLedger (RiskID, ExposureDomain, SourcePKID, EntityName, CalculatedRiskScore)
    VALUES (@nextRiskID, @v_ExposureDomain, @v_SourcePKID, @v_EntityName, @v_RiskScore);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_ExposureDomain = 'MarketExposure'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_SourcePKID, 'Table_EnterpriseRiskLedger', 'RiskID', CAST(@nextRiskID AS VARCHAR));
    END
    ELSE IF @v_ExposureDomain = 'SupplyExposure'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', @v_SourcePKID, 'Table_EnterpriseRiskLedger', 'RiskID', CAST(@nextRiskID AS VARCHAR));
    END
    
    FETCH NEXT FROM RiskCursor INTO @v_ExposureDomain, @v_SourcePKID, @v_EntityName, @v_RiskScore;
END;

CLOSE RiskCursor; 
DEALLOCATE RiskCursor;
GO