-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Logistics & Market Presence Ledger.
-- Rule: UNION ALL between different tables (Orders and Customers), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Shipping Intensity" for recent orders and "Market Priority" for regional customers.

IF OBJECT_ID('Table_LogisticsMarketLedger', 'U') IS NOT NULL DROP TABLE Table_LogisticsMarketLedger;
CREATE TABLE Table_LogisticsMarketLedger (
    LedgerID INT,
    SourceDomain VARCHAR(25),
    SourcePKID NVARCHAR(15),
    EntityDescriptor NVARCHAR(40),
    WeightedScore DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_SourceDomain VARCHAR(25),
        @v_SourcePKID NVARCHAR(15),
        @v_EntityDescriptor NVARCHAR(40),
        @v_WeightedScore DECIMAL(18, 2),
        @nextLedgerID INT;

-- Linear Transformation Constants:
-- 1. Logistics (Orders): Intensity score based on freight cost with a flat handling base (y = 0.65 * Freight + 15.00)
-- 2. Market (Customers): Priority score based on a flat regional constant (y = 0 * x + 85.00)
DECLARE @LogisticsIntensityScalar DECIMAL(10,2) = 0.65;
DECLARE @LogisticsBaseScore DECIMAL(10,2) = 15.00;
DECLARE @MarketPriorityBaseline DECIMAL(10,2) = 85.00;

-- Cursor combining DIFFERENT tables (Orders and Customers) via UNION ALL
DECLARE LogisticsMarketCursor CURSOR FOR 
    -- Branch 1: Orders (Recent Logistics Activity)
    -- Selection: Only orders shipped to Brazil (Focused market analysis)
    -- Transformation: Linear intensity scaling (y = 0.65 * x + 15.00)
    SELECT 
        'ShippingIntensity' AS SourceDomain, 
        CAST(OrderID AS NVARCHAR(15)) AS SourcePKID, 
        ShipName AS EntityDescriptor, 
        (CAST(Freight AS DECIMAL(18,2)) * @LogisticsIntensityScalar) + @LogisticsBaseScore AS WeightedScore 
    FROM Orders 
    WHERE ShipCountry = 'Brazil' -- Selection
    
    UNION ALL

    -- Branch 2: Customers (Strategic Market Presence)
    -- Selection: Only customers located in Germany
    -- Transformation: Constant linear projection for priority ranking (y = 0 * x + 85.00)
    SELECT 
        'MarketPriority' AS SourceDomain, 
        CAST(CustomerID AS NVARCHAR(15)) AS SourcePKID, 
        CompanyName AS EntityDescriptor, 
        @MarketPriorityBaseline AS WeightedScore 
    FROM Customers
    WHERE Country = 'Germany'; -- Selection

OPEN LogisticsMarketCursor;
FETCH NEXT FROM LogisticsMarketCursor INTO @v_SourceDomain, @v_SourcePKID, @v_EntityDescriptor, @v_WeightedScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLedgerID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_LogisticsMarketLedger (LedgerID, SourceDomain, SourcePKID, EntityDescriptor, WeightedScore)
    VALUES (@nextLedgerID, @v_SourceDomain, @v_SourcePKID, @v_EntityDescriptor, @v_WeightedScore);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_SourceDomain = 'ShippingIntensity'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', @v_SourcePKID, 'Table_LogisticsMarketLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    ELSE IF @v_SourceDomain = 'MarketPriority'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_SourcePKID, 'Table_LogisticsMarketLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    
    FETCH NEXT FROM LogisticsMarketCursor INTO @v_SourceDomain, @v_SourcePKID, @v_EntityDescriptor, @v_WeightedScore;
END;

CLOSE LogisticsMarketCursor; 
DEALLOCATE LogisticsMarketCursor;
GO