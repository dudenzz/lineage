-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Quality Control & Loyalty Ledger.
-- Rule: UNION ALL between different tables (Products and Customers), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Quantifying "Loyalty Points" for high-value customers and "Quality Inspection Priority" for specific products.

IF OBJECT_ID('Table_QualityLoyaltyLedger', 'U') IS NOT NULL DROP TABLE Table_QualityLoyaltyLedger;
CREATE TABLE Table_QualityLoyaltyLedger (
    EntryID INT,
    LedgerType VARCHAR(25),
    SourcePKID NVARCHAR(15),
    Descriptor NVARCHAR(40),
    WeightedMetric DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_LedgerType VARCHAR(25),
        @v_SourcePKID NVARCHAR(15),
        @v_Descriptor NVARCHAR(40),
        @v_WeightedMetric DECIMAL(18, 2),
        @nextEntryID INT;

-- Linear Transformation Constants:
-- 1. Loyalty (Customers): Points based on a flat participation bonus plus a scalar (y = 0 * x + 500.00)
-- 2. Quality (Products): Inspection priority score increases with product ID age (y = 0.75 * ProductID + 5.00)
DECLARE @LoyaltyBasePoints DECIMAL(10,2) = 500.00;
DECLARE @QualityAgeScalar DECIMAL(10,2) = 0.75;
DECLARE @QualityBaseScore DECIMAL(10,2) = 5.00;

-- Cursor combining DIFFERENT tables (Customers and Products) via UNION ALL
DECLARE QualityLoyaltyCursor CURSOR FOR 
    -- Branch 1: Customers (Loyalty Program)
    -- Selection: Only customers in France (Targeted regional campaign)
    -- Transformation: Flat linear assignment of points (y = 0 * x + 500.00)
    SELECT 
        'CustomerLoyalty' AS LedgerType, 
        CAST(CustomerID AS NVARCHAR(15)) AS SourcePKID, 
        CompanyName AS Descriptor, 
        @LoyaltyBasePoints AS WeightedMetric 
    FROM Customers 
    WHERE Country = 'France' -- Selection
    
    UNION ALL

    -- Branch 2: Products (Quality Assurance)
    -- Selection: Only products that are currently discontinued (Final audit)
    -- Transformation: Linear priority score based on ID (y = 0.75 * x + 5.00)
    SELECT 
        'DiscontinuedAudit' AS LedgerType, 
        CAST(ProductID AS NVARCHAR(15)) AS SourcePKID, 
        ProductName AS Descriptor, 
        (CAST(ProductID AS DECIMAL(18,2)) * @QualityAgeScalar) + @QualityBaseScore AS WeightedMetric 
    FROM Products
    WHERE Discontinued = 1; -- Selection

OPEN QualityLoyaltyCursor;
FETCH NEXT FROM QualityLoyaltyCursor INTO @v_LedgerType, @v_SourcePKID, @v_Descriptor, @v_WeightedMetric;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextEntryID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_QualityLoyaltyLedger (EntryID, LedgerType, SourcePKID, Descriptor, WeightedMetric)
    VALUES (@nextEntryID, @v_LedgerType, @v_SourcePKID, @v_Descriptor, @v_WeightedMetric);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_LedgerType = 'CustomerLoyalty'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_SourcePKID, 'Table_QualityLoyaltyLedger', 'EntryID', CAST(@nextEntryID AS VARCHAR));
    END
    ELSE IF @v_LedgerType = 'DiscontinuedAudit'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', @v_SourcePKID, 'Table_QualityLoyaltyLedger', 'EntryID', CAST(@nextEntryID AS VARCHAR));
    END
    
    FETCH NEXT FROM QualityLoyaltyCursor INTO @v_LedgerType, @v_SourcePKID, @v_Descriptor, @v_WeightedMetric;
END;

CLOSE QualityLoyaltyCursor; 
DEALLOCATE QualityLoyaltyCursor;
GO