-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Market Expansion & Logistics Readiness Ledger.
-- Rule: UNION ALL between different tables (Suppliers and Orders), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Expansion Capability Credits" for international suppliers and "Distance Handling Fees" for remote shipping.

IF OBJECT_ID('Table_MarketExpansionLedger', 'U') IS NOT NULL DROP TABLE Table_MarketExpansionLedger;
CREATE TABLE Table_MarketExpansionLedger (
    AuditID INT,
    AssetClass VARCHAR(25),
    SourcePKID NVARCHAR(15),
    EntityName NVARCHAR(40),
    CalculatedMetric MONEY -- Linearly transformed column
);
GO

DECLARE @v_AssetClass VARCHAR(25),
        @v_SourcePKID NVARCHAR(15),
        @v_EntityName NVARCHAR(40),
        @v_CalculatedMetric MONEY,
        @nextAuditID INT;

-- Linear Transformation Constants:
-- 1. Expansion (Suppliers): Credit based on SupplierID (node index) with a flat strategic bonus (y = 12.50 * SupplierID + 400.00)
-- 2. Logistics (Orders): Remote handling fee based on freight cost with a flat processing fee (y = 0.18 * Freight + 30.00)
DECLARE @ExpansionScalar MONEY = 12.50;
DECLARE @ExpansionBaseBonus MONEY = 400.00;
DECLARE @LogisticsScalar MONEY = 0.18;
DECLARE @LogisticsFlatFee MONEY = 30.00;

-- Cursor combining DIFFERENT tables (Suppliers and Orders) via UNION ALL
DECLARE ExpansionReadinessCursor CURSOR FOR 
    -- Branch 1: Suppliers (Market Expansion Capability)
    -- Selection: Only suppliers from Australia or Japan (Specific Pacific-Rim expansion audit)
    -- Transformation: Linear capability credit (y = 12.50 * x + 400.00)
    SELECT 
        'ExpansionCapability' AS AssetClass, 
        CAST(SupplierID AS NVARCHAR(15)) AS SourcePKID, 
        CompanyName AS EntityName, 
        (CAST(SupplierID AS MONEY) * @ExpansionScalar) + @ExpansionBaseBonus AS CalculatedMetric 
    FROM Suppliers 
    WHERE Country IN ('Australia', 'Japan') -- Selection
    
    UNION ALL

    -- Branch 2: Orders (Remote Logistics Handling)
    -- Selection: Only orders with high freight costs (> 120) destined for Europe
    -- Transformation: Linear distance handling fee (y = 0.18 * x + 30.00)
    SELECT 
        'RemoteHandling' AS AssetClass, 
        CAST(OrderID AS NVARCHAR(15)) AS SourcePKID, 
        ShipName AS EntityName, 
        (Freight * @LogisticsScalar) + @LogisticsFlatFee AS CalculatedMetric 
    FROM Orders
    WHERE Freight > 120 AND ShipCountry IN ('Germany', 'France', 'UK', 'Sweden'); -- Selection

OPEN ExpansionReadinessCursor;
FETCH NEXT FROM ExpansionReadinessCursor INTO @v_AssetClass, @v_SourcePKID, @v_EntityName, @v_CalculatedMetric;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_MarketExpansionLedger (AuditID, AssetClass, SourcePKID, EntityName, CalculatedMetric)
    VALUES (@nextAuditID, @v_AssetClass, @v_SourcePKID, @v_EntityName, @v_CalculatedMetric);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AssetClass = 'ExpansionCapability'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_SourcePKID, 'Table_MarketExpansionLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    END
    ELSE IF @v_AssetClass = 'RemoteHandling'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', @v_SourcePKID, 'Table_MarketExpansionLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    END
    
    FETCH NEXT FROM ExpansionReadinessCursor INTO @v_AssetClass, @v_SourcePKID, @v_EntityName, @v_CalculatedMetric;
END;

CLOSE ExpansionReadinessCursor; 
DEALLOCATE ExpansionReadinessCursor;
GO