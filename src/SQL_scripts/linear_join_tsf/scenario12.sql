-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Market-Adjusted Order Fulfillment & Logistics Asset Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_MarketAdjustedLogisticsLedger', 'U') IS NOT NULL DROP TABLE Table_MarketAdjustedLogisticsLedger;
CREATE TABLE Table_MarketAdjustedLogisticsLedger (
    AssetAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    ShipperID INT,           -- Native Projection from Shippers
    CustomerID NCHAR(5),     -- Native Projection from Customers
    CompanyName NVARCHAR(40), -- Native Projection from Customers
    MarketAdjustedFreight MONEY, -- Linearly transformed column (Orders.Freight)
    LogisticsRiskIndex DECIMAL(18,2), -- Linearly transformed column (Shippers.ShipperID)
    OrderPriorityScore DECIMAL(18,2)  -- Linearly transformed column (Orders.Freight)
);
GO

DECLARE @v_OrderID INT,
        @v_ShipperID INT,
        @v_CustomerID NCHAR(5),
        @v_CompanyName NVARCHAR(40),
        @v_MarketAdjustedFreight MONEY,
        @v_LogisticsRiskIndex DECIMAL(18,2),
        @v_OrderPriorityScore DECIMAL(18,2),
        @nextAssetAuditID INT;

-- Linear Transformation Constants:
-- 1. Market Adjusted Freight: Base freight plus a regional market surcharge (y = 1.15 * Freight + 28.50)
-- 2. Logistics Risk Index: Calculated based on the carrier's legacy tiering (y = 4.25 * ShipperID + 12.00)
-- 3. Order Priority Score: Weights freight cost as a proxy for fulfillment urgency (y = 0.60 * Freight + 45.00)
DECLARE @MarketScalar DECIMAL(10,2) = 1.15;
DECLARE @MarketBase MONEY = 28.50;
DECLARE @RiskScalar DECIMAL(10,2) = 4.25;
DECLARE @RiskBase DECIMAL(10,2) = 12.00;
DECLARE @PriorityScalar DECIMAL(10,2) = 0.60;
DECLARE @PriorityBase DECIMAL(10,2) = 45.00;

-- Cursor using JOIN to integrate Customer demographics, Order logistics, and Carrier metadata.
-- Selection: Only orders bound for 'Denmark', 'Finland', or 'Norway' (Nordic Cluster) with freight > 25.00.
DECLARE MarketLogisticsCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        S.ShipperID, 
        C.CustomerID,
        C.CompanyName, 
        (O.Freight * @MarketScalar) + @MarketBase AS MarketAdjustedFreight,
        (CAST(S.ShipperID AS DECIMAL(18,2)) * @RiskScalar) + @RiskBase AS LogisticsRiskIndex,
        (CAST(O.Freight AS DECIMAL(18,2)) * @PriorityScalar) + @PriorityBase AS OrderPriorityScore
    FROM Orders O
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID
    WHERE O.ShipCountry IN ('Denmark', 'Finland', 'Norway') AND O.Freight > 25.00;

OPEN MarketLogisticsCursor;
FETCH NEXT FROM MarketLogisticsCursor INTO 
    @v_OrderID, @v_ShipperID, @v_CustomerID, @v_CompanyName, @v_MarketAdjustedFreight, @v_LogisticsRiskIndex, @v_OrderPriorityScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAssetAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_MarketAdjustedLogisticsLedger (
        AssetAuditID, OrderID, ShipperID, CustomerID, CompanyName, MarketAdjustedFreight, LogisticsRiskIndex, OrderPriorityScore
    )
    VALUES (
        @nextAssetAuditID, @v_OrderID, @v_ShipperID, @v_CustomerID, @v_CompanyName, @v_MarketAdjustedFreight, @v_LogisticsRiskIndex, @v_OrderPriorityScore
    );

    -- Log Triple-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_MarketAdjustedLogisticsLedger', 'AssetAuditID', CAST(@nextAssetAuditID AS VARCHAR));
    
    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_MarketAdjustedLogisticsLedger', 'AssetAuditID', CAST(@nextAssetAuditID AS VARCHAR));

    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_MarketAdjustedLogisticsLedger', 'AssetAuditID', CAST(@nextAssetAuditID AS VARCHAR));
    
    FETCH NEXT FROM MarketLogisticsCursor INTO 
        @v_OrderID, @v_ShipperID, @v_CustomerID, @v_CompanyName, @v_MarketAdjustedFreight, @v_LogisticsRiskIndex, @v_OrderPriorityScore;
END;

CLOSE MarketLogisticsCursor; 
DEALLOCATE MarketLogisticsCursor;
GO