-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Supply Chain Economic Value & Logistics Impact Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_EconomicValueLogisticsLedger', 'U') IS NOT NULL DROP TABLE Table_EconomicValueLogisticsLedger;
CREATE TABLE Table_EconomicValueLogisticsLedger (
    ValueAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    ProductID INT,           -- Native Projection from Products
    ShipName NVARCHAR(40),   -- Native Projection from Orders
    AdjustedStockValue MONEY, -- Linearly transformed column (Products.UnitPrice)
    WeightedFreight MONEY,    -- Linearly transformed column (Orders.Freight)
    FulfillmentIndex DECIMAL(18,2) -- Linearly transformed column (Products.UnitsInStock)
);
GO

DECLARE @v_OrderID INT,
        @v_ProductID INT,
        @v_ShipName NVARCHAR(40),
        @v_AdjustedStockValue MONEY,
        @v_WeightedFreight MONEY,
        @v_FulfillmentIndex DECIMAL(18,2),
        @nextValueAuditID INT;

-- Linear Transformation Constants:
-- 1. Adjusted Stock Value: Baseline inventory cost plus a standard holding markup (y = 1.15 * UnitPrice + 5.00)
-- 2. Weighted Freight: Scaled shipping cost with an added flat international handling fee (y = 0.85 * Freight + 25.00)
-- 3. Fulfillment Index: Normalized stock capacity score (y = 2.50 * UnitsInStock + 10.00)
DECLARE @StockScalar DECIMAL(10,2) = 1.15;
DECLARE @StockBase MONEY = 5.00;
DECLARE @FreightScalar DECIMAL(10,2) = 0.85;
DECLARE @FreightBase MONEY = 25.00;
DECLARE @FulfillmentScalar DECIMAL(10,2) = 2.50;
DECLARE @FulfillmentBase DECIMAL(10,2) = 10.00;

-- Cursor using JOIN to integrate Transactional Logistics and Product Financials.
-- Selection: Only orders bound for 'France' or 'Belgium' where the product is not discontinued.
DECLARE ValueLogisticsCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        P.ProductID, 
        O.ShipName, 
        (P.UnitPrice * @StockScalar) + @StockBase AS AdjustedStockValue,
        (O.Freight * @FreightScalar) + @FreightBase AS WeightedFreight,
        (CAST(P.UnitsInStock AS DECIMAL(18,2)) * @FulfillmentScalar) + @FulfillmentBase AS FulfillmentIndex
    FROM Orders O
    INNER JOIN [Order Details] OD ON O.OrderID = OD.OrderID
    INNER JOIN Products P ON OD.ProductID = P.ProductID
    WHERE O.ShipCountry IN ('France', 'Belgium') AND P.Discontinued = 0;

OPEN ValueLogisticsCursor;
FETCH NEXT FROM ValueLogisticsCursor INTO 
    @v_OrderID, @v_ProductID, @v_ShipName, @v_AdjustedStockValue, @v_WeightedFreight, @v_FulfillmentIndex;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextValueAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_EconomicValueLogisticsLedger (
        ValueAuditID, OrderID, ProductID, ShipName, AdjustedStockValue, WeightedFreight, FulfillmentIndex
    )
    VALUES (
        @nextValueAuditID, @v_OrderID, @v_ProductID, @v_ShipName, @v_AdjustedStockValue, @v_WeightedFreight, @v_FulfillmentIndex
    );

    -- Log Dual-Source Lineage for the Joined Records
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_EconomicValueLogisticsLedger', 'ValueAuditID', CAST(@nextValueAuditID AS VARCHAR));
    
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_EconomicValueLogisticsLedger', 'ValueAuditID', CAST(@nextValueAuditID AS VARCHAR));
    
    FETCH NEXT FROM ValueLogisticsCursor INTO 
        @v_OrderID, @v_ProductID, @v_ShipName, @v_AdjustedStockValue, @v_WeightedFreight, @v_FulfillmentIndex;
END;

CLOSE ValueLogisticsCursor; 
DEALLOCATE ValueLogisticsCursor;
GO