-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Shipper-Volume Performance & Strategic Logistics Asset Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_ShipperStrategicPerformanceLedger', 'U') IS NOT NULL DROP TABLE Table_ShipperStrategicPerformanceLedger;
CREATE TABLE Table_ShipperStrategicPerformanceLedger (
    StrategicAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    ShipperID INT,           -- Native Projection from Shippers
    ProductID INT,           -- Native Projection from Products
    ShipperName NVARCHAR(40), -- Native Projection from Shippers
    AdjustedLogisticsCost MONEY, -- Linearly transformed column (Orders.Freight)
    InventoryThroughputIndex DECIMAL(18,2), -- Linearly transformed column (Products.UnitsInStock)
    CarrierServiceCoefficient DECIMAL(18,2) -- Linearly transformed column (Shippers.ShipperID)
);
GO

DECLARE @v_OrderID INT,
        @v_ShipperID INT,
        @v_ProductID INT,
        @v_ShipperName NVARCHAR(40),
        @v_AdjustedLogisticsCost MONEY,
        @v_InventoryThroughputIndex DECIMAL(18,2),
        @v_CarrierServiceCoefficient DECIMAL(18,2),
        @nextStrategicAuditID INT;

-- Linear Transformation Constants:
-- 1. Adjusted Logistics Cost: Escalated freight based on fuel overhead and flat handling (y = 1.25 * Freight + 45.00)
-- 2. Inventory Throughput Index: Normalized stock flow score (y = 0.55 * UnitsInStock + 10.00)
-- 3. Carrier Service Coefficient: Reliability ranking mapped from legacy shipper tiers (y = 8.50 * ShipperID + 25.00)
DECLARE @LogisticsScalar DECIMAL(10,2) = 1.25;
DECLARE @LogisticsBase MONEY = 45.00;
DECLARE @ThroughputScalar DECIMAL(10,2) = 0.55;
DECLARE @ThroughputBase DECIMAL(10,2) = 10.00;
DECLARE @ServiceScalar DECIMAL(10,2) = 8.50;
DECLARE @ServiceBase DECIMAL(10,2) = 25.00;

-- Cursor using JOIN to integrate Order transit data, Carrier metadata, and Product stock status.
-- Selection: Only orders shipped to 'USA', 'Mexico', or 'Canada' (NAFTA region) where units in stock > 50.
DECLARE StrategicLogisticsCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        S.ShipperID, 
        P.ProductID,
        S.CompanyName, 
        (O.Freight * @LogisticsScalar) + @LogisticsBase AS AdjustedLogisticsCost,
        (CAST(P.UnitsInStock AS DECIMAL(18,2)) * @ThroughputScalar) + @ThroughputBase AS InventoryThroughputIndex,
        (CAST(S.ShipperID AS DECIMAL(18,2)) * @ServiceScalar) + @ServiceBase AS CarrierServiceCoefficient
    FROM Orders O
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    INNER JOIN [Order Details] OD ON O.OrderID = OD.OrderID
    INNER JOIN Products P ON OD.ProductID = P.ProductID
    WHERE O.ShipCountry IN ('USA', 'Mexico', 'Canada') AND P.UnitsInStock > 50;

OPEN StrategicLogisticsCursor;
FETCH NEXT FROM StrategicLogisticsCursor INTO 
    @v_OrderID, @v_ShipperID, @v_ProductID, @v_ShipperName, @v_AdjustedLogisticsCost, @v_InventoryThroughputIndex, @v_CarrierServiceCoefficient;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextStrategicAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_ShipperStrategicPerformanceLedger (
        StrategicAuditID, OrderID, ShipperID, ProductID, ShipperName, AdjustedLogisticsCost, InventoryThroughputIndex, CarrierServiceCoefficient
    )
    VALUES (
        @nextStrategicAuditID, @v_OrderID, @v_ShipperID, @v_ProductID, @v_ShipperName, @v_AdjustedLogisticsCost, @v_InventoryThroughputIndex, @v_CarrierServiceCoefficient
    );

    -- Log Triple-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_ShipperStrategicPerformanceLedger', 'StrategicAuditID', CAST(@nextStrategicAuditID AS VARCHAR));
    
    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_ShipperStrategicPerformanceLedger', 'StrategicAuditID', CAST(@nextStrategicAuditID AS VARCHAR));

    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_ShipperStrategicPerformanceLedger', 'StrategicAuditID', CAST(@nextStrategicAuditID AS VARCHAR));
    
    FETCH NEXT FROM StrategicLogisticsCursor INTO 
        @v_OrderID, @v_ShipperID, @v_ProductID, @v_ShipperName, @v_AdjustedLogisticsCost, @v_InventoryThroughputIndex, @v_CarrierServiceCoefficient;
END;

CLOSE StrategicLogisticsCursor; 
DEALLOCATE StrategicLogisticsCursor;
GO