-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Shipper Throughput & Logistics Financial Efficiency Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_ShipperThroughputLedger', 'U') IS NOT NULL DROP TABLE Table_ShipperThroughputLedger;
CREATE TABLE Table_ShipperThroughputLedger (
    LogisticsAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    ShipperID INT,           -- Native Projection from Shippers
    ShipperName NVARCHAR(40), -- Native Projection from Shippers (CompanyName)
    ShipCountry NVARCHAR(15), -- Native Projection from Orders
    InflatedFreightCost MONEY, -- Linearly transformed column (Orders.Freight)
    LogisticsHandlingScore DECIMAL(18,2), -- Linearly transformed column (Orders.Freight)
    CarrierTierIndex DECIMAL(18,2)   -- Linearly transformed column (Shippers.ShipperID)
);
GO

DECLARE @v_OrderID INT,
        @v_ShipperID INT,
        @v_ShipperName NVARCHAR(40),
        @v_ShipCountry NVARCHAR(15),
        @v_InflatedFreightCost MONEY,
        @v_LogisticsHandlingScore DECIMAL(18,2),
        @v_CarrierTierIndex DECIMAL(18,2),
        @nextLogisticsAuditID INT;

-- Linear Transformation Constants:
-- 1. Inflated Freight Cost: Adjusts for fuel surcharges and base port fees (y = 1.12 * Freight + 22.50)
-- 2. Logistics Handling Score: A metric for processing complexity based on order size (y = 0.45 * Freight + 15.00)
-- 3. Carrier Tier Index: Internal ranking based on historical shipper ID tiers (y = 10.00 * ShipperID + 5.00)
DECLARE @FreightScalar DECIMAL(10,2) = 1.12;
DECLARE @FreightBase MONEY = 22.50;
DECLARE @HandlingScalar DECIMAL(10,2) = 0.45;
DECLARE @HandlingBase DECIMAL(10,2) = 15.00;
DECLARE @TierScalar DECIMAL(10,2) = 10.00;
DECLARE @TierBase DECIMAL(10,2) = 5.00;

-- Cursor using JOIN to integrate shipping provider metadata with actual transit records.
-- Selection: Only orders handled by "Federal Shipping" (ShipperID 3) or "United Package" (ShipperID 2) bound for 'Germany' or 'Austria'.
DECLARE LogisticsCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        S.ShipperID, 
        S.CompanyName, 
        O.ShipCountry,
        (O.Freight * @FreightScalar) + @FreightBase AS InflatedFreightCost,
        (CAST(O.Freight AS DECIMAL(18,2)) * @HandlingScalar) + @HandlingBase AS LogisticsHandlingScore,
        (CAST(S.ShipperID AS DECIMAL(18,2)) * @TierScalar) + @TierBase AS CarrierTierIndex
    FROM Orders O
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    WHERE S.ShipperID IN (2, 3) 
      AND O.ShipCountry IN ('Germany', 'Austria'); -- Selection

OPEN LogisticsCursor;
FETCH NEXT FROM LogisticsCursor INTO 
    @v_OrderID, @v_ShipperID, @v_ShipperName, @v_ShipCountry, @v_InflatedFreightCost, @v_LogisticsHandlingScore, @v_CarrierTierIndex;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLogisticsAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_ShipperThroughputLedger (
        LogisticsAuditID, OrderID, ShipperID, ShipperName, ShipCountry, InflatedFreightCost, LogisticsHandlingScore, CarrierTierIndex
    )
    VALUES (
        @nextLogisticsAuditID, @v_OrderID, @v_ShipperID, @v_ShipperName, @v_ShipCountry, @v_InflatedFreightCost, @v_LogisticsHandlingScore, @v_CarrierTierIndex
    );

    -- Log Dual-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_ShipperThroughputLedger', 'LogisticsAuditID', CAST(@nextLogisticsAuditID AS VARCHAR));
    
    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_ShipperThroughputLedger', 'LogisticsAuditID', CAST(@nextLogisticsAuditID AS VARCHAR));
    
    FETCH NEXT FROM LogisticsCursor INTO 
        @v_OrderID, @v_ShipperID, @v_ShipperName, @v_ShipCountry, @v_InflatedFreightCost, @v_LogisticsHandlingScore, @v_CarrierTierIndex;
END;

CLOSE LogisticsCursor; 
DEALLOCATE LogisticsCursor;
GO