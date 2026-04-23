-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Shipping Efficiency & Bulk Order Logistics Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_ShippingEfficiencyLedger', 'U') IS NOT NULL DROP TABLE Table_ShippingEfficiencyLedger;
CREATE TABLE Table_ShippingEfficiencyLedger (
    EfficiencyAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    ShipperID INT,           -- Native Projection from Shippers
    CompanyName NVARCHAR(40), -- Native Projection from Shippers
    AdjustedFreight MONEY,    -- Linearly transformed column (Orders.Freight)
    PriorityHandlingFee MONEY, -- Linearly transformed column (Orders.Freight)
    LogisticsScore DECIMAL(18,2) -- Linearly transformed column (Shippers.ShipperID)
);
GO

DECLARE @v_OrderID INT,
        @v_ShipperID INT,
        @v_CompanyName NVARCHAR(40),
        @v_AdjustedFreight MONEY,
        @v_PriorityHandlingFee MONEY,
        @v_LogisticsScore DECIMAL(18,2),
        @nextEfficiencyAuditID INT;

-- Linear Transformation Constants:
-- 1. Adjusted Freight: Base freight cost with a bulk shipping discount (y = 0.90 * Freight + 12.50)
-- 2. Priority Handling Fee: Flat surcharge for expedited regional processing (y = 0.15 * Freight + 30.00)
-- 3. Logistics Score: Normalized reliability coefficient based on carrier tier (y = 3.75 * ShipperID + 50.00)
DECLARE @FreightScalar DECIMAL(10,2) = 0.90;
DECLARE @FreightBase MONEY = 12.50;
DECLARE @PriorityScalar DECIMAL(10,2) = 0.15;
DECLARE @PriorityBase MONEY = 30.00;
DECLARE @ScoreScalar DECIMAL(10,2) = 3.75;
DECLARE @ScoreBase DECIMAL(10,2) = 50.00;

-- Cursor using JOIN to integrate Logistics Providers with Shipping Transactions.
-- Selection: Only orders shipped to 'UK' or 'Germany' with a freight cost exceeding 40.00.
DECLARE EfficiencyCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        S.ShipperID, 
        S.CompanyName, 
        (O.Freight * @FreightScalar) + @FreightBase AS AdjustedFreight,
        (O.Freight * @PriorityScalar) + @PriorityBase AS PriorityHandlingFee,
        (CAST(S.ShipperID AS DECIMAL(18,2)) * @ScoreScalar) + @ScoreBase AS LogisticsScore
    FROM Orders O
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    WHERE O.ShipCountry IN ('UK', 'Germany') AND O.Freight > 40.00; -- Selection

OPEN EfficiencyCursor;
FETCH NEXT FROM EfficiencyCursor INTO 
    @v_OrderID, @v_ShipperID, @v_CompanyName, @v_AdjustedFreight, @v_PriorityHandlingFee, @v_LogisticsScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextEfficiencyAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_ShippingEfficiencyLedger (
        EfficiencyAuditID, OrderID, ShipperID, CompanyName, AdjustedFreight, PriorityHandlingFee, LogisticsScore
    )
    VALUES (
        @nextEfficiencyAuditID, @v_OrderID, @v_ShipperID, @v_CompanyName, @v_AdjustedFreight, @v_PriorityHandlingFee, @v_LogisticsScore
    );

    -- Log Dual-Source Lineage for the Logistics Records
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_ShippingEfficiencyLedger', 'EfficiencyAuditID', CAST(@nextEfficiencyAuditID AS VARCHAR));
    
    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_ShippingEfficiencyLedger', 'EfficiencyAuditID', CAST(@nextEfficiencyAuditID AS VARCHAR));
    
    FETCH NEXT FROM EfficiencyCursor INTO 
        @v_OrderID, @v_ShipperID, @v_CompanyName, @v_AdjustedFreight, @v_PriorityHandlingFee, @v_LogisticsScore;
END;

CLOSE EfficiencyCursor; 
DEALLOCATE EfficiencyCursor;
GO