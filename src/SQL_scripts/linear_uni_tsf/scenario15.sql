-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Asset Liability & Fulfillment Scorecard.
-- Rule: UNION ALL between different tables (Orders and Products), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Liability Risk" for overdue shipments and "Replenishment Urgency" for low-inventory items.

IF OBJECT_ID('Table_LiabilityFulfillmentLedger', 'U') IS NOT NULL DROP TABLE Table_LiabilityFulfillmentLedger;
CREATE TABLE Table_LiabilityFulfillmentLedger (
    LedgerID INT,
    AuditContext VARCHAR(25),
    SourcePKID INT,
    EntityName NVARCHAR(40),
    WeightedScore DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_AuditContext VARCHAR(25),
        @v_SourcePKID INT,
        @v_EntityName NVARCHAR(40),
        @v_WeightedScore DECIMAL(18, 2),
        @nextLedgerID INT;

-- Linear Transformation Constants:
-- 1. Liability (Orders): Risk score based on Freight cost with a flat late-delivery penalty (y = 0.45 * Freight + 75.00)
-- 2. Fulfillment (Products): Replenishment urgency based on ReorderLevel (y = 1.80 * ReorderLevel + 10.00)
DECLARE @LiabilityScalar DECIMAL(10,2) = 0.45;
DECLARE @LiabilityBasePenalty DECIMAL(10,2) = 75.00;
DECLARE @FulfillmentScalar DECIMAL(10,2) = 1.80;
DECLARE @FulfillmentBaseScore DECIMAL(10,2) = 10.00;

-- Cursor combining DIFFERENT tables (Orders and Products) via UNION ALL
DECLARE LiabilityFulfillmentCursor CURSOR FOR 
    -- Branch 1: Orders (Shipping Liability Risk)
    -- Selection: Only orders that have not been shipped yet (ShippedDate IS NULL)
    -- Transformation: Linear liability scaling based on freight value (y = 0.45 * x + 75.00)
    SELECT 
        'PendingLiability' AS AuditContext, 
        OrderID AS SourcePKID, 
        ShipName AS EntityName, 
        (CAST(Freight AS DECIMAL(18,2)) * @LiabilityScalar) + @LiabilityBasePenalty AS WeightedScore 
    FROM Orders 
    WHERE ShippedDate IS NULL -- Selection (Unfulfilled orders)
    
    UNION ALL

    -- Branch 2: Products (Inventory Replenishment)
    -- Selection: Only products that are not discontinued but have low stock (UnitsInStock < 10)
    -- Transformation: Linear urgency scaling based on reorder thresholds (y = 1.80 * x + 10.00)
    SELECT 
        'ReplenishmentUrgency' AS AuditContext, 
        ProductID AS SourcePKID, 
        ProductName AS EntityName, 
        (CAST(ReorderLevel AS DECIMAL(18,2)) * @FulfillmentScalar) + @FulfillmentBaseScore AS WeightedScore 
    FROM Products
    WHERE Discontinued = 0 AND UnitsInStock < 10; -- Selection (Critical inventory)

OPEN LiabilityFulfillmentCursor;
FETCH NEXT FROM LiabilityFulfillmentCursor INTO @v_AuditContext, @v_SourcePKID, @v_EntityName, @v_WeightedScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLedgerID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_LiabilityFulfillmentLedger (LedgerID, AuditContext, SourcePKID, EntityName, WeightedScore)
    VALUES (@nextLedgerID, @v_AuditContext, @v_SourcePKID, @v_EntityName, @v_WeightedScore);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AuditContext = 'PendingLiability'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', CAST(@v_SourcePKID AS VARCHAR), 'Table_LiabilityFulfillmentLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    ELSE IF @v_AuditContext = 'ReplenishmentUrgency'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_SourcePKID AS VARCHAR), 'Table_LiabilityFulfillmentLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    
    FETCH NEXT FROM LiabilityFulfillmentCursor INTO @v_AuditContext, @v_SourcePKID, @v_EntityName, @v_WeightedScore;
END;

CLOSE LiabilityFulfillmentCursor; 
DEALLOCATE LiabilityFulfillmentCursor;
GO