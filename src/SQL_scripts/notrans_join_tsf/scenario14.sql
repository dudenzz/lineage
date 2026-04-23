-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Order-Detail & Product Inventory Fulfillment Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_OrderFulfillmentLedger', 'U') IS NOT NULL DROP TABLE Table_OrderFulfillmentLedger;
CREATE TABLE Table_OrderFulfillmentLedger (
    FulfillmentAuditID INT,
    OrderID INT,             -- Native Projection from Order Details
    ProductID INT,           -- Native Projection from Products
    UnitPrice MONEY,         -- Native Projection from Order Details
    Quantity SMALLINT,       -- Native Projection from Order Details
    UnitsInStock SMALLINT,   -- Native Projection from Products
    Discontinued BIT         -- Native Projection from Products
);
GO

DECLARE @v_OrderID INT,
        @v_ProductID INT,
        @v_UnitPrice MONEY,
        @v_Quantity SMALLINT,
        @v_UnitsInStock SMALLINT,
        @v_Discontinued BIT,
        @nextFulfillmentAuditID INT;

-- Cursor using JOIN for strict projection across Transactional Line Items and Inventory State.
-- Selection: Only order details for high-quantity items (> 100) where the product is still active.
-- All columns are native; no calculations (e.g., Total Price) or transformations are performed.
DECLARE FulfillmentCursor CURSOR FOR 
    SELECT 
        OD.OrderID, 
        P.ProductID, 
        OD.UnitPrice, 
        OD.Quantity, 
        P.UnitsInStock,
        P.Discontinued
    FROM [Order Details] OD
    INNER JOIN Products P ON OD.ProductID = P.ProductID
    WHERE OD.Quantity > 100 AND P.Discontinued = 0; -- Selection

OPEN FulfillmentCursor;
FETCH NEXT FROM FulfillmentCursor INTO 
    @v_OrderID, @v_ProductID, @v_UnitPrice, @v_Quantity, @v_UnitsInStock, @v_Discontinued;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextFulfillmentAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_OrderFulfillmentLedger (
        FulfillmentAuditID, OrderID, ProductID, UnitPrice, Quantity, UnitsInStock, Discontinued
    )
    VALUES (
        @nextFulfillmentAuditID, @v_OrderID, @v_ProductID, @v_UnitPrice, @v_Quantity, @v_UnitsInStock, @v_Discontinued
    );

    -- Log Dual-Source Lineage
    -- Note: [Order Details] uses a composite key (OrderID, ProductID). We track both to ensure granularity.
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Order Details', 'OrderID|ProductID', CAST(@v_OrderID AS VARCHAR) + '|' + CAST(@v_ProductID AS VARCHAR), 'Table_OrderFulfillmentLedger', 'FulfillmentAuditID', CAST(@nextFulfillmentAuditID AS VARCHAR));
    
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_OrderFulfillmentLedger', 'FulfillmentAuditID', CAST(@nextFulfillmentAuditID AS VARCHAR));
    
    FETCH NEXT FROM FulfillmentCursor INTO 
        @v_OrderID, @v_ProductID, @v_UnitPrice, @v_Quantity, @v_UnitsInStock, @v_Discontinued;
END;

CLOSE FulfillmentCursor; 
DEALLOCATE FulfillmentCursor;
GO