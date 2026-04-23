-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling a Customer Order Fulfillment & Shipping Partner Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_CustomerShippingLedger', 'U') IS NOT NULL DROP TABLE Table_CustomerShippingLedger;
CREATE TABLE Table_CustomerShippingLedger (
    ShippingAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    CustomerID NCHAR(5),     -- Native Projection from Customers
    ShipperID INT,           -- Native Projection from Shippers
    CompanyName NVARCHAR(40), -- Native Projection from Customers
    ShipperName NVARCHAR(40), -- Native Projection from Shippers
    ShipCountry NVARCHAR(15)  -- Native Projection from Orders
);
GO

DECLARE @v_OrderID INT,
        @v_CustomerID NCHAR(5),
        @v_ShipperID INT,
        @v_CompanyName NVARCHAR(40),
        @v_ShipperName NVARCHAR(40),
        @v_ShipCountry NVARCHAR(15),
        @nextShippingAuditID INT;

-- Cursor using JOIN for strict projection across Customers, their Orders, and the Shippers used.
-- Selection: Only orders shipped to Mexico using "Federal Shipping" (ShipperID 3).
-- All attributes are native; no linear transformations or formatting functions are applied.
DECLARE ShippingCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        C.CustomerID, 
        S.ShipperID, 
        C.CompanyName, 
        S.CompanyName AS ShipperName, 
        O.ShipCountry
    FROM Orders O
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    WHERE O.ShipCountry = 'Mexico' AND S.ShipperID = 3; -- Selection

OPEN ShippingCursor;
FETCH NEXT FROM ShippingCursor INTO 
    @v_OrderID, @v_CustomerID, @v_ShipperID, @v_CompanyName, @v_ShipperName, @v_ShipCountry;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextShippingAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_CustomerShippingLedger (
        ShippingAuditID, OrderID, CustomerID, ShipperID, CompanyName, ShipperName, ShipCountry
    )
    VALUES (
        @nextShippingAuditID, @v_OrderID, @v_CustomerID, @v_ShipperID, @v_CompanyName, @v_ShipperName, @v_ShipCountry
    );

    -- Log Triple-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_CustomerShippingLedger', 'ShippingAuditID', CAST(@nextShippingAuditID AS VARCHAR));
    
    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_CustomerShippingLedger', 'ShippingAuditID', CAST(@nextShippingAuditID AS VARCHAR));

    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_CustomerShippingLedger', 'ShippingAuditID', CAST(@nextShippingAuditID AS VARCHAR));
    
    FETCH NEXT FROM ShippingCursor INTO 
        @v_OrderID, @v_CustomerID, @v_ShipperID, @v_CompanyName, @v_ShipperName, @v_ShipCountry;
END;

CLOSE ShippingCursor; 
DEALLOCATE ShippingCursor;
GO