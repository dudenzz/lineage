-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Order Fulfillment & Freight Logistics Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_OrderFreightLedger', 'U') IS NOT NULL DROP TABLE Table_OrderFreightLedger;
CREATE TABLE Table_OrderFreightLedger (
    FreightAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    ShipperID INT,           -- Native Projection from Shippers
    ShipName NVARCHAR(40),   -- Native Projection from Orders
    ShipperCompany NVARCHAR(40), -- Native Projection from Shippers (CompanyName)
    Freight MONEY,           -- Native Projection from Orders
    ShippedDate DATETIME     -- Native Projection from Orders
);
GO

DECLARE @v_OrderID INT,
        @v_ShipperID INT,
        @v_ShipName NVARCHAR(40),
        @v_ShipperCompany NVARCHAR(40),
        @v_Freight MONEY,
        @v_ShippedDate DATETIME,
        @nextFreightAuditID INT;

-- Cursor using JOIN for strict projection across Order shipping requirements and Logistics providers.
-- Selection: Only orders shipped via "Speedy Express" (ShipperID 1) that have high freight costs (> 50.00).
-- All attributes are native; no linear transformations or rounding are applied to the freight currency.
DECLARE FreightCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        S.ShipperID, 
        O.ShipName, 
        S.CompanyName AS ShipperCompany, 
        O.Freight,
        O.ShippedDate
    FROM Orders O
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    WHERE S.ShipperID = 1 AND O.Freight > 50.00; -- Selection

OPEN FreightCursor;
FETCH NEXT FROM FreightCursor INTO 
    @v_OrderID, @v_ShipperID, @v_ShipName, @v_ShipperCompany, @v_Freight, @v_ShippedDate;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextFreightAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_OrderFreightLedger (
        FreightAuditID, OrderID, ShipperID, ShipName, ShipperCompany, Freight, ShippedDate
    )
    VALUES (
        @nextFreightAuditID, @v_OrderID, @v_ShipperID, @v_ShipName, @v_ShipperCompany, @v_Freight, @v_ShippedDate
    );

    -- Log Dual-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_OrderFreightLedger', 'FreightAuditID', CAST(@nextFreightAuditID AS VARCHAR));
    
    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_OrderFreightLedger', 'FreightAuditID', CAST(@nextFreightAuditID AS VARCHAR));
    
    FETCH NEXT FROM FreightCursor INTO 
        @v_OrderID, @v_ShipperID, @v_ShipName, @v_ShipperCompany, @v_Freight, @v_ShippedDate;
END;

CLOSE FreightCursor; 
DEALLOCATE FreightCursor;
GO