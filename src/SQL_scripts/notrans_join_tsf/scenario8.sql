-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling a Customer-Order Geographic Distribution & Credit Reference Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_CustomerOrderGeoLedger', 'U') IS NOT NULL DROP TABLE Table_CustomerOrderGeoLedger;
CREATE TABLE Table_CustomerOrderGeoLedger (
    GeoAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    CustomerID NCHAR(5),     -- Native Projection from Customers
    CompanyName NVARCHAR(40), -- Native Projection from Customers
    ShipCity NVARCHAR(15),   -- Native Projection from Orders
    PostalCode NVARCHAR(10),  -- Native Projection from Customers
    RequiredDate DATETIME    -- Native Projection from Orders
);
GO

DECLARE @v_OrderID INT,
        @v_CustomerID NCHAR(5),
        @v_CompanyName NVARCHAR(40),
        @v_ShipCity NVARCHAR(15),
        @v_PostalCode NVARCHAR(10),
        @v_RequiredDate DATETIME,
        @nextGeoAuditID INT;

-- Cursor using JOIN for strict projection across Customer accounts and specific Order requirements.
-- Selection: Only customers located in Brazil where the order is bound for Rio de Janeiro.
-- All attributes are native; no linear transformations or regional grouping logic applied.
DECLARE GeoCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        C.CustomerID, 
        C.CompanyName, 
        O.ShipCity, 
        C.PostalCode,
        O.RequiredDate
    FROM Orders O
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID
    WHERE C.Country = 'Brazil' AND O.ShipCity = 'Rio de Janeiro'; -- Selection

OPEN GeoCursor;
FETCH NEXT FROM GeoCursor INTO 
    @v_OrderID, @v_CustomerID, @v_CompanyName, @v_ShipCity, @v_PostalCode, @v_RequiredDate;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextGeoAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_CustomerOrderGeoLedger (
        GeoAuditID, OrderID, CustomerID, CompanyName, ShipCity, PostalCode, RequiredDate
    )
    VALUES (
        @nextGeoAuditID, @v_OrderID, @v_CustomerID, @v_CompanyName, @v_ShipCity, @v_PostalCode, @v_RequiredDate
    );

    -- Log Dual-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_CustomerOrderGeoLedger', 'GeoAuditID', CAST(@nextGeoAuditID AS VARCHAR));
    
    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_CustomerOrderGeoLedger', 'GeoAuditID', CAST(@nextGeoAuditID AS VARCHAR));
    
    FETCH NEXT FROM GeoCursor INTO 
        @v_OrderID, @v_CustomerID, @v_CompanyName, @v_ShipCity, @v_PostalCode, @v_RequiredDate;
END;

CLOSE GeoCursor; 
DEALLOCATE GeoCursor;
GO