-- Section: Create a Physical Table using Selection, Projection, Joins (2 links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Employee-Shipper Fulfillment Velocity & Management Entropy Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Orders -> Employees, Orders -> Shippers).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the relationship.

IF OBJECT_ID('Table_FulfillmentEntropyLedger', 'U') IS NOT NULL DROP TABLE Table_FulfillmentEntropyLedger;
CREATE TABLE Table_FulfillmentEntropyLedger (
    EntropyAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    EmployeeID INT,             -- Native Projection from Employees
    ShipperID INT,             -- Native Projection from Shippers
    ShipCity NVARCHAR(15),     -- Native Projection from Orders
    LogisticsPowerIndex FLOAT,  -- Non-Linear Transformation: (Freight * EmployeeID)
    CarrierFrictionScore FLOAT, -- Non-Linear Transformation: SQRT(Freight * ShipperID)
    SupervisoryDampening FLOAT  -- Non-Linear Transformation: LOG(Freight + 10) / SQRT(EmployeeID)
);
GO

DECLARE @v_OrderID INT,
        @v_EmployeeID INT,
        @v_ShipperID INT,
        @v_ShipCity NVARCHAR(15),
        @v_Freight MONEY,
        @v_LogisticsPowerIndex FLOAT,
        @v_CarrierFrictionScore FLOAT,
        @v_SupervisoryDampening FLOAT,
        @nextEntropyAuditID INT;

-- Cursor using two table links branched from the central Orders entity.
-- Selection: Only orders handled by 'Sales Representatives' shipped to 'London', 'Paris', or 'Berlin'.
-- Transformations:
-- 1. Logistics Power Index: A non-linear valuation of throughput mass based on employee seniority (A' = A * B).
-- 2. Carrier Friction Score: Square root interaction modeling the resistance between freight value and carrier tier.
-- 3. Supervisory Dampening: Logarithmic scaling of freight volume divided by the personnel square root to model managerial overhead.
DECLARE EntropyLinkCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        E.EmployeeID, 
        S.ShipperID,
        O.ShipCity,
        O.Freight
    FROM Orders O
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID -- Link 1: Personnel Data
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID   -- Link 2: Carrier Data
    WHERE E.Title = 'Sales Representative' 
      AND O.ShipCity IN ('London', 'Paris', 'Berlin');

OPEN EntropyLinkCursor;
FETCH NEXT FROM EntropyLinkCursor INTO 
    @v_OrderID, @v_EmployeeID, @v_ShipperID, @v_ShipCity, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextEntropyAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_LogisticsPowerIndex = CAST(@v_Freight AS FLOAT) * CAST(@v_EmployeeID AS FLOAT);
    SET @v_CarrierFrictionScore = SQRT(CAST(@v_Freight AS FLOAT) * CAST(@v_ShipperID AS FLOAT));
    SET @v_SupervisoryDampening = LOG(CAST(@v_Freight AS FLOAT) + 10.0) / SQRT(CAST(@v_EmployeeID AS FLOAT));

    INSERT INTO Table_FulfillmentEntropyLedger (
        EntropyAuditID, OrderID, EmployeeID, ShipperID, ShipCity, 
        LogisticsPowerIndex, CarrierFrictionScore, SupervisoryDampening
    )
    VALUES (
        @nextEntropyAuditID, @v_OrderID, @v_EmployeeID, @v_ShipperID, @v_ShipCity, 
        @v_LogisticsPowerIndex, @v_CarrierFrictionScore, @v_SupervisoryDampening
    );

    -- Log Triple-Source Lineage (Central Table + Two Join Links)
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_FulfillmentEntropyLedger', 'EntropyAuditID', CAST(@nextEntropyAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_FulfillmentEntropyLedger', 'EntropyAuditID', CAST(@nextEntropyAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_FulfillmentEntropyLedger', 'EntropyAuditID', CAST(@nextEntropyAuditID AS VARCHAR));
    
    FETCH NEXT FROM EntropyLinkCursor INTO 
        @v_OrderID, @v_EmployeeID, @v_ShipperID, @v_ShipCity, @v_Freight;
END;

CLOSE EntropyLinkCursor; 
DEALLOCATE EntropyLinkCursor;
GO