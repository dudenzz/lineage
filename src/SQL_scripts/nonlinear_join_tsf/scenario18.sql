-- Section: Create a Physical Table using Selection, Projection, Joins (3 table links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Employee-Customer-Shipper Regional Impact & Market Velocity Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link: Orders -> Employees, Orders -> Customers, Orders -> Shippers).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_RegionalMarketDynamicsLedger', 'U') IS NOT NULL DROP TABLE Table_RegionalMarketDynamicsLedger;
CREATE TABLE Table_RegionalMarketDynamicsLedger (
    DynamicsAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    EmployeeID INT,            -- Native Projection from Employees
    CustomerID NCHAR(5),       -- Native Projection from Customers
    ShipperID INT,             -- Native Projection from Shippers
    ShipCountry NVARCHAR(15),  -- Native Projection from Orders
    FulfillmentMomentum MONEY, -- Non-Linear Transformation: (Freight * EmployeeID)
    ClientCoverageEntropy FLOAT, -- Non-Linear Transformation: SQRT(Freight * LEN(CustomerID))
    CarrierVelocityIndex FLOAT  -- Non-Linear Transformation: POWER(Freight, 1.1) / LOG(ShipperID + 2.0)
);
GO

DECLARE @v_OrderID INT,
        @v_EmployeeID INT,
        @v_CustomerID NCHAR(5),
        @v_ShipperID INT,
        @v_ShipCountry NVARCHAR(15),
        @v_Freight MONEY,
        @v_FulfillmentMomentum MONEY,
        @v_ClientCoverageEntropy FLOAT,
        @v_CarrierVelocityIndex FLOAT,
        @nextDynamicsAuditID INT;

-- Cursor using three distinct table links branched from the central Orders entity.
-- Selection: Only orders handled by 'Sales Representatives' for customers in 'Scandinavia' (Sweden, Norway, Denmark, Finland).
-- Transformations:
-- 1. Fulfillment Momentum: Models the logistical force of a transaction as a product of cost and staff ID (A' = A * B).
-- 2. Client Coverage Entropy: Square root interaction between freight volume and customer identifier length.
-- 3. Carrier Velocity Index: Exponential freight scaling dampened by the carrier identifier to model regional transit speed.
DECLARE DynamicsLinkCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        E.EmployeeID, 
        C.CustomerID,
        S.ShipperID,
        O.ShipCountry,
        O.Freight
    FROM Orders O
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID -- Link 1: Internal Personnel
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID -- Link 2: External Client
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID   -- Link 3: Logistics Carrier
    WHERE E.Title = 'Sales Representative' 
      AND O.ShipCountry IN ('Sweden', 'Norway', 'Denmark', 'Finland');

OPEN DynamicsLinkCursor;
FETCH NEXT FROM DynamicsLinkCursor INTO 
    @v_OrderID, @v_EmployeeID, @v_CustomerID, @v_ShipperID, @v_ShipCountry, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextDynamicsAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_FulfillmentMomentum = @v_Freight * CAST(@v_EmployeeID AS FLOAT);
    SET @v_ClientCoverageEntropy = SQRT(CAST(@v_Freight AS FLOAT) * CAST(LEN(@v_CustomerID) AS FLOAT));
    SET @v_CarrierVelocityIndex = POWER(CAST(@v_Freight AS FLOAT), 1.1) / LOG(CAST(@v_ShipperID AS FLOAT) + 2.0);

    INSERT INTO Table_RegionalMarketDynamicsLedger (
        DynamicsAuditID, OrderID, EmployeeID, CustomerID, ShipperID, ShipCountry, 
        FulfillmentMomentum, ClientCoverageEntropy, CarrierVelocityIndex
    )
    VALUES (
        @nextDynamicsAuditID, @v_OrderID, @v_EmployeeID, @v_CustomerID, @v_ShipperID, @v_ShipCountry, 
        @v_FulfillmentMomentum, @v_ClientCoverageEntropy, @v_CarrierVelocityIndex
    );

    -- Log Quad-Source Lineage (Central Table + Three Regional Links)
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_RegionalMarketDynamicsLedger', 'DynamicsAuditID', CAST(@nextDynamicsAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_RegionalMarketDynamicsLedger', 'DynamicsAuditID', CAST(@nextDynamicsAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_RegionalMarketDynamicsLedger', 'DynamicsAuditID', CAST(@nextDynamicsAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_RegionalMarketDynamicsLedger', 'DynamicsAuditID', CAST(@nextDynamicsAuditID AS VARCHAR));
    
    FETCH NEXT FROM DynamicsLinkCursor INTO 
        @v_OrderID, @v_EmployeeID, @v_CustomerID, @v_ShipperID, @v_ShipCountry, @v_Freight;
END;

CLOSE DynamicsLinkCursor; 
DEALLOCATE DynamicsLinkCursor;
GO