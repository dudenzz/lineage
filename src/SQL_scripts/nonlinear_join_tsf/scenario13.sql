-- Section: Create a Physical Table using Selection, Projection, Joins (2 links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Employee-Customer Account Value & Relationship Momentum Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Orders -> Employees, Orders -> Customers).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, powers, or square roots.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_RelationshipMomentumLedger', 'U') IS NOT NULL DROP TABLE Table_RelationshipMomentumLedger;
CREATE TABLE Table_RelationshipMomentumLedger (
    MomentumAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    EmployeeID INT,            -- Native Projection from Employees
    CustomerID NCHAR(5),        -- Native Projection from Customers
    ShipRegion NVARCHAR(15),   -- Native Projection from Orders
    TransactionalForce MONEY,  -- Non-Linear Transformation: (Freight * EmployeeID)
    AccountVelocityScore FLOAT, -- Non-Linear Transformation: SQRT(Freight * LEN(CustomerID))
    ServiceSaturationIndex FLOAT -- Non-Linear Transformation: POWER(EmployeeID, 1.2) / LOG(Freight + 3.0)
);
GO

DECLARE @v_OrderID INT,
        @v_EmployeeID INT,
        @v_CustomerID NCHAR(5),
        @v_ShipRegion NVARCHAR(15),
        @v_Freight MONEY,
        @v_TransactionalForce MONEY,
        @v_AccountVelocityScore FLOAT,
        @v_ServiceSaturationIndex FLOAT,
        @nextMomentumAuditID INT;

-- Cursor using two table links branched from the central Orders entity to personnel and client data.
-- Selection: Only orders where a ShipRegion is specified, handled by 'Sales Representatives' for clients in 'UK', 'Canada', or 'USA'.
-- Transformations:
-- 1. Transactional Force: Models the "weight" of a transaction as a product of cost and staff seniority (A' = A * B).
-- 2. Account Velocity: Square root of freight cost interacted with customer ID length to proxy account complexity.
-- 3. Service Saturation: Exponential staff factor dampened by the logarithmic growth of freight volume.
DECLARE MomentumLinkCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        E.EmployeeID, 
        C.CustomerID,
        O.ShipRegion,
        O.Freight
    FROM Orders O
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID -- Link 1: Internal Staff
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID -- Link 2: External Client
    WHERE O.ShipRegion IS NOT NULL 
      AND E.Title = 'Sales Representative'
      AND C.Country IN ('UK', 'Canada', 'USA');

OPEN MomentumLinkCursor;
FETCH NEXT FROM MomentumLinkCursor INTO 
    @v_OrderID, @v_EmployeeID, @v_CustomerID, @v_ShipRegion, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextMomentumAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_TransactionalForce = @v_Freight * CAST(@v_EmployeeID AS FLOAT);
    SET @v_AccountVelocityScore = SQRT(CAST(@v_Freight AS FLOAT) * CAST(LEN(@v_CustomerID) AS FLOAT));
    SET @v_ServiceSaturationIndex = POWER(CAST(@v_EmployeeID AS FLOAT), 1.2) / LOG(CAST(@v_Freight AS FLOAT) + 3.0);

    INSERT INTO Table_RelationshipMomentumLedger (
        MomentumAuditID, OrderID, EmployeeID, CustomerID, ShipRegion, 
        TransactionalForce, AccountVelocityScore, ServiceSaturationIndex
    )
    VALUES (
        @nextMomentumAuditID, @v_OrderID, @v_EmployeeID, @v_CustomerID, @v_ShipRegion, 
        @v_TransactionalForce, @v_AccountVelocityScore, @v_ServiceSaturationIndex
    );

    -- Log Triple-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_RelationshipMomentumLedger', 'MomentumAuditID', CAST(@nextMomentumAuditID AS VARCHAR));
    
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_RelationshipMomentumLedger', 'MomentumAuditID', CAST(@nextMomentumAuditID AS VARCHAR));

    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_RelationshipMomentumLedger', 'MomentumAuditID', CAST(@nextMomentumAuditID AS VARCHAR));
    
    FETCH NEXT FROM MomentumLinkCursor INTO 
        @v_OrderID, @v_EmployeeID, @v_CustomerID, @v_ShipRegion, @v_Freight;
END;

CLOSE MomentumLinkCursor; 
DEALLOCATE MomentumLinkCursor;
GO