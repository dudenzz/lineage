-- Section: Create a Physical Table using Selection, Projection, Joins, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Customer Engagement Dynamics & Geometric Logistics Risk Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_EngagementDynamicsLedger', 'U') IS NOT NULL DROP TABLE Table_EngagementDynamicsLedger;
CREATE TABLE Table_EngagementDynamicsLedger (
    EngagementAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    CustomerID NCHAR(5),     -- Native Projection from Customers
    EmployeeID INT,          -- Native Projection from Employees
    ShipName NVARCHAR(40),   -- Native Projection from Orders
    LogisticsIntensity FLOAT, -- Non-Linear Transformation: SQRT(Freight * EmployeeID)
    TransactionGeometricVolume MONEY, -- Non-Linear Transformation: (Freight * OrderID / 100.0)
    EngagementDecayFactor FLOAT -- Non-Linear Transformation: (EmployeeID * 1.0) / LOG(OrderID + 2.0)
);
GO

DECLARE @v_OrderID INT,
        @v_CustomerID NCHAR(5),
        @v_EmployeeID INT,
        @v_ShipName NVARCHAR(40),
        @v_Freight MONEY,
        @v_LogisticsIntensity FLOAT,
        @v_TransactionGeometricVolume MONEY,
        @v_EngagementDecayFactor FLOAT,
        @nextEngagementAuditID INT;

-- Cursor using JOIN to integrate Account Ownership, Geographic Shipping, and Sales Personnel data.
-- Selection: Only orders shipped to 'USA', 'UK', or 'Ireland' where the Freight cost is > 50.00.
-- Transformations:
-- 1. Logistics Intensity: Square root of the interaction between freight cost and employee ID (representing handling complexity).
-- 2. Transaction Geometric Volume: A non-linear scaling of freight weight relative to the unique order sequence (A' = A * B).
-- 3. Engagement Decay: Logarithmic dampening of employee influence based on order depth to model service saturation.
DECLARE EngagementCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        C.CustomerID, 
        E.EmployeeID,
        O.ShipName,
        O.Freight
    FROM Orders O
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID
    WHERE O.ShipCountry IN ('USA', 'UK', 'Ireland') 
      AND O.Freight > 50.00;

OPEN EngagementCursor;
FETCH NEXT FROM EngagementCursor INTO 
    @v_OrderID, @v_CustomerID, @v_EmployeeID, @v_ShipName, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextEngagementAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_LogisticsIntensity = SQRT(CAST(@v_Freight AS FLOAT) * CAST(@v_EmployeeID AS FLOAT));
    SET @v_TransactionGeometricVolume = @v_Freight * (CAST(@v_OrderID AS FLOAT) / 100.0);
    SET @v_EngagementDecayFactor = (CAST(@v_EmployeeID AS FLOAT) * 1.0) / LOG(CAST(@v_OrderID AS FLOAT) + 2.0);

    INSERT INTO Table_EngagementDynamicsLedger (
        EngagementAuditID, OrderID, CustomerID, EmployeeID, ShipName, 
        LogisticsIntensity, TransactionGeometricVolume, EngagementDecayFactor
    )
    VALUES (
        @nextEngagementAuditID, @v_OrderID, @v_CustomerID, @v_EmployeeID, @v_ShipName, 
        @v_LogisticsIntensity, @v_TransactionGeometricVolume, @v_EngagementDecayFactor
    );

    -- Log Triple-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_EngagementDynamicsLedger', 'EngagementAuditID', CAST(@nextEngagementAuditID AS VARCHAR));
    
    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_EngagementDynamicsLedger', 'EngagementAuditID', CAST(@nextEngagementAuditID AS VARCHAR));

    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_EngagementDynamicsLedger', 'EngagementAuditID', CAST(@nextEngagementAuditID AS VARCHAR));
    
    FETCH NEXT FROM EngagementCursor INTO 
        @v_OrderID, @v_CustomerID, @v_EmployeeID, @v_ShipName, @v_Freight;
END;

CLOSE EngagementCursor; 
DEALLOCATE EngagementCursor;
GO