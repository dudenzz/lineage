-- Section: Create a Physical Table using Selection, Projection, Joins, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Regional Customer-Shipper Logistical Friction & Performance Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_LogisticalFrictionLedger', 'U') IS NOT NULL DROP TABLE Table_LogisticalFrictionLedger;
CREATE TABLE Table_LogisticalFrictionLedger (
    FrictionAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    CustomerID NCHAR(5),       -- Native Projection from Customers
    ShipperID INT,             -- Native Projection from Shippers
    ShipCountry NVARCHAR(15),  -- Native Projection from Orders
    KineticLogisticsEnergy FLOAT, -- Non-Linear Transformation: (Freight * Freight) / 2.0
    RegionalComplexityScore FLOAT, -- Non-Linear Transformation: SQRT(Freight * ShipperID)
    MarketDensityCoefficient FLOAT -- Non-Linear Transformation: (Freight * 1.0) / LOG(CAST(LEN(CustomerID) AS FLOAT) + 2.0)
);
GO

DECLARE @v_OrderID INT,
        @v_CustomerID NCHAR(5),
        @v_ShipperID INT,
        @v_ShipCountry NVARCHAR(15),
        @v_Freight MONEY,
        @v_KineticLogisticsEnergy FLOAT,
        @v_RegionalComplexityScore FLOAT,
        @v_MarketDensityCoefficient FLOAT,
        @nextFrictionAuditID INT;

-- Cursor using JOIN to integrate Customer demographics, Order transit data, and Carrier metadata.
-- Selection: Only orders bound for 'Sweden', 'France', or 'Spain' with a freight cost exceeding 35.00.
-- Transformations:
-- 1. Kinetic Logistics Energy: A parabolic scaling of freight cost to model exponential increases in resource burn.
-- 2. Regional Complexity Score: Square root interaction between carrier tier and shipping overhead.
-- 3. Market Density Coefficient: Inverse logarithmic scaling of freight relative to customer identifier length.
DECLARE FrictionCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        C.CustomerID, 
        S.ShipperID,
        O.ShipCountry,
        O.Freight
    FROM Orders O
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    WHERE O.ShipCountry IN ('Sweden', 'France', 'Spain') AND O.Freight > 35.00;

OPEN FrictionCursor;
FETCH NEXT FROM FrictionCursor INTO 
    @v_OrderID, @v_CustomerID, @v_ShipperID, @v_ShipCountry, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextFrictionAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_KineticLogisticsEnergy = (POWER(CAST(@v_Freight AS FLOAT), 2)) / 2.0;
    SET @v_RegionalComplexityScore = SQRT(CAST(@v_Freight AS FLOAT) * CAST(@v_ShipperID AS FLOAT));
    SET @v_MarketDensityCoefficient = CAST(@v_Freight AS FLOAT) / LOG(CAST(LEN(@v_CustomerID) AS FLOAT) + 2.0);

    INSERT INTO Table_LogisticalFrictionLedger (
        FrictionAuditID, OrderID, CustomerID, ShipperID, ShipCountry, 
        KineticLogisticsEnergy, RegionalComplexityScore, MarketDensityCoefficient
    )
    VALUES (
        @nextFrictionAuditID, @v_OrderID, @v_CustomerID, @v_ShipperID, @v_ShipCountry, 
        @v_KineticLogisticsEnergy, @v_RegionalComplexityScore, @v_MarketDensityCoefficient
    );

    -- Log Triple-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_LogisticalFrictionLedger', 'FrictionAuditID', CAST(@nextFrictionAuditID AS VARCHAR));
    
    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_LogisticalFrictionLedger', 'FrictionAuditID', CAST(@nextFrictionAuditID AS VARCHAR));

    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_LogisticalFrictionLedger', 'FrictionAuditID', CAST(@nextFrictionAuditID AS VARCHAR));
    
    FETCH NEXT FROM FrictionCursor INTO 
        @v_OrderID, @v_CustomerID, @v_ShipperID, @v_ShipCountry, @v_Freight;
END;

CLOSE FrictionCursor; 
DEALLOCATE FrictionCursor;
GO