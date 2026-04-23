-- Section: Create a Physical Table using Selection, Projection, Joins, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Shipper-Carrier Performance & Logistical Turbulence Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, SQRT, or Power functions.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_CarrierTurbulenceLedger', 'U') IS NOT NULL DROP TABLE Table_CarrierTurbulenceLedger;
CREATE TABLE Table_CarrierTurbulenceLedger (
    LogisticsAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    ShipperID INT,             -- Native Projection from Shippers
    ProductID INT,             -- Native Projection from Products
    CompanyName NVARCHAR(40),   -- Native Projection from Shippers
    LogisticalFrictionIndex FLOAT, -- Non-Linear Transformation: (Freight * UnitsOnOrder)
    TransitVolatilityScore FLOAT,  -- Non-Linear Transformation: SQRT(UnitPrice * Freight)
    ComplexityCoefficient FLOAT    -- Non-Linear Transformation: POWER(ShipperID, 1.5) / LOG(Freight + 2)
);
GO

DECLARE @v_OrderID INT,
        @v_ShipperID INT,
        @v_ProductID INT,
        @v_CompanyName NVARCHAR(40),
        @v_Freight MONEY,
        @v_UnitsOnOrder SMALLINT,
        @v_UnitPrice MONEY,
        @v_LogisticalFrictionIndex FLOAT,
        @v_TransitVolatilityScore FLOAT,
        @v_ComplexityCoefficient FLOAT,
        @nextLogisticsAuditID INT;

-- Cursor using JOIN to integrate Shipping Carriers, Transactional Orders, and Product Specifications.
-- Selection: Only orders shipped to 'Germany', 'France', or 'Belgium' where UnitsOnOrder is greater than 0.
-- Transformations:
-- 1. Logistical Friction: Models the interaction between mass (units) and cost (freight) (A' = A * B).
-- 2. Transit Volatility: Square root of the product of value and transit cost to identify high-risk shipments.
-- 3. Complexity Coefficient: Non-linear scaling of the carrier tier relative to the logarithmic dampening of freight cost.
DECLARE TurbulenceCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        S.ShipperID, 
        P.ProductID,
        S.CompanyName,
        O.Freight,
        P.UnitsOnOrder,
        P.UnitPrice
    FROM Orders O
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    INNER JOIN [Order Details] OD ON O.OrderID = OD.OrderID
    INNER JOIN Products P ON OD.ProductID = P.ProductID
    WHERE O.ShipCountry IN ('Germany', 'France', 'Belgium') 
      AND P.UnitsOnOrder > 0;

OPEN TurbulenceCursor;
FETCH NEXT FROM TurbulenceCursor INTO 
    @v_OrderID, @v_ShipperID, @v_ProductID, @v_CompanyName, @v_Freight, @v_UnitsOnOrder, @v_UnitPrice;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLogisticsAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_LogisticalFrictionIndex = CAST(@v_Freight AS FLOAT) * CAST(@v_UnitsOnOrder AS FLOAT);
    SET @v_TransitVolatilityScore = SQRT(CAST(@v_UnitPrice AS FLOAT) * CAST(@v_Freight AS FLOAT));
    SET @v_ComplexityCoefficient = POWER(CAST(@v_ShipperID AS FLOAT), 1.5) / LOG(CAST(@v_Freight AS FLOAT) + 2.0);

    INSERT INTO Table_CarrierTurbulenceLedger (
        LogisticsAuditID, OrderID, ShipperID, ProductID, CompanyName, 
        LogisticalFrictionIndex, TransitVolatilityScore, ComplexityCoefficient
    )
    VALUES (
        @nextLogisticsAuditID, @v_OrderID, @v_ShipperID, @v_ProductID, @v_CompanyName, 
        @v_LogisticalFrictionIndex, @v_TransitVolatilityScore, @v_ComplexityCoefficient
    );

    -- Log Triple-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_CarrierTurbulenceLedger', 'LogisticsAuditID', CAST(@nextLogisticsAuditID AS VARCHAR));
    
    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_CarrierTurbulenceLedger', 'LogisticsAuditID', CAST(@nextLogisticsAuditID AS VARCHAR));

    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_CarrierTurbulenceLedger', 'LogisticsAuditID', CAST(@nextLogisticsAuditID AS VARCHAR));
    
    FETCH NEXT FROM TurbulenceCursor INTO 
        @v_OrderID, @v_ShipperID, @v_ProductID, @v_CompanyName, @v_Freight, @v_UnitsOnOrder, @v_UnitPrice;
END;

CLOSE TurbulenceCursor; 
DEALLOCATE TurbulenceCursor;
GO