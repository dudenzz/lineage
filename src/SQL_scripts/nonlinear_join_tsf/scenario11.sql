-- Section: Create a Physical Table using Selection, Projection, Joins (2 links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Customer-Territory Market Influence & Regional Penetration Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Orders -> Customers, Orders -> Employees -> EmployeeTerritories -> Territories).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the relationship.

IF OBJECT_ID('Table_MarketInfluenceLedger', 'U') IS NOT NULL DROP TABLE Table_MarketInfluenceLedger;
CREATE TABLE Table_MarketInfluenceLedger (
    InfluenceAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    CustomerID NCHAR(5),       -- Native Projection from Customers
    TerritoryID NVARCHAR(20),  -- Native Projection from Territories
    ShipCountry NVARCHAR(15),  -- Native Projection from Orders
    InfluenceMass MONEY,       -- Non-Linear Transformation: (Freight * LEN(CustomerID))
    GeographicVelocity FLOAT,  -- Non-Linear Transformation: SQRT(Freight * CAST(LEFT(TerritoryID, 5) AS INT)) -- Partial numeric cast
    SaturationCurvature FLOAT  -- Non-Linear Transformation: POWER(Freight, 1.2) / LOG(CAST(LEN(ShipCountry) AS FLOAT) + 2.0)
);
GO

DECLARE @v_OrderID INT,
        @v_CustomerID NCHAR(5),
        @v_TerritoryID NVARCHAR(20),
        @v_ShipCountry NVARCHAR(15),
        @v_Freight MONEY,
        @v_InfluenceMass MONEY,
        @v_GeographicVelocity FLOAT,
        @v_SaturationCurvature FLOAT,
        @nextInfluenceAuditID INT;

-- Cursor using two table links branched from the central Orders entity to demographics and regional coverage.
-- Selection: Only orders from 'Germany', 'Sweden', or 'Austria' involving territories in Region 1 (Northern).
-- Transformations:
-- 1. Influence Mass: Scales freight impact by the unique customer identifier complexity (A' = A * B).
-- 2. Geographic Velocity: Square root interaction between transit cost and territory identifiers to model regional spread.
-- 3. Saturation Curvature: Exponential growth of logistics cost dampened by the linguistic length of the destination country.
DECLARE InfluenceLinkCursor CURSOR FOR 
    SELECT DISTINCT
        O.OrderID, 
        C.CustomerID, 
        T.TerritoryID,
        O.ShipCountry,
        O.Freight
    FROM Orders O
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID -- Link 1: Customer Demographics
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID
    INNER JOIN EmployeeTerritories ET ON E.EmployeeID = ET.EmployeeID
    INNER JOIN Territories T ON ET.TerritoryID = T.TerritoryID -- Link 2: Territory Coverage
    WHERE O.ShipCountry IN ('Germany', 'Sweden', 'Austria') 
      AND T.RegionID = 1;

OPEN InfluenceLinkCursor;
FETCH NEXT FROM InfluenceLinkCursor INTO 
    @v_OrderID, @v_CustomerID, @v_TerritoryID, @v_ShipCountry, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextInfluenceAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_InfluenceMass = @v_Freight * LEN(@v_CustomerID);
    
    -- Numeric conversion logic for non-linear processing of alphanumeric Territory IDs
    SET @v_GeographicVelocity = SQRT(CAST(@v_Freight AS FLOAT) * ABS(CHECKSUM(@v_TerritoryID) % 1000));
    
    SET @v_SaturationCurvature = POWER(CAST(@v_Freight AS FLOAT), 1.2) / LOG(CAST(LEN(@v_ShipCountry) AS FLOAT) + 2.0);

    INSERT INTO Table_MarketInfluenceLedger (
        InfluenceAuditID, OrderID, CustomerID, TerritoryID, ShipCountry, 
        InfluenceMass, GeographicVelocity, SaturationCurvature
    )
    VALUES (
        @nextInfluenceAuditID, @v_OrderID, @v_CustomerID, @v_TerritoryID, @v_ShipCountry, 
        @v_InfluenceMass, @v_GeographicVelocity, @v_SaturationCurvature
    );

    -- Log Triple-Source Lineage (Central Table + Two Main Join Links)
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_MarketInfluenceLedger', 'InfluenceAuditID', CAST(@nextInfluenceAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_MarketInfluenceLedger', 'InfluenceAuditID', CAST(@nextInfluenceAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', CAST(@v_TerritoryID AS VARCHAR), 'Table_MarketInfluenceLedger', 'InfluenceAuditID', CAST(@nextInfluenceAuditID AS VARCHAR));
    
    FETCH NEXT FROM InfluenceLinkCursor INTO 
        @v_OrderID, @v_CustomerID, @v_TerritoryID, @v_ShipCountry, @v_Freight;
END;

CLOSE InfluenceLinkCursor; 
DEALLOCATE InfluenceLinkCursor;
GO