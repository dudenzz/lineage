-- Section: Create a Physical Table using Selection, Projection, Joins (2 links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Category-Shipper Logistics Synergy & Volume Turbulence Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Orders -> Shippers, Orders -> [Order Details] -> Products -> Categories).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithms.
-- Lineage: Tracks specific source table and primary key for every entity in the relationship.

IF OBJECT_ID('Table_CategoryLogisticsSynergyLedger', 'U') IS NOT NULL DROP TABLE Table_CategoryLogisticsSynergyLedger;
CREATE TABLE Table_CategoryLogisticsSynergyLedger (
    SynergyAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    ShipperID INT,             -- Native Projection from Shippers
    CategoryID INT,            -- Native Projection from Categories
    ShipName NVARCHAR(40),     -- Native Projection from Orders
    LogisticsFinancialMass MONEY, -- Non-Linear Transformation: (Freight * CategoryID)
    TransitCurvatureScore FLOAT,  -- Non-Linear Transformation: SQRT(Freight * ShipperID)
    CarrierSaturationIndex FLOAT  -- Non-Linear Transformation: POWER(Freight, 1.1) / LOG(ShipperID + 1.5)
);
GO

DECLARE @v_OrderID INT,
        @v_ShipperID INT,
        @v_CategoryID INT,
        @v_ShipName NVARCHAR(40),
        @v_Freight MONEY,
        @v_LogisticsFinancialMass MONEY,
        @v_TransitCurvatureScore FLOAT,
        @v_CarrierSaturationIndex FLOAT,
        @nextSynergyAuditID INT;

-- Cursor using two table links branched from the central Orders entity.
-- Selection: Only orders shipped via carriers 1 or 3, containing products from 'Grains/Cereals' or 'Produce'.
-- Transformations:
-- 1. Logistics Financial Mass: A non-linear product of transit cost and category tier (A' = A * B).
-- 2. Transit Curvature Score: Square root interaction modeling the friction between freight and carrier ID.
-- 3. Carrier Saturation Index: Exponential freight scaling dampened by the logarithmic carrier identifier.
DECLARE SynergyLinkCursor CURSOR FOR 
    SELECT DISTINCT
        O.OrderID, 
        S.ShipperID, 
        C.CategoryID,
        O.ShipName,
        O.Freight
    FROM Orders O
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID -- Link 1: Carrier Data
    INNER JOIN [Order Details] OD ON O.OrderID = OD.OrderID
    INNER JOIN Products P ON OD.ProductID = P.ProductID
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID -- Link 2: Category Taxonomy
    WHERE S.ShipperID IN (1, 3) 
      AND C.CategoryName IN ('Grains/Cereals', 'Produce');

OPEN SynergyLinkCursor;
FETCH NEXT FROM SynergyLinkCursor INTO 
    @v_OrderID, @v_ShipperID, @v_CategoryID, @v_ShipName, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextSynergyAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_LogisticsFinancialMass = @v_Freight * CAST(@v_CategoryID AS FLOAT);
    SET @v_TransitCurvatureScore = SQRT(CAST(@v_Freight AS FLOAT) * CAST(@v_ShipperID AS FLOAT));
    SET @v_CarrierSaturationIndex = POWER(CAST(@v_Freight AS FLOAT), 1.1) / LOG(CAST(@v_ShipperID AS FLOAT) + 1.5);

    INSERT INTO Table_CategoryLogisticsSynergyLedger (
        SynergyAuditID, OrderID, ShipperID, CategoryID, ShipName, 
        LogisticsFinancialMass, TransitCurvatureScore, CarrierSaturationIndex
    )
    VALUES (
        @nextSynergyAuditID, @v_OrderID, @v_ShipperID, @v_CategoryID, @v_ShipName, 
        @v_LogisticsFinancialMass, @v_TransitCurvatureScore, @v_CarrierSaturationIndex
    );

    -- Log Triple-Source Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_CategoryLogisticsSynergyLedger', 'SynergyAuditID', CAST(@nextSynergyAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_CategoryLogisticsSynergyLedger', 'SynergyAuditID', CAST(@nextSynergyAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_CategoryLogisticsSynergyLedger', 'SynergyAuditID', CAST(@nextSynergyAuditID AS VARCHAR));
    
    FETCH NEXT FROM SynergyLinkCursor INTO 
        @v_OrderID, @v_ShipperID, @v_CategoryID, @v_ShipName, @v_Freight;
END;

CLOSE SynergyLinkCursor; 
DEALLOCATE SynergyLinkCursor;
GO