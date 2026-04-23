-- Section: Create a Physical Table using Selection, Projection, Joins (2 links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Category-Shipper Logistics Efficiency & Transit Mass Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Orders -> Shippers, Orders -> [Order Details] -> Products -> Categories).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Powers, or Square Roots.
-- Lineage: Tracks specific source table and primary key for every entity in the relationship.

IF OBJECT_ID('Table_CategoryShipperEfficiencyLedger', 'U') IS NOT NULL DROP TABLE Table_CategoryShipperEfficiencyLedger;
CREATE TABLE Table_CategoryShipperEfficiencyLedger (
    EfficiencyAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    ShipperID INT,             -- Native Projection from Shippers
    CategoryID INT,            -- Native Projection from Categories
    ShipCountry NVARCHAR(15),  -- Native Projection from Orders
    TransitMomentum MONEY,     -- Non-Linear Transformation: (Freight * CategoryID)
    LogisticsInertiaScore FLOAT, -- Non-Linear Transformation: SQRT(Freight * ShipperID)
    MarketFrictionCoefficient FLOAT -- Non-Linear Transformation: POWER(Freight, 1.15) / LOG(CategoryID + 1.5)
);
GO

DECLARE @v_OrderID INT,
        @v_ShipperID INT,
        @v_CategoryID INT,
        @v_ShipCountry NVARCHAR(15),
        @v_Freight MONEY,
        @v_TransitMomentum MONEY,
        @v_LogisticsInertiaScore FLOAT,
        @v_MarketFrictionCoefficient FLOAT,
        @nextEfficiencyAuditID INT;

-- Cursor using two table links branched from the Orders entity.
-- Selection: Only orders shipped to 'Italy', 'Spain', or 'Portugal' (Mediterranean Hub) with Freight > 20.00.
-- Transformations:
-- 1. Transit Momentum: Non-linear product of cost and category tier (A' = A * B).
-- 2. Logistics Inertia: Square root of the interaction between freight and the carrier ID.
-- 3. Market Friction: Exponential growth of freight cost dampened by a logarithmic category factor.
DECLARE EfficiencyLinkCursor CURSOR FOR 
    SELECT DISTINCT
        O.OrderID, 
        S.ShipperID, 
        C.CategoryID,
        O.ShipCountry,
        O.Freight
    FROM Orders O
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID -- Link 1: Carrier Metadata
    INNER JOIN [Order Details] OD ON O.OrderID = OD.OrderID
    INNER JOIN Products P ON OD.ProductID = P.ProductID
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID -- Link 2: Category Taxonomy
    WHERE O.ShipCountry IN ('Italy', 'Spain', 'Portugal') 
      AND O.Freight > 20.00;

OPEN EfficiencyLinkCursor;
FETCH NEXT FROM EfficiencyLinkCursor INTO 
    @v_OrderID, @v_ShipperID, @v_CategoryID, @v_ShipCountry, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextEfficiencyAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_TransitMomentum = @v_Freight * CAST(@v_CategoryID AS FLOAT);
    SET @v_LogisticsInertiaScore = SQRT(CAST(@v_Freight AS FLOAT) * CAST(@v_ShipperID AS FLOAT));
    SET @v_MarketFrictionCoefficient = POWER(CAST(@v_Freight AS FLOAT), 1.15) / LOG(CAST(@v_CategoryID AS FLOAT) + 1.5);

    INSERT INTO Table_CategoryShipperEfficiencyLedger (
        EfficiencyAuditID, OrderID, ShipperID, CategoryID, ShipCountry, 
        TransitMomentum, LogisticsInertiaScore, MarketFrictionCoefficient
    )
    VALUES (
        @nextEfficiencyAuditID, @v_OrderID, @v_ShipperID, @v_CategoryID, @v_ShipCountry, 
        @v_TransitMomentum, @v_LogisticsInertiaScore, @v_MarketFrictionCoefficient
    );

    -- Log Triple-Source Lineage (Central Table + Two Join Links)
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_CategoryShipperEfficiencyLedger', 'EfficiencyAuditID', CAST(@nextEfficiencyAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_CategoryShipperEfficiencyLedger', 'EfficiencyAuditID', CAST(@nextEfficiencyAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_CategoryShipperEfficiencyLedger', 'EfficiencyAuditID', CAST(@nextEfficiencyAuditID AS VARCHAR));
    
    FETCH NEXT FROM EfficiencyLinkCursor INTO 
        @v_OrderID, @v_ShipperID, @v_CategoryID, @v_ShipCountry, @v_Freight;
END;

CLOSE EfficiencyLinkCursor; 
DEALLOCATE EfficiencyLinkCursor;
GO