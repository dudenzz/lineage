-- Section: Create a Physical Table using Selection, Projection, Joins (3 table links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Supplier-Category-Shipper Strategic Logistics Efficiency Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link: Products -> Suppliers, Products -> Categories, Products -> [Order Details] -> Orders -> Shippers).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_StrategicLogisticsEntropyLedger', 'U') IS NOT NULL DROP TABLE Table_StrategicLogisticsEntropyLedger;
CREATE TABLE Table_StrategicLogisticsEntropyLedger (
    StrategicAuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    CategoryID INT,          -- Native Projection from Categories
    ShipperID INT,           -- Native Projection from Shippers
    ProductName NVARCHAR(40), -- Native Projection from Products
    OperationalMomentum MONEY, -- Non-Linear Transformation: (UnitPrice * UnitsInStock)
    GlobalFrictionIndex FLOAT, -- Non-Linear Transformation: SQRT(UnitPrice * Freight)
    SourcingVolatility FLOAT   -- Non-Linear Transformation: POWER(SupplierID, 1.1) / LOG(ShipperID + 1.5)
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_CategoryID INT,
        @v_ShipperID INT,
        @v_ProductName NVARCHAR(40),
        @v_UnitPrice MONEY,
        @v_UnitsInStock SMALLINT,
        @v_Freight MONEY,
        @v_OperationalMomentum MONEY,
        @v_GlobalFrictionIndex FLOAT,
        @v_SourcingVolatility FLOAT,
        @nextStrategicAuditID INT;

-- Cursor using three distinct table links branched from the Products/Order Details intersection.
-- Selection: Only products in 'Beverages' or 'Condiments' from suppliers in 'UK' or 'USA' shipped via 'Speedy Express' or 'United Package'.
-- Transformations:
-- 1. Operational Momentum: Non-linear capital weight of current stock (A' = A * B).
-- 2. Global Friction Index: Square root of the interaction between unit value and transit overhead.
-- 3. Sourcing Volatility: Exponential supplier tiering dampened by the logarithmic carrier identifier to model network instability.
DECLARE StrategicLinkCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        C.CategoryID,
        SH.ShipperID,
        P.ProductName,
        P.UnitPrice,
        P.UnitsInStock,
        O.Freight
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID -- Link 1: Sourcing
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID -- Link 2: Taxonomy
    INNER JOIN [Order Details] OD ON P.ProductID = OD.ProductID
    INNER JOIN Orders O ON OD.OrderID = O.OrderID
    INNER JOIN Shippers SH ON O.ShipVia = SH.ShipperID   -- Link 3: Fulfillment
    WHERE C.CategoryName IN ('Beverages', 'Condiments')
      AND S.Country IN ('UK', 'USA')
      AND SH.CompanyName IN ('Speedy Express', 'United Package');

OPEN StrategicLinkCursor;
FETCH NEXT FROM StrategicLinkCursor INTO 
    @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ShipperID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextStrategicAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_OperationalMomentum = @v_UnitPrice * @v_UnitsInStock;
    SET @v_GlobalFrictionIndex = SQRT(CAST(@v_UnitPrice AS FLOAT) * CAST(@v_Freight AS FLOAT));
    SET @v_SourcingVolatility = POWER(CAST(@v_SupplierID AS FLOAT), 1.1) / LOG(CAST(@v_ShipperID AS FLOAT) + 1.5);

    INSERT INTO Table_StrategicLogisticsEntropyLedger (
        StrategicAuditID, ProductID, SupplierID, CategoryID, ShipperID, ProductName, 
        OperationalMomentum, GlobalFrictionIndex, SourcingVolatility
    )
    VALUES (
        @nextStrategicAuditID, @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ShipperID, @v_ProductName, 
        @v_OperationalMomentum, @v_GlobalFrictionIndex, @v_SourcingVolatility
    );

    -- Log Quad-Source Lineage (Central Table + Three Strategic Links)
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_StrategicLogisticsEntropyLedger', 'StrategicAuditID', CAST(@nextStrategicAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_StrategicLogisticsEntropyLedger', 'StrategicAuditID', CAST(@nextStrategicAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_StrategicLogisticsEntropyLedger', 'StrategicAuditID', CAST(@nextStrategicAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_StrategicLogisticsEntropyLedger', 'StrategicAuditID', CAST(@nextStrategicAuditID AS VARCHAR));
    
    FETCH NEXT FROM StrategicLinkCursor INTO 
        @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ShipperID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock, @v_Freight;
END;

CLOSE StrategicLinkCursor; 
DEALLOCATE StrategicLinkCursor;
GO