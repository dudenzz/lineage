-- Section: Create a Physical Table using Selection, Projection, Joins (3 table links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Supplier-Category-Shipper Network Resilience & Operational Entropy Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link: Products -> Suppliers, Products -> Categories, Products -> [Order Details] -> Orders -> Shippers).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_NetworkResilienceLedger', 'U') IS NOT NULL DROP TABLE Table_NetworkResilienceLedger;
CREATE TABLE Table_NetworkResilienceLedger (
    ResilienceAuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    CategoryID INT,          -- Native Projection from Categories
    ShipperID INT,           -- Native Projection from Shippers
    ProductName NVARCHAR(40), -- Native Projection from Products
    InventoryFinancialMass MONEY, -- Non-Linear Transformation: (UnitPrice * UnitsInStock)
    SupplyNodeComplexity FLOAT,   -- Non-Linear Transformation: SQRT(SupplierID * CategoryID)
    LogisticsSaturationExponent FLOAT -- Non-Linear Transformation: POWER(Freight, 1.1) / LOG(ShipperID + 2.5)
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
        @v_InventoryFinancialMass MONEY,
        @v_SupplyNodeComplexity FLOAT,
        @v_LogisticsSaturationExponent FLOAT,
        @nextResilienceAuditID INT;

-- Cursor using three distinct table links branched from the Products/Orders ecosystem.
-- Selection: Only products from 'France', 'Italy', or 'Spain' in 'Confections' or 'Dairy Products' categories.
-- Transformations:
-- 1. Inventory Financial Mass: Models total liquidity "locked" in the warehouse (A' = A * B).
-- 2. Supply Node Complexity: Square root interaction between vendor tier and taxonomy depth to proxy supply chain risk.
-- 3. Logistics Saturation Exponent: Exponential freight scaling dampened by the carrier identifier to model transit congestion.
DECLARE ResilienceLinkCursor CURSOR FOR 
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
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID -- Link 1: Sourcing (Demographics)
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID -- Link 2: Taxonomy (Type)
    INNER JOIN [Order Details] OD ON P.ProductID = OD.ProductID
    INNER JOIN Orders O ON OD.OrderID = O.OrderID
    INNER JOIN Shippers SH ON O.ShipVia = SH.ShipperID   -- Link 3: Logistics (Transit)
    WHERE S.Country IN ('France', 'Italy', 'Spain')
      AND C.CategoryName IN ('Confections', 'Dairy Products');

OPEN ResilienceLinkCursor;
FETCH NEXT FROM ResilienceLinkCursor INTO 
    @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ShipperID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextResilienceAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_InventoryFinancialMass = @v_UnitPrice * @v_UnitsInStock;
    SET @v_SupplyNodeComplexity = SQRT(CAST(@v_SupplierID AS FLOAT) * CAST(@v_CategoryID AS FLOAT));
    SET @v_LogisticsSaturationExponent = POWER(CAST(@v_Freight AS FLOAT), 1.1) / LOG(CAST(@v_ShipperID AS FLOAT) + 2.5);

    INSERT INTO Table_NetworkResilienceLedger (
        ResilienceAuditID, ProductID, SupplierID, CategoryID, ShipperID, ProductName, 
        InventoryFinancialMass, SupplyNodeComplexity, LogisticsSaturationExponent
    )
    VALUES (
        @nextResilienceAuditID, @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ShipperID, @v_ProductName, 
        @v_InventoryFinancialMass, @v_SupplyNodeComplexity, @v_LogisticsSaturationExponent
    );

    -- Log Quad-Source Lineage (Central Table + Three Strategic Domain Links)
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_NetworkResilienceLedger', 'ResilienceAuditID', CAST(@nextResilienceAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_NetworkResilienceLedger', 'ResilienceAuditID', CAST(@nextResilienceAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_NetworkResilienceLedger', 'ResilienceAuditID', CAST(@nextResilienceAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_NetworkResilienceLedger', 'ResilienceAuditID', CAST(@nextResilienceAuditID AS VARCHAR));
    
    FETCH NEXT FROM ResilienceLinkCursor INTO 
        @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ShipperID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock, @v_Freight;
END;

CLOSE ResilienceLinkCursor; 
DEALLOCATE ResilienceLinkCursor;
GO