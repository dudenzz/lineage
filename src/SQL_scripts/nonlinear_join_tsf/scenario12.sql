-- Section: Create a Physical Table using Selection, Projection, Joins (2 links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Category-Supplier Strategic Synergy & Financial Volatility Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Products -> Categories, Products -> Suppliers).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Powers, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the relationship.

IF OBJECT_ID('Table_SupplierCategorySynergyLedger', 'U') IS NOT NULL DROP TABLE Table_SupplierCategorySynergyLedger;
CREATE TABLE Table_SupplierCategorySynergyLedger (
    SynergyAuditID INT,
    ProductID INT,           -- Native Projection from Products
    CategoryID INT,          -- Native Projection from Categories
    SupplierID INT,          -- Native Projection from Suppliers
    ProductName NVARCHAR(40), -- Native Projection from Products
    InventoryCapitalMass MONEY,  -- Non-Linear Transformation: (UnitPrice * UnitsInStock)
    SynergyCurvatureScore FLOAT, -- Non-Linear Transformation: SQRT(CategoryID * SupplierID)
    EconomicRiskExponent FLOAT   -- Non-Linear Transformation: POWER(UnitPrice, 1.1) / LOG(UnitsInStock + 2.0)
);
GO

DECLARE @v_ProductID INT,
        @v_CategoryID INT,
        @v_SupplierID INT,
        @v_ProductName NVARCHAR(40),
        @v_UnitPrice MONEY,
        @v_UnitsInStock SMALLINT,
        @v_InventoryCapitalMass MONEY,
        @v_SynergyCurvatureScore FLOAT,
        @v_EconomicRiskExponent FLOAT,
        @nextSynergyAuditID INT;

-- Cursor using two table links branched from the central Products entity to taxonomy and sourcing.
-- Selection: Only active products from 'Australia', 'Japan', or 'Singapore' in 'Seafood' or 'Produce'.
-- Transformations:
-- 1. Inventory Capital Mass: Models total liquidity tied up in physical inventory (A' = A * B).
-- 2. Synergy Curvature: Square root interaction between category classification and vendor tier.
-- 3. Economic Risk Exponent: Exponential value growth dampened by stock depth to model financial exposure.
DECLARE SynergyLinkCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        C.CategoryID, 
        S.SupplierID,
        P.ProductName,
        P.UnitPrice,
        P.UnitsInStock
    FROM Products P
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID -- Link 1: Taxonomy
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID -- Link 2: Sourcing
    WHERE S.Country IN ('Australia', 'Japan', 'Singapore') 
      AND C.CategoryName IN ('Seafood', 'Produce')
      AND P.Discontinued = 0;

OPEN SynergyLinkCursor;
FETCH NEXT FROM SynergyLinkCursor INTO 
    @v_ProductID, @v_CategoryID, @v_SupplierID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextSynergyAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_InventoryCapitalMass = @v_UnitPrice * @v_UnitsInStock;
    SET @v_SynergyCurvatureScore = SQRT(CAST(@v_CategoryID AS FLOAT) * CAST(@v_SupplierID AS FLOAT));
    SET @v_EconomicRiskExponent = POWER(CAST(@v_UnitPrice AS FLOAT), 1.1) / LOG(CAST(@v_UnitsInStock AS FLOAT) + 2.0);

    INSERT INTO Table_SupplierCategorySynergyLedger (
        SynergyAuditID, ProductID, CategoryID, SupplierID, ProductName, 
        InventoryCapitalMass, SynergyCurvatureScore, EconomicRiskExponent
    )
    VALUES (
        @nextSynergyAuditID, @v_ProductID, @v_CategoryID, @v_SupplierID, @v_ProductName, 
        @v_InventoryCapitalMass, @v_SynergyCurvatureScore, @v_EconomicRiskExponent
    );

    -- Log Triple-Source Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_SupplierCategorySynergyLedger', 'SynergyAuditID', CAST(@nextSynergyAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_SupplierCategorySynergyLedger', 'SynergyAuditID', CAST(@nextSynergyAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_SupplierCategorySynergyLedger', 'SynergyAuditID', CAST(@nextSynergyAuditID AS VARCHAR));
    
    FETCH NEXT FROM SynergyLinkCursor INTO 
        @v_ProductID, @v_CategoryID, @v_SupplierID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock;
END;

CLOSE SynergyLinkCursor; 
DEALLOCATE SynergyLinkCursor;
GO