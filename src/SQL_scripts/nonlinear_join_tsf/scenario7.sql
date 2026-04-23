-- Section: Create a Physical Table using Selection, Projection, Joins (2 links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Supplier-Category Financial Risk & Inventory Mass Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Products -> Suppliers, Products -> Categories).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B.
-- Lineage: Tracks specific source table and primary key for every entity in the relationship.

IF OBJECT_ID('Table_SupplierCategoryRiskLedger', 'U') IS NOT NULL DROP TABLE Table_SupplierCategoryRiskLedger;
CREATE TABLE Table_SupplierCategoryRiskLedger (
    AuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    CategoryID INT,          -- Native Projection from Categories
    ProductName NVARCHAR(40), -- Native Projection from Products
    InventoryCapitalMass MONEY, -- Non-Linear Transformation: (UnitPrice * UnitsInStock)
    RiskVolatilityIndex FLOAT,  -- Non-Linear Transformation: SQRT(UnitPrice * SupplierID)
    CategorySaturationIndex FLOAT -- Non-Linear Transformation: LOG(UnitPrice + 2) * CategoryID
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_CategoryID INT,
        @v_ProductName NVARCHAR(40),
        @v_UnitPrice MONEY,
        @v_UnitsInStock SMALLINT,
        @v_InventoryCapitalMass MONEY,
        @v_RiskVolatilityIndex FLOAT,
        @v_CategorySaturationIndex FLOAT,
        @nextAuditID INT;

-- Cursor using two table links (Products as the center-piece).
-- Selection: Only active products from 'USA' or 'UK' within 'Beverages' or 'Condiments'.
DECLARE RiskLinkCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        C.CategoryID,
        P.ProductName,
        P.UnitPrice,
        P.UnitsInStock
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID -- Link 1
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID -- Link 2
    WHERE S.Country IN ('USA', 'UK') 
      AND C.CategoryName IN ('Beverages', 'Condiments')
      AND P.Discontinued = 0;

OPEN RiskLinkCursor;
FETCH NEXT FROM RiskLinkCursor INTO 
    @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Non-Linear Transformations
    SET @v_InventoryCapitalMass = @v_UnitPrice * @v_UnitsInStock; -- A' = A * B
    SET @v_RiskVolatilityIndex = SQRT(CAST(@v_UnitPrice AS FLOAT) * @v_SupplierID);
    SET @v_CategorySaturationIndex = LOG(CAST(@v_UnitPrice AS FLOAT) + 2.0) * @v_CategoryID;

    INSERT INTO Table_SupplierCategoryRiskLedger (
        AuditID, ProductID, SupplierID, CategoryID, ProductName, 
        InventoryCapitalMass, RiskVolatilityIndex, CategorySaturationIndex
    )
    VALUES (
        @nextAuditID, @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ProductName, 
        @v_InventoryCapitalMass, @v_RiskVolatilityIndex, @v_CategorySaturationIndex
    );

    -- Log Dual-Link Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_SupplierCategoryRiskLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_SupplierCategoryRiskLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_SupplierCategoryRiskLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM RiskLinkCursor INTO 
        @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock;
END;

CLOSE RiskLinkCursor; 
DEALLOCATE RiskLinkCursor;
GO