-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Product Catalog & Category Mapping Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_CategoryProductLedger', 'U') IS NOT NULL DROP TABLE Table_CategoryProductLedger;
CREATE TABLE Table_CategoryProductLedger (
    CatalogLineID INT,
    ProductID INT,           -- Native Projection from Products
    CategoryID INT,          -- Native Projection from Categories
    ProductName NVARCHAR(40), -- Native Projection from Products
    CategoryName NVARCHAR(15),-- Native Projection from Categories
    QuantityPerUnit NVARCHAR(20), -- Native Projection from Products
    UnitPrice MONEY          -- Native Projection from Products
);
GO

DECLARE @v_ProductID INT,
        @v_CategoryID INT,
        @v_ProductName NVARCHAR(40),
        @v_CategoryName NVARCHAR(15),
        @v_QuantityPerUnit NVARCHAR(20),
        @v_UnitPrice MONEY,
        @nextCatalogLineID INT;

-- Cursor using JOIN for strict projection across Inventory and Taxonomy domains.
-- Selection: Only products belonging to the 'Beverages' or 'Condiments' categories.
-- All columns are native; no price adjustments or string modifications are performed.
DECLARE CatalogCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        C.CategoryID, 
        P.ProductName, 
        C.CategoryName, 
        P.QuantityPerUnit,
        P.UnitPrice
    FROM Products P
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    WHERE C.CategoryName IN ('Beverages', 'Condiments'); -- Selection

OPEN CatalogCursor;
FETCH NEXT FROM CatalogCursor INTO 
    @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_QuantityPerUnit, @v_UnitPrice;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextCatalogLineID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_CategoryProductLedger (
        CatalogLineID, ProductID, CategoryID, ProductName, CategoryName, QuantityPerUnit, UnitPrice
    )
    VALUES (
        @nextCatalogLineID, @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_QuantityPerUnit, @v_UnitPrice
    );

    -- Log Dual-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_CategoryProductLedger', 'CatalogLineID', CAST(@nextCatalogLineID AS VARCHAR));
    
    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_CategoryProductLedger', 'CatalogLineID', CAST(@nextCatalogLineID AS VARCHAR));
    
    FETCH NEXT FROM CatalogCursor INTO 
        @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_QuantityPerUnit, @v_UnitPrice;
END;

CLOSE CatalogCursor; 
DEALLOCATE CatalogCursor;
GO