-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Product-Category Taxonomy & Inventory Status Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_TaxonomyInventoryLedger', 'U') IS NOT NULL DROP TABLE Table_TaxonomyInventoryLedger;
CREATE TABLE Table_TaxonomyInventoryLedger (
    AuditID INT,
    ProductID INT,           -- Native Projection from Products
    CategoryID INT,          -- Native Projection from Categories
    ProductName NVARCHAR(40), -- Native Projection from Products
    CategoryName NVARCHAR(15),-- Native Projection from Categories
    UnitsInStock SMALLINT,   -- Native Projection from Products
    ReorderLevel SMALLINT    -- Native Projection from Products
);
GO

DECLARE @v_ProductID INT,
        @v_CategoryID INT,
        @v_ProductName NVARCHAR(40),
        @v_CategoryName NVARCHAR(15),
        @v_UnitsInStock SMALLINT,
        @v_ReorderLevel SMALLINT,
        @nextAuditID INT;

-- Cursor using JOIN for strict projection across Catalog Taxonomy and Inventory thresholds.
-- Selection: Only products in the 'Seafood' category that have reached their ReorderLevel.
-- All columns are native; no linear transformations or calculations (e.g., diff) are used.
DECLARE TaxonomyCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        C.CategoryID, 
        P.ProductName, 
        C.CategoryName, 
        P.UnitsInStock,
        P.ReorderLevel
    FROM Products P
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    WHERE C.CategoryName = 'Seafood' AND P.UnitsInStock <= P.ReorderLevel; -- Selection

OPEN TaxonomyCursor;
FETCH NEXT FROM TaxonomyCursor INTO 
    @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_UnitsInStock, @v_ReorderLevel;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_TaxonomyInventoryLedger (
        AuditID, ProductID, CategoryID, ProductName, CategoryName, UnitsInStock, ReorderLevel
    )
    VALUES (
        @nextAuditID, @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_UnitsInStock, @v_ReorderLevel
    );

    -- Log Dual-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_TaxonomyInventoryLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_TaxonomyInventoryLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TaxonomyCursor INTO 
        @v_ProductID, @v_CategoryID, @v_ProductName, @v_CategoryName, @v_UnitsInStock, @v_ReorderLevel;
END;

CLOSE TaxonomyCursor; 
DEALLOCATE TaxonomyCursor;
GO