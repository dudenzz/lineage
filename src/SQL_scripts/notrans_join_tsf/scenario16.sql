-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Supplier-Category Supply Chain Diversity Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_SupplierCategoryLedger', 'U') IS NOT NULL DROP TABLE Table_SupplierCategoryLedger;
CREATE TABLE Table_SupplierCategoryLedger (
    DiversityAuditID INT,
    SupplierID INT,          -- Native Projection from Suppliers
    CategoryID INT,         -- Native Projection from Categories
    ProductID INT,          -- Native Projection from Products
    CompanyName NVARCHAR(40), -- Native Projection from Suppliers
    CategoryName NVARCHAR(15),-- Native Projection from Categories
    ProductName NVARCHAR(40)  -- Native Projection from Products
);
GO

DECLARE @v_SupplierID INT,
        @v_CategoryID INT,
        @v_ProductID INT,
        @v_CompanyName NVARCHAR(40),
        @v_CategoryName NVARCHAR(15),
        @v_ProductName NVARCHAR(40),
        @nextDiversityAuditID INT;

-- Cursor using a complex JOIN for strict projection across Procurement and Classification domains.
-- Selection: Only products in the 'Dairy Products' or 'Grains/Cereals' categories provided by international suppliers (Non-USA/UK).
-- All attributes are native; no linear transformations or country-code mappings are applied.
DECLARE DiversityCursor CURSOR FOR 
    SELECT 
        S.SupplierID, 
        C.CategoryID, 
        P.ProductID,
        S.CompanyName, 
        C.CategoryName,
        P.ProductName
    FROM Suppliers S
    INNER JOIN Products P ON S.SupplierID = P.SupplierID
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    WHERE C.CategoryName IN ('Dairy Products', 'Grains/Cereals') 
      AND S.Country NOT IN ('USA', 'UK'); -- Selection

OPEN DiversityCursor;
FETCH NEXT FROM DiversityCursor INTO 
    @v_SupplierID, @v_CategoryID, @v_ProductID, @v_CompanyName, @v_CategoryName, @v_ProductName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextDiversityAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data (No Transformations)
    INSERT INTO Table_SupplierCategoryLedger (
        DiversityAuditID, SupplierID, CategoryID, ProductID, CompanyName, CategoryName, ProductName
    )
    VALUES (
        @nextDiversityAuditID, @v_SupplierID, @v_CategoryID, @v_ProductID, @v_CompanyName, @v_CategoryName, @v_ProductName
    );

    -- Log Triple-Source Lineage
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_SupplierCategoryLedger', 'DiversityAuditID', CAST(@nextDiversityAuditID AS VARCHAR));
    
    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_SupplierCategoryLedger', 'DiversityAuditID', CAST(@nextDiversityAuditID AS VARCHAR));

    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_SupplierCategoryLedger', 'DiversityAuditID', CAST(@nextDiversityAuditID AS VARCHAR));
    
    FETCH NEXT FROM DiversityCursor INTO 
        @v_SupplierID, @v_CategoryID, @v_ProductID, @v_CompanyName, @v_CategoryName, @v_ProductName;
END;

CLOSE DiversityCursor; 
DEALLOCATE DiversityCursor;
GO