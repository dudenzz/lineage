-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Supply Chain Product-Supplier Mapping Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_ProductSupplierSourceLedger', 'U') IS NOT NULL DROP TABLE Table_ProductSupplierSourceLedger;
CREATE TABLE Table_ProductSupplierSourceLedger (
    SourceAuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    ProductName NVARCHAR(40), -- Native Projection from Products
    CompanyName NVARCHAR(40), -- Native Projection from Suppliers
    ContactTitle NVARCHAR(30),-- Native Projection from Suppliers
    UnitsInStock SMALLINT    -- Native Projection from Products
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_ProductName NVARCHAR(40),
        @v_CompanyName NVARCHAR(40),
        @v_ContactTitle NVARCHAR(30),
        @v_UnitsInStock SMALLINT,
        @nextSourceAuditID INT;

-- Cursor using JOIN for strict projection across the Product catalog and the providing Suppliers.
-- Selection: Only products from suppliers in 'France' that are currently in stock.
-- All columns are native; no linear transformations or data formatting applied.
DECLARE SourceCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        P.ProductName, 
        S.CompanyName, 
        S.ContactTitle,
        P.UnitsInStock
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    WHERE S.Country = 'France' AND P.UnitsInStock > 0; -- Selection

OPEN SourceCursor;
FETCH NEXT FROM SourceCursor INTO 
    @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_ContactTitle, @v_UnitsInStock;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextSourceAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_ProductSupplierSourceLedger (
        SourceAuditID, ProductID, SupplierID, ProductName, CompanyName, ContactTitle, UnitsInStock
    )
    VALUES (
        @nextSourceAuditID, @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_ContactTitle, @v_UnitsInStock
    );

    -- Log Dual-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_ProductSupplierSourceLedger', 'SourceAuditID', CAST(@nextSourceAuditID AS VARCHAR));
    
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_ProductSupplierSourceLedger', 'SourceAuditID', CAST(@nextSourceAuditID AS VARCHAR));
    
    FETCH NEXT FROM SourceCursor INTO 
        @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_ContactTitle, @v_UnitsInStock;
END;

CLOSE SourceCursor; 
DEALLOCATE SourceCursor;
GO