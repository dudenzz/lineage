-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling a Targeted Procurement & Vendor Relationship Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_VendorProcurementLedger', 'U') IS NOT NULL DROP TABLE Table_VendorProcurementLedger;
CREATE TABLE Table_VendorProcurementLedger (
    ProcurementLineID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    ProductName NVARCHAR(40), -- Native Projection from Products
    CompanyName NVARCHAR(40), -- Native Projection from Suppliers
    ContactName NVARCHAR(30), -- Native Projection from Suppliers
    UnitsInStock SMALLINT    -- Native Projection from Products
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_ProductName NVARCHAR(40),
        @v_CompanyName NVARCHAR(40),
        @v_ContactName NVARCHAR(30),
        @v_UnitsInStock SMALLINT,
        @nextProcurementLineID INT;

-- Cursor using JOIN for strict projection across related domains.
-- Selection: Only currently active products (Discontinued = 0) with low stock (< 20).
-- All columns are extracted natively without any linear or non-linear transformation.
DECLARE ProcurementCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        P.ProductName, 
        S.CompanyName, 
        S.ContactName, 
        P.UnitsInStock
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    WHERE P.Discontinued = 0 AND P.UnitsInStock < 20; -- Selection

OPEN ProcurementCursor;
FETCH NEXT FROM ProcurementCursor INTO 
    @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_ContactName, @v_UnitsInStock;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextProcurementLineID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected records (No transformations)
    INSERT INTO Table_VendorProcurementLedger (
        ProcurementLineID, ProductID, SupplierID, ProductName, CompanyName, ContactName, UnitsInStock
    )
    VALUES (
        @nextProcurementLineID, @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_ContactName, @v_UnitsInStock
    );

    -- Log Dual-Source Lineage for the Join
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_VendorProcurementLedger', 'ProcurementLineID', CAST(@nextProcurementLineID AS VARCHAR));
    
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_VendorProcurementLedger', 'ProcurementLineID', CAST(@nextProcurementLineID AS VARCHAR));
    
    FETCH NEXT FROM ProcurementCursor INTO 
        @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_ContactName, @v_UnitsInStock;
END;

CLOSE ProcurementCursor; 
DEALLOCATE ProcurementCursor;
GO