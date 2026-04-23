-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Supplier-Product Quality & Procurement Audit Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_SupplierQualityAuditLedger', 'U') IS NOT NULL DROP TABLE Table_SupplierQualityAuditLedger;
CREATE TABLE Table_SupplierQualityAuditLedger (
    AuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    ProductName NVARCHAR(40), -- Native Projection from Products
    CompanyName NVARCHAR(40), -- Native Projection from Suppliers
    QuantityPerUnit NVARCHAR(20), -- Native Projection from Products
    Phone NVARCHAR(24)       -- Native Projection from Suppliers
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_ProductName NVARCHAR(40),
        @v_CompanyName NVARCHAR(40),
        @v_QuantityPerUnit NVARCHAR(20),
        @v_Phone NVARCHAR(24),
        @nextAuditID INT;

-- Cursor using JOIN for strict projection across the Inventory and Vendor domains.
-- Selection: Only products that are measured in 'boxes' and provided by suppliers in 'Germany'.
-- All attributes are native; no unit conversions or phone number formatting applied.
DECLARE QualityAuditCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        P.ProductName, 
        S.CompanyName, 
        P.QuantityPerUnit,
        S.Phone
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    WHERE S.Country = 'Germany' AND P.QuantityPerUnit LIKE '%boxes%'; -- Selection

OPEN QualityAuditCursor;
FETCH NEXT FROM QualityAuditCursor INTO 
    @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_QuantityPerUnit, @v_Phone;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data (No Transformations)
    INSERT INTO Table_SupplierQualityAuditLedger (
        AuditID, ProductID, SupplierID, ProductName, CompanyName, QuantityPerUnit, Phone
    )
    VALUES (
        @nextAuditID, @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_QuantityPerUnit, @v_Phone
    );

    -- Log Dual-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_SupplierQualityAuditLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_SupplierQualityAuditLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM QualityAuditCursor INTO 
        @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_QuantityPerUnit, @v_Phone;
END;

CLOSE QualityAuditCursor; 
DEALLOCATE QualityAuditCursor;
GO