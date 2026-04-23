-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling a Supplier-Product Inventory & Logistics Origin Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_SupplierInventoryLedger', 'U') IS NOT NULL DROP TABLE Table_SupplierInventoryLedger;
CREATE TABLE Table_SupplierInventoryLedger (
    InventoryAuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    ProductName NVARCHAR(40), -- Native Projection from Products
    CompanyName NVARCHAR(40), -- Native Projection from Suppliers
    Country NVARCHAR(15),     -- Native Projection from Suppliers
    UnitsOnOrder SMALLINT    -- Native Projection from Products
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_ProductName NVARCHAR(40),
        @v_CompanyName NVARCHAR(40),
        @v_Country NVARCHAR(15),
        @v_UnitsOnOrder SMALLINT,
        @nextInventoryAuditID INT;

-- Cursor using JOIN for strict projection across Supplier origins and Product replenishment status.
-- Selection: Only products currently on order (UnitsOnOrder > 0) from suppliers located in the USA.
-- All columns are native; no linear transformations or regional mapping functions are used.
DECLARE InventoryCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        P.ProductName, 
        S.CompanyName, 
        S.Country,
        P.UnitsOnOrder
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    WHERE S.Country = 'USA' AND P.UnitsOnOrder > 0; -- Selection

OPEN InventoryCursor;
FETCH NEXT FROM InventoryCursor INTO 
    @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_Country, @v_UnitsOnOrder;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextInventoryAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_SupplierInventoryLedger (
        InventoryAuditID, ProductID, SupplierID, ProductName, CompanyName, Country, UnitsOnOrder
    )
    VALUES (
        @nextInventoryAuditID, @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_Country, @v_UnitsOnOrder
    );

    -- Log Dual-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_SupplierInventoryLedger', 'InventoryAuditID', CAST(@nextInventoryAuditID AS VARCHAR));
    
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_SupplierInventoryLedger', 'InventoryAuditID', CAST(@nextInventoryAuditID AS VARCHAR));
    
    FETCH NEXT FROM InventoryCursor INTO 
        @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_Country, @v_UnitsOnOrder;
END;

CLOSE InventoryCursor; 
DEALLOCATE InventoryCursor;
GO