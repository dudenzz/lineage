-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Category-Based Stock Value & Procurement Lead-Time Buffer Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_CategoryProcurementLedger', 'U') IS NOT NULL DROP TABLE Table_CategoryProcurementLedger;
CREATE TABLE Table_CategoryProcurementLedger (
    ProcurementAuditID INT,
    ProductID INT,           -- Native Projection from Products
    CategoryID INT,          -- Native Projection from Categories
    SupplierID INT,          -- Native Projection from Suppliers
    ProductName NVARCHAR(40), -- Native Projection from Products
    CategoryName NVARCHAR(15),-- Native Projection from Categories
    AdjustedInventoryValue MONEY, -- Linearly transformed column (Products.UnitPrice)
    SafetyLeadTimeIndex DECIMAL(18,2), -- Linearly transformed column (Products.ReorderLevel)
    VendorReliabilityWeight DECIMAL(18,2) -- Linearly transformed column (Suppliers.SupplierID)
);
GO

DECLARE @v_ProductID INT,
        @v_CategoryID INT,
        @v_SupplierID INT,
        @v_ProductName NVARCHAR(40),
        @v_CategoryName NVARCHAR(15),
        @v_AdjustedInventoryValue MONEY,
        @v_SafetyLeadTimeIndex DECIMAL(18,2),
        @v_VendorReliabilityWeight DECIMAL(18,2),
        @nextProcurementAuditID INT;

-- Linear Transformation Constants:
-- 1. Adjusted Inventory Value: Unit price plus a flat warehouse overhead and logistics fee (y = 1.30 * UnitPrice + 15.00)
-- 2. Safety Lead Time Index: Calculated buffer for replenishment cycles (y = 2.25 * ReorderLevel + 5.00)
-- 3. Vendor Reliability Weight: Proxy score for supplier consistency based on legacy ID tiering (y = 1.50 * SupplierID + 40.00)
DECLARE @ValueScalar DECIMAL(10,2) = 1.30;
DECLARE @ValueBase MONEY = 15.00;
DECLARE @LeadTimeScalar DECIMAL(10,2) = 2.25;
DECLARE @LeadTimeBase DECIMAL(10,2) = 5.00;
DECLARE @VendorScalar DECIMAL(10,2) = 1.50;
DECLARE @VendorBase DECIMAL(10,2) = 40.00;

-- Cursor using JOIN to integrate Product inventory thresholds, Category taxonomies, and Vendor demographics.
-- Selection: Only products in 'Seafood' or 'Produce' with a unit price between 10.00 and 50.00.
DECLARE ProcurementCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        C.CategoryID, 
        S.SupplierID,
        P.ProductName, 
        C.CategoryName, 
        (P.UnitPrice * @ValueScalar) + @ValueBase AS AdjustedInventoryValue,
        (CAST(P.ReorderLevel AS DECIMAL(18,2)) * @LeadTimeScalar) + @LeadTimeBase AS SafetyLeadTimeIndex,
        (CAST(S.SupplierID AS DECIMAL(18,2)) * @VendorScalar) + @VendorBase AS VendorReliabilityWeight
    FROM Products P
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    WHERE C.CategoryName IN ('Seafood', 'Produce') 
      AND P.UnitPrice BETWEEN 10.00 AND 50.00; -- Selection

OPEN ProcurementCursor;
FETCH NEXT FROM ProcurementCursor INTO 
    @v_ProductID, @v_CategoryID, @v_SupplierID, @v_ProductName, @v_CategoryName, @v_AdjustedInventoryValue, @v_SafetyLeadTimeIndex, @v_VendorReliabilityWeight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextProcurementAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_CategoryProcurementLedger (
        ProcurementAuditID, ProductID, CategoryID, SupplierID, ProductName, CategoryName, AdjustedInventoryValue, SafetyLeadTimeIndex, VendorReliabilityWeight
    )
    VALUES (
        @nextProcurementAuditID, @v_ProductID, @v_CategoryID, @v_SupplierID, @v_ProductName, @v_CategoryName, @v_AdjustedInventoryValue, @v_SafetyLeadTimeIndex, @v_VendorReliabilityWeight
    );

    -- Log Triple-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_CategoryProcurementLedger', 'ProcurementAuditID', CAST(@nextProcurementAuditID AS VARCHAR));
    
    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_CategoryProcurementLedger', 'ProcurementAuditID', CAST(@nextProcurementAuditID AS VARCHAR));

    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_CategoryProcurementLedger', 'ProcurementAuditID', CAST(@nextProcurementAuditID AS VARCHAR));
    
    FETCH NEXT FROM ProcurementCursor INTO 
        @v_ProductID, @v_CategoryID, @v_SupplierID, @v_ProductName, @v_CategoryName, @v_AdjustedInventoryValue, @v_SafetyLeadTimeIndex, @v_VendorReliabilityWeight;
END;

CLOSE ProcurementCursor; 
DEALLOCATE ProcurementCursor;
GO