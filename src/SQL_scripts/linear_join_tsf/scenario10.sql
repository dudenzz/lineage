-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Supplier-Product Inventory Inflation & Carrying Cost Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_InventoryFinancialsLedger', 'U') IS NOT NULL DROP TABLE Table_InventoryFinancialsLedger;
CREATE TABLE Table_InventoryFinancialsLedger (
    FinancialAuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    ProductName NVARCHAR(40), -- Native Projection from Products
    CompanyName NVARCHAR(40), -- Native Projection from Suppliers
    InflatedUnitCost MONEY,   -- Linearly transformed column (Products.UnitPrice)
    StorageRiskIndex DECIMAL(18,2), -- Linearly transformed column (Products.UnitsInStock)
    VendorTierScore DECIMAL(18,2)   -- Linearly transformed column (Suppliers.SupplierID)
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_ProductName NVARCHAR(40),
        @v_CompanyName NVARCHAR(40),
        @v_InflatedUnitCost MONEY,
        @v_StorageRiskIndex DECIMAL(18,2),
        @v_VendorTierScore DECIMAL(18,2),
        @nextFinancialAuditID INT;

-- Linear Transformation Constants:
-- 1. Inflated Unit Cost: Adjusts for market inflation and a base import duty (y = 1.18 * UnitPrice + 12.00)
-- 2. Storage Risk Index: Calculates a risk score based on stock volume (y = 0.40 * UnitsInStock + 25.00)
-- 3. Vendor Tier Score: Normalizes SupplierID into a legacy ranking system (y = 1.05 * SupplierID + 50.00)
DECLARE @PriceScalar DECIMAL(10,2) = 1.18;
DECLARE @PriceBase MONEY = 12.00;
DECLARE @StockRiskScalar DECIMAL(10,2) = 0.40;
DECLARE @StockRiskBase DECIMAL(10,2) = 25.00;
DECLARE @VendorScalar DECIMAL(10,2) = 1.05;
DECLARE @VendorBase DECIMAL(10,2) = 50.00;

-- Cursor using JOIN to integrate Supplier geographic data with Product stock levels.
-- Selection: Only products from suppliers in 'UK', 'USA', or 'Canada' that have stock levels > 20.
DECLARE FinancialsCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        P.ProductName, 
        S.CompanyName, 
        (P.UnitPrice * @PriceScalar) + @PriceBase AS InflatedUnitCost,
        (CAST(P.UnitsInStock AS DECIMAL(18,2)) * @StockRiskScalar) + @StockRiskBase AS StorageRiskIndex,
        (CAST(S.SupplierID AS DECIMAL(18,2)) * @VendorScalar) + @VendorBase AS VendorTierScore
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    WHERE S.Country IN ('UK', 'USA', 'Canada') AND P.UnitsInStock > 20;

OPEN FinancialsCursor;
FETCH NEXT FROM FinancialsCursor INTO 
    @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_InflatedUnitCost, @v_StorageRiskIndex, @v_VendorTierScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextFinancialAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_InventoryFinancialsLedger (
        FinancialAuditID, ProductID, SupplierID, ProductName, CompanyName, InflatedUnitCost, StorageRiskIndex, VendorTierScore
    )
    VALUES (
        @nextFinancialAuditID, @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_InflatedUnitCost, @v_StorageRiskIndex, @v_VendorTierScore
    );

    -- Log Dual-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_InventoryFinancialsLedger', 'FinancialAuditID', CAST(@nextFinancialAuditID AS VARCHAR));
    
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_InventoryFinancialsLedger', 'FinancialAuditID', CAST(@nextFinancialAuditID AS VARCHAR));
    
    FETCH NEXT FROM FinancialsCursor INTO 
        @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_InflatedUnitCost, @v_StorageRiskIndex, @v_VendorTierScore;
END;

CLOSE FinancialsCursor; 
DEALLOCATE FinancialsCursor;
GO