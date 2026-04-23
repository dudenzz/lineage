-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Supplier Lead-Time & Procurement Risk Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_ProcurementRiskLedger', 'U') IS NOT NULL DROP TABLE Table_ProcurementRiskLedger;
CREATE TABLE Table_ProcurementRiskLedger (
    RiskAuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    ProductName NVARCHAR(40), -- Native Projection from Products
    CompanyName NVARCHAR(40), -- Native Projection from Suppliers
    RiskWeightedStock DECIMAL(18,2), -- Linearly transformed column (Products.UnitsInStock)
    InvoicedCostBasis MONEY,  -- Linearly transformed column (Products.UnitPrice)
    SupplyVolatilityIndex DECIMAL(18,2) -- Linearly transformed column (Suppliers.SupplierID)
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_ProductName NVARCHAR(40),
        @v_CompanyName NVARCHAR(40),
        @v_RiskWeightedStock DECIMAL(18,2),
        @v_InvoicedCostBasis MONEY,
        @v_SupplyVolatilityIndex DECIMAL(18,2),
        @nextRiskAuditID INT;

-- Linear Transformation Constants:
-- 1. Risk Weighted Stock: Inventory level adjusted for potential supply chain shrinkage (y = 0.88 * UnitsInStock + 5.00)
-- 2. Invoiced Cost Basis: Base unit price including a standard procurement handling fee (y = 1.05 * UnitPrice + 2.50)
-- 3. Supply Volatility Index: Risk score derived from supplier tiering (y = 2.15 * SupplierID + 15.00)
DECLARE @StockRiskScalar DECIMAL(10,2) = 0.88;
DECLARE @StockRiskBase DECIMAL(10,2) = 5.00;
DECLARE @CostScalar DECIMAL(10,2) = 1.05;
DECLARE @CostBase MONEY = 2.50;
DECLARE @VolatilityScalar DECIMAL(10,2) = 2.15;
DECLARE @VolatilityBase DECIMAL(10,2) = 15.00;

-- Cursor using JOIN to integrate Vendor logistics with Product inventory states.
-- Selection: Only products from suppliers in 'Australia' or 'Japan' with at least 10 units in stock.
DECLARE RiskCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        P.ProductName, 
        S.CompanyName, 
        (CAST(P.UnitsInStock AS DECIMAL(18,2)) * @StockRiskScalar) + @StockRiskBase AS RiskWeightedStock,
        (P.UnitPrice * @CostScalar) + @CostBase AS InvoicedCostBasis,
        (CAST(S.SupplierID AS DECIMAL(18,2)) * @VolatilityScalar) + @VolatilityBase AS SupplyVolatilityIndex
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    WHERE S.Country IN ('Australia', 'Japan') AND P.UnitsInStock >= 10;

OPEN RiskCursor;
FETCH NEXT FROM RiskCursor INTO 
    @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_RiskWeightedStock, @v_InvoicedCostBasis, @v_SupplyVolatilityIndex;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextRiskAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_ProcurementRiskLedger (
        RiskAuditID, ProductID, SupplierID, ProductName, CompanyName, RiskWeightedStock, InvoicedCostBasis, SupplyVolatilityIndex
    )
    VALUES (
        @nextRiskAuditID, @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_RiskWeightedStock, @v_InvoicedCostBasis, @v_SupplyVolatilityIndex
    );

    -- Log Dual-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_ProcurementRiskLedger', 'RiskAuditID', CAST(@nextRiskAuditID AS VARCHAR));
    
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_ProcurementRiskLedger', 'RiskAuditID', CAST(@nextRiskAuditID AS VARCHAR));
    
    FETCH NEXT FROM RiskCursor INTO 
        @v_ProductID, @v_SupplierID, @v_ProductName, @v_CompanyName, @v_RiskWeightedStock, @v_InvoicedCostBasis, @v_SupplyVolatilityIndex;
END;

CLOSE RiskCursor; 
DEALLOCATE RiskCursor;
GO