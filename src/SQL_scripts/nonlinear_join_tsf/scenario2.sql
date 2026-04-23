-- Section: Create a Physical Table using Selection, Projection, Joins, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Supplier-Inventory Capacity & Logistical Velocity Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B or exponential modifiers.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_InventoryVelocityLedger', 'U') IS NOT NULL DROP TABLE Table_InventoryVelocityLedger;
CREATE TABLE Table_InventoryVelocityLedger (
    VelocityAuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    CategoryID INT,          -- Native Projection from Categories
    ProductName NVARCHAR(40), -- Native Projection from Products
    InventoryPowerIndex MONEY, -- Non-Linear Transformation: (UnitPrice * UnitsInStock) 
    SupplyChainCurvature FLOAT, -- Non-Linear Transformation: (SupplierID * CategoryID) / SQRT(UnitsInStock)
    MarketDensityIndex FLOAT    -- Non-Linear Transformation: LOG(UnitPrice + 1.1) * ReorderLevel
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_CategoryID INT,
        @v_ProductName NVARCHAR(40),
        @v_UnitPrice MONEY,
        @v_UnitsInStock SMALLINT,
        @v_ReorderLevel SMALLINT,
        @v_InventoryPowerIndex MONEY,
        @v_SupplyChainCurvature FLOAT,
        @v_MarketDensityIndex FLOAT,
        @nextVelocityAuditID INT;

-- Cursor using JOIN to integrate Procurement logistics, Taxonomy, and Inventory thresholds.
-- Selection: Only products from 'Germany', 'Brazil', or 'Japan' where the ReorderLevel is > 0.
-- Transformations:
-- 1. Inventory Power: A non-linear valuation of on-hand capital (A' = A * B).
-- 2. Supply Chain Curvature: A complex ratio modeling the interaction between vendor tiers and stock levels.
-- 3. Market Density: Logarithmic scaling of price applied to replenishment frequency to dampen extreme outliers.
DECLARE VelocityCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        C.CategoryID,
        P.ProductName,
        P.UnitPrice,
        P.UnitsInStock,
        P.ReorderLevel
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    WHERE S.Country IN ('Germany', 'Brazil', 'Japan') AND P.ReorderLevel > 0;

OPEN VelocityCursor;
FETCH NEXT FROM VelocityCursor INTO 
    @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock, @v_ReorderLevel;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextVelocityAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    -- Note: Using CASE or NULLIF logic where necessary to prevent division by zero in SQRT
    SET @v_InventoryPowerIndex = @v_UnitPrice * @v_UnitsInStock;
    SET @v_SupplyChainCurvature = (CAST(@v_SupplierID AS FLOAT) * CAST(@v_CategoryID AS FLOAT)) / SQRT(NULLIF(@v_UnitsInStock, 0));
    SET @v_MarketDensityIndex = LOG(CAST(@v_UnitPrice AS FLOAT) + 1.1) * @v_ReorderLevel;

    INSERT INTO Table_InventoryVelocityLedger (
        VelocityAuditID, ProductID, SupplierID, CategoryID, ProductName, 
        InventoryPowerIndex, SupplyChainCurvature, MarketDensityIndex
    )
    VALUES (
        @nextVelocityAuditID, @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ProductName, 
        @v_InventoryPowerIndex, @v_SupplyChainCurvature, @v_MarketDensityIndex
    );

    -- Log Triple-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_InventoryVelocityLedger', 'VelocityAuditID', CAST(@nextVelocityAuditID AS VARCHAR));
    
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_InventoryVelocityLedger', 'VelocityAuditID', CAST(@nextVelocityAuditID AS VARCHAR));

    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_InventoryVelocityLedger', 'VelocityAuditID', CAST(@nextVelocityAuditID AS VARCHAR));
    
    FETCH NEXT FROM VelocityCursor INTO 
        @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock, @v_ReorderLevel;
END;

CLOSE VelocityCursor; 
DEALLOCATE VelocityCursor;
GO