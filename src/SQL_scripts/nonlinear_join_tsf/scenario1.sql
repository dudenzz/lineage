-- Section: Create a Physical Table using Selection, Projection, Joins, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Product-Market Saturation & Exponential Risk Valuation Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, powers, or square roots.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_NonLinearRiskLedger', 'U') IS NOT NULL DROP TABLE Table_NonLinearRiskLedger;
CREATE TABLE Table_NonLinearRiskLedger (
    RiskAuditID INT,
    ProductID INT,           -- Native Projection from Products
    CategoryID INT,          -- Native Projection from Categories
    OrderID INT,             -- Native Projection from Orders
    ProductName NVARCHAR(40), -- Native Projection from Products
    MarketSaturationScore MONEY, -- Non-Linear Transformation: (UnitPrice * UnitsInStock)
    LogisticsEntropyIndex DECIMAL(18,4), -- Non-Linear Transformation: SQRT(Freight * UnitsInStock)
    FinancialVolatility DECIMAL(18,4)    -- Non-Linear Transformation: POWER(UnitPrice, 1.2)
);
GO

DECLARE @v_ProductID INT,
        @v_CategoryID INT,
        @v_OrderID INT,
        @v_ProductName NVARCHAR(40),
        @v_UnitPrice MONEY,
        @v_UnitsInStock SMALLINT,
        @v_Freight MONEY,
        @v_MarketSaturationScore MONEY,
        @v_LogisticsEntropyIndex DECIMAL(18,4),
        @v_FinancialVolatility DECIMAL(18,4),
        @nextRiskAuditID INT;

-- Cursor using JOIN to integrate Transactional Logistics, Category context, and Inventory state.
-- Selection: Only active products (Discontinued = 0) in 'Beverages' or 'Confections' where freight > 10.00.
-- Transformations:
-- 1. Market Saturation: A non-linear product of price and quantity (A' = A * B).
-- 2. Logistics Entropy: Square root of the interaction between transport cost and stock availability.
-- 3. Financial Volatility: Exponential scaling of the unit price to model high-value risk.
DECLARE NonLinearCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        C.CategoryID, 
        O.OrderID,
        P.ProductName,
        P.UnitPrice,
        P.UnitsInStock,
        O.Freight
    FROM Products P
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    INNER JOIN [Order Details] OD ON P.ProductID = OD.ProductID
    INNER JOIN Orders O ON OD.OrderID = O.OrderID
    WHERE P.Discontinued = 0 
      AND C.CategoryName IN ('Beverages', 'Confections')
      AND O.Freight > 10.00;

OPEN NonLinearCursor;
FETCH NEXT FROM NonLinearCursor INTO 
    @v_ProductID, @v_CategoryID, @v_OrderID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextRiskAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_MarketSaturationScore = @v_UnitPrice * @v_UnitsInStock;
    SET @v_LogisticsEntropyIndex = SQRT(@v_Freight * CAST(@v_UnitsInStock AS DECIMAL(18,4)));
    SET @v_FinancialVolatility = POWER(CAST(@v_UnitPrice AS FLOAT), 1.2);

    INSERT INTO Table_NonLinearRiskLedger (
        RiskAuditID, ProductID, CategoryID, OrderID, ProductName, 
        MarketSaturationScore, LogisticsEntropyIndex, FinancialVolatility
    )
    VALUES (
        @nextRiskAuditID, @v_ProductID, @v_CategoryID, @v_OrderID, @v_ProductName, 
        @v_MarketSaturationScore, @v_LogisticsEntropyIndex, @v_FinancialVolatility
    );

    -- Log Triple-Source Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_NonLinearRiskLedger', 'RiskAuditID', CAST(@nextRiskAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_NonLinearRiskLedger', 'RiskAuditID', CAST(@nextRiskAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_NonLinearRiskLedger', 'RiskAuditID', CAST(@nextRiskAuditID AS VARCHAR));
    
    FETCH NEXT FROM NonLinearCursor INTO 
        @v_ProductID, @v_CategoryID, @v_OrderID, @v_ProductName, @v_UnitPrice, @v_UnitsInStock, @v_Freight;
END;

CLOSE NonLinearCursor; 
DEALLOCATE NonLinearCursor;
GO