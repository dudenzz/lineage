-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Supplier-Logistics Strategic Value & Import Duty Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_SupplierStrategicValueLedger', 'U') IS NOT NULL DROP TABLE Table_SupplierStrategicValueLedger;
CREATE TABLE Table_SupplierStrategicValueLedger (
    StrategyAuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    OrderID INT,             -- Native Projection from Orders
    CompanyName NVARCHAR(40), -- Native Projection from Suppliers
    ImportDutyProjected MONEY, -- Linearly transformed column (Products.UnitPrice)
    SupplyChainRiskScore DECIMAL(18,2), -- Linearly transformed column (Suppliers.SupplierID)
    LogisticsBufferCost MONEY  -- Linearly transformed column (Orders.Freight)
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_OrderID INT,
        @v_CompanyName NVARCHAR(40),
        @v_ImportDutyProjected MONEY,
        @v_SupplyChainRiskScore DECIMAL(18,2),
        @v_LogisticsBufferCost MONEY,
        @nextStrategyAuditID INT;

-- Linear Transformation Constants:
-- 1. Import Duty Projected: Estimates customs fees based on product value (y = 0.22 * UnitPrice + 55.00)
-- 2. Supply Chain Risk Score: Weighted score based on vendor ID and geographic distance proxy (y = 1.85 * SupplierID + 30.00)
-- 3. Logistics Buffer Cost: Contingency fund for freight variability (y = 0.40 * Freight + 15.00)
DECLARE @DutyScalar DECIMAL(10,2) = 0.22;
DECLARE @DutyBase MONEY = 55.00;
DECLARE @RiskScalar DECIMAL(10,2) = 1.85;
DECLARE @RiskBase DECIMAL(10,2) = 30.00;
DECLARE @BufferScalar DECIMAL(10,2) = 0.40;
DECLARE @BufferBase MONEY = 15.00;

-- Cursor using JOIN to link Vendor demographics, Product financials, and Order transit data.
-- Selection: Only products from suppliers in 'Japan', 'Singapore', or 'Australia' where freight exceeds 50.00.
DECLARE StrategyCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        O.OrderID,
        S.CompanyName, 
        (P.UnitPrice * @DutyScalar) + @DutyBase AS ImportDutyProjected,
        (CAST(S.SupplierID AS DECIMAL(18,2)) * @RiskScalar) + @RiskBase AS SupplyChainRiskScore,
        (O.Freight * @BufferScalar) + @BufferBase AS LogisticsBufferCost
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    INNER JOIN [Order Details] OD ON P.ProductID = OD.ProductID
    INNER JOIN Orders O ON OD.OrderID = O.OrderID
    WHERE S.Country IN ('Japan', 'Singapore', 'Australia') AND O.Freight > 50.00; -- Selection

OPEN StrategyCursor;
FETCH NEXT FROM StrategyCursor INTO 
    @v_ProductID, @v_SupplierID, @v_OrderID, @v_CompanyName, @v_ImportDutyProjected, @v_SupplyChainRiskScore, @v_LogisticsBufferCost;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextStrategyAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_SupplierStrategicValueLedger (
        StrategyAuditID, ProductID, SupplierID, OrderID, CompanyName, ImportDutyProjected, SupplyChainRiskScore, LogisticsBufferCost
    )
    VALUES (
        @nextStrategyAuditID, @v_ProductID, @v_SupplierID, @v_OrderID, @v_CompanyName, @v_ImportDutyProjected, @v_SupplyChainRiskScore, @v_LogisticsBufferCost
    );

    -- Log Triple-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_SupplierStrategicValueLedger', 'StrategyAuditID', CAST(@nextStrategyAuditID AS VARCHAR));
    
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_SupplierStrategicValueLedger', 'StrategyAuditID', CAST(@nextStrategyAuditID AS VARCHAR));

    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_SupplierStrategicValueLedger', 'StrategyAuditID', CAST(@nextStrategyAuditID AS VARCHAR));
    
    FETCH NEXT FROM StrategyCursor INTO 
        @v_ProductID, @v_SupplierID, @v_OrderID, @v_CompanyName, @v_ImportDutyProjected, @v_SupplyChainRiskScore, @v_LogisticsBufferCost;
END;

CLOSE StrategyCursor; 
DEALLOCATE StrategyCursor;
GO