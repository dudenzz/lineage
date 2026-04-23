-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Customer Lifetime Value (CLV) & Regional Loyalty Index.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_CustomerLoyaltyValuationLedger', 'U') IS NOT NULL DROP TABLE Table_CustomerLoyaltyValuationLedger;
CREATE TABLE Table_CustomerLoyaltyValuationLedger (
    LoyaltyAuditID INT,
    CustomerID NCHAR(5),      -- Native Projection from Customers
    OrderID INT,              -- Native Projection from Orders
    EmployeeID INT,           -- Native Projection from Employees
    CompanyName NVARCHAR(40), -- Native Projection from Customers
    AdjustedOrderFreight MONEY, -- Linearly transformed column (Orders.Freight)
    LoyaltyTierIndex DECIMAL(18,2), -- Linearly transformed column (Employees.EmployeeID)
    RegionalMarketWeight MONEY   -- Linearly transformed column (Orders.Freight)
);
GO

DECLARE @v_CustomerID NCHAR(5),
        @v_OrderID INT,
        @v_EmployeeID INT,
        @v_CompanyName NVARCHAR(40),
        @v_AdjustedOrderFreight MONEY,
        @v_LoyaltyTierIndex DECIMAL(18,2),
        @v_RegionalMarketWeight MONEY,
        @nextLoyaltyAuditID INT;

-- Linear Transformation Constants:
-- 1. Adjusted Order Freight: Freight costs adjusted for loyalty program shipping subsidies (y = 0.80 * Freight + 15.00)
-- 2. Loyalty Tier Index: Mapping account manager seniority to client service levels (y = 3.50 * EmployeeID + 100.00)
-- 3. Regional Market Weight: Local economic adjustment factor for market penetration (y = 1.10 * Freight + 50.00)
DECLARE @FreightScalar DECIMAL(10,2) = 0.80;
DECLARE @FreightBase MONEY = 15.00;
DECLARE @TierScalar DECIMAL(10,2) = 3.50;
DECLARE @TierBase DECIMAL(10,2) = 100.00;
DECLARE @MarketScalar DECIMAL(10,2) = 1.10;
DECLARE @MarketBase MONEY = 50.00;

-- Cursor using JOIN to integrate Customer account data, Sales history, and HR Representative assignments.
-- Selection: Only customers in 'France', 'Germany', or 'Switzerland' (Central Europe) with orders > 30.00 freight.
DECLARE LoyaltyCursor CURSOR FOR 
    SELECT 
        C.CustomerID, 
        O.OrderID, 
        E.EmployeeID, 
        C.CompanyName, 
        (O.Freight * @FreightScalar) + @FreightBase AS AdjustedOrderFreight,
        (CAST(E.EmployeeID AS DECIMAL(18,2)) * @TierScalar) + @TierBase AS LoyaltyTierIndex,
        (O.Freight * @MarketScalar) + @MarketBase AS RegionalMarketWeight
    FROM Customers C
    INNER JOIN Orders O ON C.CustomerID = O.CustomerID
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID
    WHERE C.Country IN ('France', 'Germany', 'Switzerland') AND O.Freight > 30.00;

OPEN LoyaltyCursor;
FETCH NEXT FROM LoyaltyCursor INTO 
    @v_CustomerID, @v_OrderID, @v_EmployeeID, @v_CompanyName, @v_AdjustedOrderFreight, @v_LoyaltyTierIndex, @v_RegionalMarketWeight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLoyaltyAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_CustomerLoyaltyValuationLedger (
        LoyaltyAuditID, CustomerID, OrderID, EmployeeID, CompanyName, AdjustedOrderFreight, LoyaltyTierIndex, RegionalMarketWeight
    )
    VALUES (
        @nextLoyaltyAuditID, @v_CustomerID, @v_OrderID, @v_EmployeeID, @v_CompanyName, @v_AdjustedOrderFreight, @v_LoyaltyTierIndex, @v_RegionalMarketWeight
    );

    -- Log Triple-Source Lineage
    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_CustomerLoyaltyValuationLedger', 'LoyaltyAuditID', CAST(@nextLoyaltyAuditID AS VARCHAR));
    
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_CustomerLoyaltyValuationLedger', 'LoyaltyAuditID', CAST(@nextLoyaltyAuditID AS VARCHAR));

    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_CustomerLoyaltyValuationLedger', 'LoyaltyAuditID', CAST(@nextLoyaltyAuditID AS VARCHAR));
    
    FETCH NEXT FROM LoyaltyCursor INTO 
        @v_CustomerID, @v_OrderID, @v_EmployeeID, @v_CompanyName, @v_AdjustedOrderFreight, @v_LoyaltyTierIndex, @v_RegionalMarketWeight;
END;

CLOSE LoyaltyCursor; 
DEALLOCATE LoyaltyCursor;
GO