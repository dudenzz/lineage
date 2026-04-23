-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Personnel Optimization & Regional Revenue Impact Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_PersonnelOptimizationLedger', 'U') IS NOT NULL DROP TABLE Table_PersonnelOptimizationLedger;
CREATE TABLE Table_PersonnelOptimizationLedger (
    OptimizationAuditID INT,
    EmployeeID INT,           -- Native Projection from Employees
    OrderID INT,              -- Native Projection from Orders
    TerritoryID NVARCHAR(20), -- Native Projection from Territories
    LastName NVARCHAR(20),    -- Native Projection from Employees
    AdjustedPerformanceBonus MONEY, -- Linearly transformed column (Orders.Freight)
    CoverageSeniorityScore DECIMAL(18,2), -- Linearly transformed column (Employees.EmployeeID)
    RegionalEconomicImpact MONEY   -- Linearly transformed column (Orders.Freight)
);
GO

DECLARE @v_EmployeeID INT,
        @v_OrderID INT,
        @v_TerritoryID NVARCHAR(20),
        @v_LastName NVARCHAR(20),
        @v_AdjustedPerformanceBonus MONEY,
        @v_CoverageSeniorityScore DECIMAL(18,2),
        @v_RegionalEconomicImpact MONEY,
        @nextOptimizationAuditID INT;

-- Linear Transformation Constants:
-- 1. Adjusted Performance Bonus: Scaled incentive based on shipping volume handled (y = 0.15 * Freight + 50.00)
-- 2. Coverage Seniority Score: Mapping ID longevity into a service tier (y = 2.10 * EmployeeID + 120.00)
-- 3. Regional Economic Impact: Localized value contribution factor (y = 1.35 * Freight + 75.00)
DECLARE @BonusScalar DECIMAL(10,2) = 0.15;
DECLARE @BonusBase MONEY = 50.00;
DECLARE @SeniorityScalar DECIMAL(10,2) = 2.10;
DECLARE @SeniorityBase DECIMAL(10,2) = 120.00;
DECLARE @ImpactScalar DECIMAL(10,2) = 1.35;
DECLARE @ImpactBase MONEY = 75.00;

-- Cursor using JOIN to link HR records, Territory assignments, and active Sales transactions.
-- Selection: Only employees in 'Sales Representative' roles assigned to 'London' or 'Seattle' territories.
DECLARE OptimizationCursor CURSOR FOR 
    SELECT 
        E.EmployeeID, 
        O.OrderID, 
        T.TerritoryID,
        E.LastName, 
        (O.Freight * @BonusScalar) + @BonusBase AS AdjustedPerformanceBonus,
        (CAST(E.EmployeeID AS DECIMAL(18,2)) * @SeniorityScalar) + @SeniorityBase AS CoverageSeniorityScore,
        (O.Freight * @ImpactScalar) + @ImpactBase AS RegionalEconomicImpact
    FROM Employees E
    INNER JOIN EmployeeTerritories ET ON E.EmployeeID = ET.EmployeeID
    INNER JOIN Territories T ON ET.TerritoryID = T.TerritoryID
    INNER JOIN Orders O ON E.EmployeeID = O.EmployeeID
    WHERE E.Title = 'Sales Representative' 
      AND T.TerritoryDescription IN ('London', 'Seattle'); -- Selection

OPEN OptimizationCursor;
FETCH NEXT FROM OptimizationCursor INTO 
    @v_EmployeeID, @v_OrderID, @v_TerritoryID, @v_LastName, @v_AdjustedPerformanceBonus, @v_CoverageSeniorityScore, @v_RegionalEconomicImpact;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextOptimizationAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_PersonnelOptimizationLedger (
        OptimizationAuditID, EmployeeID, OrderID, TerritoryID, LastName, AdjustedPerformanceBonus, CoverageSeniorityScore, RegionalEconomicImpact
    )
    VALUES (
        @nextOptimizationAuditID, @v_EmployeeID, @v_OrderID, @v_TerritoryID, @v_LastName, @v_AdjustedPerformanceBonus, @v_CoverageSeniorityScore, @v_RegionalEconomicImpact
    );

    -- Log Triple-Source Lineage
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_PersonnelOptimizationLedger', 'OptimizationAuditID', CAST(@nextOptimizationAuditID AS VARCHAR));
    
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_PersonnelOptimizationLedger', 'OptimizationAuditID', CAST(@nextOptimizationAuditID AS VARCHAR));

    -- Record source for Territories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', CAST(@v_TerritoryID AS VARCHAR), 'Table_PersonnelOptimizationLedger', 'OptimizationAuditID', CAST(@nextOptimizationAuditID AS VARCHAR));
    
    FETCH NEXT FROM OptimizationCursor INTO 
        @v_EmployeeID, @v_OrderID, @v_TerritoryID, @v_LastName, @v_AdjustedPerformanceBonus, @v_CoverageSeniorityScore, @v_RegionalEconomicImpact;
END;

CLOSE OptimizationCursor; 
DEALLOCATE OptimizationCursor;
GO