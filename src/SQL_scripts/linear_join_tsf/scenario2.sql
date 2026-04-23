-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Personnel Productivity & Regional Revenue Contribution Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_StaffProductivityLedger', 'U') IS NOT NULL DROP TABLE Table_StaffProductivityLedger;
CREATE TABLE Table_StaffProductivityLedger (
    StaffAuditID INT,
    EmployeeID INT,          -- Native Projection from Employees
    OrderID INT,             -- Native Projection from Orders
    LastName NVARCHAR(20),   -- Native Projection from Employees
    ProjectedCommission MONEY, -- Linearly transformed column (Orders.Freight)
    TenureImpactScore DECIMAL(18,2), -- Linearly transformed column (Employees.EmployeeID)
    RegionalWeight MONEY      -- Linearly transformed column (Orders.Freight)
);
GO

DECLARE @v_EmployeeID INT,
        @v_OrderID INT,
        @v_LastName NVARCHAR(20),
        @v_ProjectedCommission MONEY,
        @v_TenureImpactScore DECIMAL(18,2),
        @v_RegionalWeight MONEY,
        @nextStaffAuditID INT;

-- Linear Transformation Constants:
-- 1. Projected Commission: Calculated as a percentage of freight handled plus a base bonus (y = 0.12 * Freight + 15.00)
-- 2. Tenure Impact Score: Using EmployeeID as a proxy for seniority/tenure (y = 1.40 * EmployeeID + 100.00)
-- 3. Regional Weight: Adjusted logistical impact for specific zones (y = 0.50 * Freight + 45.00)
DECLARE @CommissionScalar DECIMAL(10,2) = 0.12;
DECLARE @CommissionBase MONEY = 15.00;
DECLARE @TenureScalar DECIMAL(10,2) = 1.40;
DECLARE @TenureBase DECIMAL(10,2) = 100.00;
DECLARE @RegionalScalar DECIMAL(10,2) = 0.50;
DECLARE @RegionalBase MONEY = 45.00;

-- Cursor using JOIN to integrate Human Resources and Sales Fulfillment.
-- Selection: Only employees with the title 'Sales Representative' handling orders for 'USA' or 'Canada'.
DECLARE StaffProductivityCursor CURSOR FOR 
    SELECT 
        E.EmployeeID, 
        O.OrderID, 
        E.LastName, 
        (O.Freight * @CommissionScalar) + @CommissionBase AS ProjectedCommission,
        (CAST(E.EmployeeID AS DECIMAL(18,2)) * @TenureScalar) + @TenureBase AS TenureImpactScore,
        (O.Freight * @RegionalScalar) + @RegionalBase AS RegionalWeight
    FROM Employees E
    INNER JOIN Orders O ON E.EmployeeID = O.EmployeeID
    WHERE E.Title = 'Sales Representative' AND O.ShipCountry IN ('USA', 'Canada');

OPEN StaffProductivityCursor;
FETCH NEXT FROM StaffProductivityCursor INTO 
    @v_EmployeeID, @v_OrderID, @v_LastName, @v_ProjectedCommission, @v_TenureImpactScore, @v_RegionalWeight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextStaffAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_StaffProductivityLedger (
        StaffAuditID, EmployeeID, OrderID, LastName, ProjectedCommission, TenureImpactScore, RegionalWeight
    )
    VALUES (
        @nextStaffAuditID, @v_EmployeeID, @v_OrderID, @v_LastName, @v_ProjectedCommission, @v_TenureImpactScore, @v_RegionalWeight
    );

    -- Log Dual-Source Lineage
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_StaffProductivityLedger', 'StaffAuditID', CAST(@nextStaffAuditID AS VARCHAR));
    
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_StaffProductivityLedger', 'StaffAuditID', CAST(@nextStaffAuditID AS VARCHAR));
    
    FETCH NEXT FROM StaffProductivityCursor INTO 
        @v_EmployeeID, @v_OrderID, @v_LastName, @v_ProjectedCommission, @v_TenureImpactScore, @v_RegionalWeight;
END;

CLOSE StaffProductivityCursor; 
DEALLOCATE StaffProductivityCursor;
GO