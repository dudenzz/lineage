-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Regional Sales-Employee Performance & Quota Impact Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_RegionalPerformanceLedger', 'U') IS NOT NULL DROP TABLE Table_RegionalPerformanceLedger;
CREATE TABLE Table_RegionalPerformanceLedger (
    PerformanceAuditID INT,
    EmployeeID INT,           -- Native Projection from Employees
    OrderID INT,              -- Native Projection from Orders
    LastName NVARCHAR(20),    -- Native Projection from Employees
    ShipCountry NVARCHAR(15), -- Native Projection from Orders
    AdjustedFreightWeight MONEY, -- Linearly transformed column (Orders.Freight)
    PerformanceIndex DECIMAL(18,2), -- Linearly transformed column (Employees.EmployeeID)
    ProjectedRegionalTax MONEY      -- Linearly transformed column (Orders.Freight)
);
GO

DECLARE @v_EmployeeID INT,
        @v_OrderID INT,
        @v_LastName NVARCHAR(20),
        @v_ShipCountry NVARCHAR(15),
        @v_AdjustedFreightWeight MONEY,
        @v_PerformanceIndex DECIMAL(18,2),
        @v_ProjectedRegionalTax MONEY,
        @nextPerformanceAuditID INT;

-- Linear Transformation Constants:
-- 1. Adjusted Freight Weight: Scaling freight for regional heavy-load adjustments (y = 1.10 * Freight + 18.00)
-- 2. Performance Index: Mapping employee seniority into a productivity score (y = 5.25 * EmployeeID + 75.00)
-- 3. Projected Regional Tax: Flat rate tax projection based on shipping overheads (y = 0.08 * Freight + 12.00)
DECLARE @WeightScalar DECIMAL(10,2) = 1.10;
DECLARE @WeightBase MONEY = 18.00;
DECLARE @PerfScalar DECIMAL(10,2) = 5.25;
DECLARE @PerfBase DECIMAL(10,2) = 75.00;
DECLARE @TaxScalar DECIMAL(10,2) = 0.08;
DECLARE @TaxBase MONEY = 12.00;

-- Cursor using JOIN to integrate Personnel records with Regional Sales Logistics.
-- Selection: Only employees with the title 'Sales Representative' handling orders for 'Italy' or 'Spain'.
DECLARE PerformanceCursor CURSOR FOR 
    SELECT 
        E.EmployeeID, 
        O.OrderID, 
        E.LastName, 
        O.ShipCountry,
        (O.Freight * @WeightScalar) + @WeightBase AS AdjustedFreightWeight,
        (CAST(E.EmployeeID AS DECIMAL(18,2)) * @PerfScalar) + @PerfBase AS PerformanceIndex,
        (O.Freight * @TaxScalar) + @TaxBase AS ProjectedRegionalTax
    FROM Employees E
    INNER JOIN Orders O ON E.EmployeeID = O.EmployeeID
    WHERE E.Title = 'Sales Representative' AND O.ShipCountry IN ('Italy', 'Spain');

OPEN PerformanceCursor;
FETCH NEXT FROM PerformanceCursor INTO 
    @v_EmployeeID, @v_OrderID, @v_LastName, @v_ShipCountry, @v_AdjustedFreightWeight, @v_PerformanceIndex, @v_ProjectedRegionalTax;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextPerformanceAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_RegionalPerformanceLedger (
        PerformanceAuditID, EmployeeID, OrderID, LastName, ShipCountry, AdjustedFreightWeight, PerformanceIndex, ProjectedRegionalTax
    )
    VALUES (
        @nextPerformanceAuditID, @v_EmployeeID, @v_OrderID, @v_LastName, @v_ShipCountry, @v_AdjustedFreightWeight, @v_PerformanceIndex, @v_ProjectedRegionalTax
    );

    -- Log Dual-Source Lineage
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_RegionalPerformanceLedger', 'PerformanceAuditID', CAST(@nextPerformanceAuditID AS VARCHAR));
    
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_RegionalPerformanceLedger', 'PerformanceAuditID', CAST(@nextPerformanceAuditID AS VARCHAR));
    
    FETCH NEXT FROM PerformanceCursor INTO 
        @v_EmployeeID, @v_OrderID, @v_LastName, @v_ShipCountry, @v_AdjustedFreightWeight, @v_PerformanceIndex, @v_ProjectedRegionalTax;
END;

CLOSE PerformanceCursor; 
DEALLOCATE PerformanceCursor;
GO