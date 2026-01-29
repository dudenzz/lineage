-- Section 1: Recursive CTE for Revenue Aggregation
-- Tests: Lineage through a join between a recursive CTE and a physical Fact table (Orders).
WITH MemberSales AS (
    -- Get base sales for every employee
    SELECT 
        e.EmployeeID,
        e.ReportsTo,
        ISNULL(SUM(od.UnitPrice * od.Quantity), 0) AS DirectSales
    FROM Employees e
    LEFT JOIN Orders o ON e.EmployeeID = o.EmployeeID
    LEFT JOIN [Order Details] od ON o.OrderID = od.OrderID
    GROUP BY e.EmployeeID, e.ReportsTo
),
RollupHierarchy AS (
    -- Anchor: Leaf nodes (Employees who are not managers)
    SELECT 
        EmployeeID,
        ReportsTo,
        DirectSales AS TotalBranchRevenue,
        CAST(EmployeeID AS VARCHAR(MAX)) AS Path
    FROM MemberSales
    
    UNION ALL

    -- Recursive: Add direct sales to the parent's total
    SELECT 
        m.EmployeeID,
        m.ReportsTo,
        m.DirectSales + rh.TotalBranchRevenue,
        rh.Path + '|' + CAST(m.EmployeeID AS VARCHAR(MAX))
    FROM MemberSales m
    INNER JOIN RollupHierarchy rh ON m.EmployeeID = rh.ReportsTo
)
-- Materialize the roll-up into a temporary table
SELECT * INTO #TempRevenueRollup FROM RollupHierarchy;

-- Log Lineage:
-- Tool must link 'Employees', 'Orders', and 'Order Details' to '#TempRevenueRollup'
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Orders', 'OrderID', 'Aggregation', '#TempRevenueRollup', 'EmployeeID', CAST(EmployeeID AS VARCHAR)
FROM #TempRevenueRollup;
GO

-- Section 2: Persistence to Management Table
-- Tests: Finalizing lineage from a complex roll-up to a physical reporting object.
IF OBJECT_ID('Final_Management_Revenue_Report', 'U') IS NOT NULL DROP TABLE Final_Management_Revenue_Report;
CREATE TABLE Final_Management_Revenue_Report (
    ReportID INT PRIMARY KEY,
    MgrID INT,
    TotalRollupRevenue DECIMAL(18,2)
);

DECLARE @mgr_id INT, @rev DECIMAL(18,2), @nextID INT;
DECLARE RollupCursor CURSOR FOR 
    SELECT EmployeeID, MAX(TotalBranchRevenue) -- Get the highest roll-up value per manager
    FROM #TempRevenueRollup 
    GROUP BY EmployeeID;

OPEN RollupCursor;
FETCH NEXT FROM RollupCursor INTO @mgr_id, @rev;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Final_Management_Revenue_Report (ReportID, MgrID, TotalRollupRevenue)
    VALUES (@nextID, @mgr_id, @rev);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('#TempRevenueRollup', 'EmployeeID', CAST(@mgr_id AS VARCHAR), 'Final_Management_Revenue_Report', 'ReportID', CAST(@nextID AS VARCHAR));

    FETCH NEXT FROM RollupCursor INTO @mgr_id, @rev;
END;
CLOSE RollupCursor; DEALLOCATE RollupCursor;

DROP TABLE #TempRevenueRollup;
GO