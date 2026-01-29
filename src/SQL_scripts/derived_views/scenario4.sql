-- Section 0: Ensure Sequence Exists
IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'GlobalIDSequence')
    CREATE SEQUENCE GlobalIDSequence START WITH 1 INCREMENT BY 1;
GO

-- Section 1: Aggregation View (Category Sales)
-- Tests: Lineage through GROUP BY and SUM.
CREATE OR ALTER VIEW vw_CategorySalesPerformance AS
SELECT 
    c.CategoryID,
    c.CategoryName,
    SUM(od.UnitPrice * od.Quantity) AS TotalCategoryRevenue
FROM Categories c
JOIN Products p ON c.CategoryID = p.CategoryID
JOIN [Order Details] od ON p.ProductID = od.ProductID
GROUP BY c.CategoryID, c.CategoryName;
GO

-- Log Lineage: View Dependency
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
VALUES ('Categories', 'CategoryID', 'Agg-Logic', 'vw_CategorySalesPerformance', 'CategoryID', 'View-Result');
GO

-- Section 2: Comparison View (Supplier to Category Benchmark)
-- Tests: Nested views (View-on-View + Table Join).
CREATE OR ALTER VIEW vw_SupplierVsCategoryRevenue AS
SELECT 
    s.SupplierID,
    s.CompanyName,
    v.CategoryName,
    v.TotalCategoryRevenue,
    SUM(od.UnitPrice * od.Quantity) AS SupplierContribution
FROM Suppliers s
JOIN Products p ON s.SupplierID = p.SupplierID
JOIN [Order Details] od ON p.ProductID = od.ProductID
JOIN vw_CategorySalesPerformance v ON p.CategoryID = v.CategoryID
GROUP BY s.SupplierID, s.CompanyName, v.CategoryName, v.TotalCategoryRevenue;
GO

-- Section 3: Physical Table Persistence
-- Tests: Moving complex derived view data into a permanent table with a Computed Column.
IF OBJECT_ID('Table_Supplier_Benchmarks', 'U') IS NOT NULL DROP TABLE Table_Supplier_Benchmarks;
CREATE TABLE Table_Supplier_Benchmarks (
    BenchmarkID INT PRIMARY KEY,
    SupplierID INT,
    RevenueContribution DECIMAL(18,2),
    CategoryRevenueReference DECIMAL(18,2), -- Added to hold the denominator for the computed column
    MarketSharePercentage AS (RevenueContribution / NULLIF(CategoryRevenueReference, 0))
);

-- Set-based insert to avoid cursor-column mismatch
INSERT INTO Table_Supplier_Benchmarks (BenchmarkID, SupplierID, RevenueContribution, CategoryRevenueReference)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    SupplierID, 
    SupplierContribution, 
    TotalCategoryRevenue
FROM vw_SupplierVsCategoryRevenue;

-- Log Lineage: Map View to Physical Table
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'vw_SupplierVsCategoryRevenue', 'SupplierID', CAST(SupplierID AS VARCHAR), 'Table_Supplier_Benchmarks', 'BenchmarkID', 'Persisted'
FROM Table_Supplier_Benchmarks;
GO

-- Section 4: Final Procedure for Management Dashboard
-- Tests: Multi-hop resolution from physical persistence to terminal node.
CREATE OR ALTER PROCEDURE proc_FinalizeManagementDashboard AS
BEGIN
    IF OBJECT_ID('Final_Executive_Dashboard', 'U') IS NOT NULL DROP TABLE Final_Executive_Dashboard;
    CREATE TABLE Final_Executive_Dashboard (
        DashID INT PRIMARY KEY, 
        SupplierRef INT, 
        PerformanceScore DECIMAL(18,2)
    );

    INSERT INTO Final_Executive_Dashboard (DashID, SupplierRef, PerformanceScore)
    SELECT 
        NEXT VALUE FOR GlobalIDSequence,
        SupplierID,
        RevenueContribution * 0.1
    FROM Table_Supplier_Benchmarks;

    -- Log Lineage: Terminal Node
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Supplier_Benchmarks', 'BenchmarkID', CAST(BenchmarkID AS VARCHAR), 'Final_Executive_Dashboard', 'DashID', 'Final-KPI'
    FROM Table_Supplier_Benchmarks;
END;
GO

EXEC proc_FinalizeManagementDashboard;