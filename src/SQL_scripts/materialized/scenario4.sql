-- Section 1: Define the Comparative Summary Table
-- Tests: Lineage into a table containing both actuals and historical references.
IF OBJECT_ID('Table_YOY_Sales_Comparison', 'U') IS NOT NULL DROP TABLE Table_YOY_Sales_Comparison;

CREATE TABLE Table_YOY_Sales_Comparison (
    ComparisonID INT PRIMARY KEY,
    CategoryID INT,
    CurrentYearRevenue DECIMAL(18,2),
    PriorYearRevenue DECIMAL(18,2),
    GrowthPercentage DECIMAL(18,4),
    CalculationDate DATETIME DEFAULT GETDATE()
);
GO

-- Section 2: Materializing the Comparative Matrix
-- Tests: Lineage resolution when the same source table (Orders/Order Details) 
-- is accessed twice via different aliases to represent different time periods.
INSERT INTO Table_YOY_Sales_Comparison (
    ComparisonID, 
    CategoryID, 
    CurrentYearRevenue, 
    PriorYearRevenue, 
    GrowthPercentage
)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    Curr.CatID,
    Curr.Revenue AS CurrentRev,
    Prev.Revenue AS PriorRev,
    (Curr.Revenue - Prev.Revenue) / NULLIF(Prev.Revenue, 0) AS Growth
FROM (
    -- Sub-query for 1997 (Current)
    SELECT p.CategoryID AS CatID, SUM(od.UnitPrice * od.Quantity) AS Revenue
    FROM [Order Details] od
    JOIN Orders o ON od.OrderID = o.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    WHERE o.OrderDate BETWEEN '1997-01-01' AND '1997-12-31'
    GROUP BY p.CategoryID
) Curr
LEFT JOIN (
    -- Sub-query for 1996 (Prior)
    SELECT p.CategoryID AS CatID, SUM(od.UnitPrice * od.Quantity) AS Revenue
    FROM [Order Details] od
    JOIN Orders o ON od.OrderID = o.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    WHERE o.OrderDate BETWEEN '1996-01-01' AND '1996-12-31'
    GROUP BY p.CategoryID
) Prev ON Curr.CatID = Prev.CatID;

-- Log Lineage:
-- Tool must show two distinct paths from 'Orders' to the target, distinguished by date filters.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Order Details', 'Multiple', 'Year-Split', 'Table_YOY_Sales_Comparison', 'ComparisonID', 'YOY_96_97'
FROM Table_YOY_Sales_Comparison;
GO

-- Section 3: High-Growth Strategy Reporting
-- Tests: Lineage from a comparative summary to a strategic planning table.
IF OBJECT_ID('Final_Growth_Strategy_Brief', 'U') IS NOT NULL DROP TABLE Final_Growth_Strategy_Brief;
CREATE TABLE Final_Growth_Strategy_Brief (
    BriefID INT PRIMARY KEY,
    TargetCategoryID INT,
    Status NVARCHAR(20)
);

INSERT INTO Final_Growth_Strategy_Brief (BriefID, TargetCategoryID, Status)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    CategoryID,
    CASE WHEN GrowthPercentage > 0.1 THEN 'Expansion' ELSE 'Stability' END
FROM Table_YOY_Sales_Comparison;

-- Log Lineage:
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Table_YOY_Sales_Comparison', 'ComparisonID', 'Strategy-Logic', 'Final_Growth_Strategy_Brief', 'BriefID', 'Strategic_Status'
FROM Final_Growth_Strategy_Brief;
GO