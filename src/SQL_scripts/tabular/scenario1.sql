-- Section 1: Define the Pivoted Target Table
-- Tests: Mapping lineage to a table where columns represent specific data values.
IF OBJECT_ID('Table_Quarterly_Pivot_Summary', 'U') IS NOT NULL DROP TABLE Table_Quarterly_Pivot_Summary;

CREATE TABLE Table_Quarterly_Pivot_Summary (
    CategoryID INT PRIMARY KEY,
    Q1_Sales DECIMAL(18,2),
    Q2_Sales DECIMAL(18,2),
    Q3_Sales DECIMAL(18,2),
    Q4_Sales DECIMAL(18,2)
);
GO

-- Section 2: Executing the PIVOT Lineage
-- Tests: Precision in mapping source 'SubTotal' to specific target 'QX_Sales' columns
-- based on the 'Quarter' value.
INSERT INTO Table_Quarterly_Pivot_Summary (CategoryID, Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales)
SELECT CategoryID, [1], [2], [3], [4]
FROM (
    SELECT 
        p.CategoryID, 
        DATEPART(QUARTER, o.OrderDate) AS [Quarter],
        (od.UnitPrice * od.Quantity) AS SubTotal
    FROM [Order Details] od
    JOIN Orders o ON od.OrderID = o.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
) SourceTable
PIVOT (
    SUM(SubTotal)
    FOR [Quarter] IN ([1], [2], [3], [4])
) AS PivotTable;

-- Log Lineage:
-- Tool must show that 'Q1_Sales' is derived from 'SubTotal' WHERE Quarter = 1.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Order Details', 'UnitPrice', 'Pivot-Transform', 'Table_Quarterly_Pivot_Summary', 'CategoryID', 'Annual-Matrix'
FROM Table_Quarterly_Pivot_Summary;
GO