-- Section 1: Setup the Rolling Accumulator Table
-- Tests: Lineage into a table that persists across multiple batch runs.
IF OBJECT_ID('Table_Daily_Sales_Accumulator', 'U') IS NOT NULL DROP TABLE Table_Daily_Sales_Accumulator;

CREATE TABLE Table_Daily_Sales_Accumulator (
    SummaryDate DATE PRIMARY KEY,
    DailyRevenue DECIMAL(18,2),
    DailyVolume INT,
    TopProductCategory NVARCHAR(50)
);
GO

-- Section 2: The Incremental Load Procedure
-- Tests: Lineage through a procedure designed to run daily.
-- The tool must capture the dependency on Products, Categories, and Order Details.
CREATE OR ALTER PROCEDURE proc_UpdateDailySalesAccumulator 
    @TargetDate DATE
AS
BEGIN
    -- Identify the category with the highest sales for the target date
    DECLARE @TopCat NVARCHAR(50);
    
    SELECT TOP 1 @TopCat = c.CategoryName
    FROM [Order Details] od
    JOIN Orders o ON od.OrderID = o.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    JOIN Categories c ON p.CategoryID = c.CategoryID
    WHERE CAST(o.OrderDate AS DATE) = @TargetDate
    GROUP BY c.CategoryName
    ORDER BY SUM(od.UnitPrice * od.Quantity) DESC;

    -- Insert the summarized snapshot for the day
    INSERT INTO Table_Daily_Sales_Accumulator (SummaryDate, DailyRevenue, DailyVolume, TopProductCategory)
    SELECT 
        CAST(o.OrderDate AS DATE),
        SUM(od.UnitPrice * od.Quantity),
        COUNT(DISTINCT o.OrderID),
        @TopCat
    FROM Orders o
    JOIN [Order Details] od ON o.OrderID = od.OrderID
    WHERE CAST(o.OrderDate AS DATE) = @TargetDate
    GROUP BY CAST(o.OrderDate AS DATE);

    -- Log Lineage:
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderDate', CAST(@TargetDate AS VARCHAR), 'Table_Daily_Sales_Accumulator', 'SummaryDate', CAST(@TargetDate AS VARCHAR));
END;
GO

-- Simulate three days of incremental loads
EXEC proc_UpdateDailySalesAccumulator '1997-01-01';
EXEC proc_UpdateDailySalesAccumulator '1997-01-02';
EXEC proc_UpdateDailySalesAccumulator '1997-01-03';
GO

-- Section 3: Period-End Materialization
-- Tests: Lineage from an incremental accumulator to a final period-end report.
IF OBJECT_ID('Final_Q1_Summary', 'U') IS NOT NULL DROP TABLE Final_Q1_Summary;
CREATE TABLE Final_Q1_Summary (QuarterLabel NVARCHAR(10), TotalQ1Revenue DECIMAL(18,2));

INSERT INTO Final_Q1_Summary (QuarterLabel, TotalQ1Revenue)
SELECT '1997-Q1', SUM(DailyRevenue)
FROM Table_Daily_Sales_Accumulator
WHERE SummaryDate BETWEEN '1997-01-01' AND '1997-03-31';