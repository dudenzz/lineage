-- Level 1: View
CREATE VIEW vw_OrderTotals AS 
SELECT OrderID, SUM(UnitPrice * Quantity) AS GrossAmount
FROM [Order Details] GROUP BY OrderID;

-- Level 2: Table
SELECT OrderID, GrossAmount, 
       CASE WHEN GrossAmount > 1000 THEN 'Tier1' ELSE 'Tier2' END AS Category
INTO Table_OrderCategories FROM vw_OrderTotals;

-- Level 3 & 4: Procedures
CREATE PROCEDURE proc_ProcessHighValueOrders AS
BEGIN
    SELECT OrderID, GrossAmount INTO #TempHighValue 
    FROM Table_OrderCategories WHERE Category = 'Tier1';

    EXEC proc_FinalizeHighValueReport;
END;

CREATE PROCEDURE proc_FinalizeHighValueReport AS
BEGIN
    INSERT INTO Final_HighValueReport (OrderID, FinalAmount)
    SELECT OrderID, GrossAmount * 0.9 FROM #TempHighValue; -- Applying discount
END;