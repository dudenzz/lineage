-- Section 1: Materializing Latest Pricing Snapshot
IF OBJECT_ID('Table_Latest_Price_Snapshot', 'U') IS NOT NULL DROP TABLE Table_Latest_Price_Snapshot;

CREATE TABLE Table_Latest_Price_Snapshot (
    ProductID INT PRIMARY KEY,
    CurrentPrice DECIMAL(18,2),
    PriceSourceID INT
);

INSERT INTO Table_Latest_Price_Snapshot (ProductID, CurrentPrice, PriceSourceID)
SELECT ProductID, UnitPrice, OrderID
FROM (
    SELECT 
        ProductID, 
        UnitPrice, 
        OrderID,
        ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY OrderID DESC) as PriceRank
    FROM [Order Details]
) AS RankedPrices
WHERE PriceRank = 1;
GO