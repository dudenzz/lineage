-- Section 1: Create a view with Grouping
-- Tests: Lineage through aggregation. The PK of this view is 'CategoryID'.
CREATE VIEW vw_CategoryStockAnalysis AS
SELECT 
    CategoryID, 
    SUM(UnitsInStock) AS TotalStock,
    AVG(UnitPrice) AS AveragePrice
FROM Products
GROUP BY CategoryID;
GO

-- Log row-level lineage for the Grouping
-- Note: Multiple ProductIDs feed into one CategoryID.
DECLARE @catID INT;
DECLARE ViewCursor CURSOR FOR SELECT CategoryID FROM vw_CategoryStockAnalysis;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @catID;
WHILE @@FETCH_STATUS = 0
BEGIN
    -- This captures that many Products were 'used for creation' of one Category row
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Products', 'ProductID', CAST(ProductID AS VARCHAR), 'vw_CategoryStockAnalysis', 'CategoryID', CAST(@catID AS VARCHAR)
    FROM Products WHERE CategoryID = @catID;

    FETCH NEXT FROM ViewCursor INTO @catID;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Table_CategoryTiers (Transformation with CASE)
IF OBJECT_ID('Table_CategoryTiers', 'U') IS NOT NULL DROP TABLE Table_CategoryTiers;
CREATE TABLE Table_CategoryTiers (
    TierID INT, 
    CategoryID INT, 
    StockLevel VARCHAR(20), 
    AvgPrice DECIMAL(18,2)
);

DECLARE @c_id INT, @c_stock INT, @c_price DECIMAL(18,2), @nextTierID INT;
DECLARE TableCursor CURSOR FOR SELECT CategoryID, TotalStock, AveragePrice FROM vw_CategoryStockAnalysis;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @c_id, @c_stock, @c_price;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextTierID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_CategoryTiers (TierID, CategoryID, StockLevel, AvgPrice)
    VALUES (@nextTierID, @c_id, 
            CASE WHEN @c_stock > 300 THEN 'Abundant' ELSE 'Limited' END, 
            @c_price);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_CategoryStockAnalysis', 'CategoryID', CAST(@c_id AS VARCHAR), 'Table_CategoryTiers', 'TierID', CAST(@nextTierID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @c_id, @c_stock, @c_price;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using ##TempTable
CREATE OR ALTER PROCEDURE proc_FinalizeCategoryReport AS
BEGIN
    IF OBJECT_ID('Final_CategoryReport', 'U') IS NOT NULL DROP TABLE Final_CategoryReport;
    CREATE TABLE Final_CategoryReport (ReportID INT, CategoryID INT, AdjustedPrice DECIMAL(18,2));

    DECLARE @t_id INT, @t_cat INT, @t_price DECIMAL(18,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CategoryID, AvgPrice FROM ##TempPriceBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cat, @t_price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        -- Apply a transformation (e.g., a 10% markup)
        INSERT INTO Final_CategoryReport (ReportID, CategoryID, AdjustedPrice)
        VALUES (@finalID, @t_cat, @t_price * 1.1);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempPriceBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_CategoryReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cat, @t_price;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessCategoryTiers AS
BEGIN
    IF OBJECT_ID('tempdb..##TempPriceBuffer') IS NOT NULL DROP TABLE ##TempPriceBuffer;
    CREATE TABLE ##TempPriceBuffer (TempID INT, CategoryID INT, AvgPrice DECIMAL(18,2));

    DECLARE @tid INT, @cid INT, @price DECIMAL(18,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT TierID, CategoryID, AvgPrice FROM Table_CategoryTiers WHERE StockLevel = 'Abundant';

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempPriceBuffer VALUES (@newTempID, @cid, @price);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_CategoryTiers', 'TierID', CAST(@tid AS VARCHAR), '##TempPriceBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @price;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeCategoryReport;
END;
GO

EXEC proc_ProcessCategoryTiers;