-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring budget-friendly Products for a clearance promotion catalog.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_BudgetProducts AS
SELECT 
    ProductID, 
    ProductName, 
    CategoryID,
    UnitPrice
FROM Products
WHERE UnitPrice < 10.00; -- Selection applied here (Bargain Items)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_BudgetProducts;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_BudgetProducts', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Condiments only).
IF OBJECT_ID('Table_Condiments_Bargain', 'U') IS NOT NULL DROP TABLE Table_Condiments_Bargain;
CREATE TABLE Table_Condiments_Bargain (
    PromoID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitPrice DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_price DECIMAL(10,2), @nextPromoID INT;
-- Filter: Only process budget items from Category 2 (Condiments)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitPrice 
    FROM vw_BudgetProducts 
    WHERE CategoryID = 2;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextPromoID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Condiments_Bargain (PromoID, OriginalProductID, ProductName, UnitPrice)
    VALUES (@nextPromoID, @v_pid, @v_pname, @v_price);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_BudgetProducts', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Condiments_Bargain', 'PromoID', CAST(@nextPromoID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizePromoReport AS
BEGIN
    IF OBJECT_ID('Final_BargainPromoCatalog', 'U') IS NOT NULL DROP TABLE Final_BargainPromoCatalog;
    CREATE TABLE Final_BargainPromoCatalog (
        ReportID INT, 
        PromoItemName NVARCHAR(40), 
        PromoPrice DECIMAL(10,2),
        CampaignStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_price DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ProductName, UnitPrice FROM ##TempPromoBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_BargainPromoCatalog (ReportID, PromoItemName, PromoPrice, CampaignStatus)
        VALUES (@finalID, @t_pname, @t_price, 'Added to Flyer');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempPromoBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_BargainPromoCatalog', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_price;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessPromoStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempPromoBuffer') IS NOT NULL DROP TABLE ##TempPromoBuffer;
    CREATE TABLE ##TempPromoBuffer (
        TempID INT, 
        ProductName NVARCHAR(40),
        UnitPrice DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @price DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT PromoID, ProductName, UnitPrice FROM Table_Condiments_Bargain;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempPromoBuffer VALUES (@newTempID, @pname, @price);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Condiments_Bargain', 'PromoID', CAST(@tid AS VARCHAR), '##TempPromoBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @price;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizePromoReport;
END;
GO

EXEC proc_ProcessPromoStaging;