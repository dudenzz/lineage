-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring high-value Premium Products for a VIP customer catalog.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_PremiumProducts AS
SELECT 
    ProductID, 
    ProductName, 
    CategoryID,
    UnitPrice
FROM Products
WHERE UnitPrice >= 50.00; -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_PremiumProducts;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_PremiumProducts', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Confections only).
IF OBJECT_ID('Table_Premium_Confections', 'U') IS NOT NULL DROP TABLE Table_Premium_Confections;
CREATE TABLE Table_Premium_Confections (
    LocalPremiumID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitPrice DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_price DECIMAL(10,2), @nextLocalID INT;
-- Filter: Only process premium items from Category 3 (Confections)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitPrice 
    FROM vw_PremiumProducts 
    WHERE CategoryID = 3;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLocalID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Premium_Confections (LocalPremiumID, OriginalProductID, ProductName, UnitPrice)
    VALUES (@nextLocalID, @v_pid, @v_pname, @v_price);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_PremiumProducts', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Premium_Confections', 'LocalPremiumID', CAST(@nextLocalID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizePremiumReport AS
BEGIN
    IF OBJECT_ID('Final_VIPCatalog', 'U') IS NOT NULL DROP TABLE Final_VIPCatalog;
    CREATE TABLE Final_VIPCatalog (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        ListedPrice DECIMAL(10,2),
        CatalogStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_price DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ProductName, UnitPrice FROM ##TempPremiumBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_VIPCatalog (ReportID, ItemName, ListedPrice, CatalogStatus)
        VALUES (@finalID, @t_pname, @t_price, 'Approved for VIP');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempPremiumBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_VIPCatalog', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_price;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessPremiumStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempPremiumBuffer') IS NOT NULL DROP TABLE ##TempPremiumBuffer;
    CREATE TABLE ##TempPremiumBuffer (
        TempID INT, 
        ProductName NVARCHAR(40),
        UnitPrice DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @price DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT LocalPremiumID, ProductName, UnitPrice FROM Table_Premium_Confections;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempPremiumBuffer VALUES (@newTempID, @pname, @price);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Premium_Confections', 'LocalPremiumID', CAST(@tid AS VARCHAR), '##TempPremiumBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @price;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizePremiumReport;
END;
GO

EXEC proc_ProcessPremiumStaging;