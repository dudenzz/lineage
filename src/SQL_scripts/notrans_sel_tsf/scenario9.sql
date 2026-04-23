-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring Discontinued Products for an inventory liquidation and archival registry.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_DiscontinuedProducts AS
SELECT 
    ProductID, 
    ProductName, 
    CategoryID,
    UnitPrice
FROM Products
WHERE Discontinued = 1; -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_DiscontinuedProducts;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_DiscontinuedProducts', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Beverages only).
IF OBJECT_ID('Table_Beverage_Clearance', 'U') IS NOT NULL DROP TABLE Table_Beverage_Clearance;
CREATE TABLE Table_Beverage_Clearance (
    LocalClearanceID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitPrice DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_price DECIMAL(10,2), @nextLocalID INT;
-- Filter: Only process discontinued items in Category 1 (Beverages)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitPrice 
    FROM vw_DiscontinuedProducts 
    WHERE CategoryID = 1;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLocalID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Beverage_Clearance (LocalClearanceID, OriginalProductID, ProductName, UnitPrice)
    VALUES (@nextLocalID, @v_pid, @v_pname, @v_price);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_DiscontinuedProducts', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Beverage_Clearance', 'LocalClearanceID', CAST(@nextLocalID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeClearanceReport AS
BEGIN
    IF OBJECT_ID('Final_ClearanceArchival', 'U') IS NOT NULL DROP TABLE Final_ClearanceArchival;
    CREATE TABLE Final_ClearanceArchival (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        LastKnownPrice DECIMAL(10,2),
        ArchivalStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_price DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ProductName, UnitPrice FROM ##TempClearanceBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ClearanceArchival (ReportID, ItemName, LastKnownPrice, ArchivalStatus)
        VALUES (@finalID, @t_pname, @t_price, 'Archived & Flagged');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempClearanceBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ClearanceArchival', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_price;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessClearanceStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempClearanceBuffer') IS NOT NULL DROP TABLE ##TempClearanceBuffer;
    CREATE TABLE ##TempClearanceBuffer (
        TempID INT, 
        ProductName NVARCHAR(40),
        UnitPrice DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @price DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT LocalClearanceID, ProductName, UnitPrice FROM Table_Beverage_Clearance;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempClearanceBuffer VALUES (@newTempID, @pname, @price);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Beverage_Clearance', 'LocalClearanceID', CAST(@tid AS VARCHAR), '##TempClearanceBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @price;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeClearanceReport;
END;
GO

EXEC proc_ProcessClearanceStaging;