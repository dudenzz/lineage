-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring glass-packaged Products for a fragile handling and logistics queue.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_FragileProducts AS
SELECT 
    ProductID, 
    ProductName, 
    QuantityPerUnit,
    CategoryID
FROM Products
WHERE QuantityPerUnit LIKE '%bottle%' OR QuantityPerUnit LIKE '%glass%'; -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_FragileProducts;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_FragileProducts', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Beverages only).
IF OBJECT_ID('Table_Beverage_Bottles', 'U') IS NOT NULL DROP TABLE Table_Beverage_Bottles;
CREATE TABLE Table_Beverage_Bottles (
    HandlingID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    PackagingDetails NVARCHAR(20)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_qty NVARCHAR(20), @nextHandlingID INT;
-- Filter: Only process fragile items from Category 1 (Beverages)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, QuantityPerUnit 
    FROM vw_FragileProducts 
    WHERE CategoryID = 1;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_qty;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextHandlingID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Beverage_Bottles (HandlingID, OriginalProductID, ProductName, PackagingDetails)
    VALUES (@nextHandlingID, @v_pid, @v_pname, @v_qty);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_FragileProducts', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Beverage_Bottles', 'HandlingID', CAST(@nextHandlingID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_qty;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeHandlingReport AS
BEGIN
    IF OBJECT_ID('Final_FragilePackagingRegistry', 'U') IS NOT NULL DROP TABLE Final_FragilePackagingRegistry;
    CREATE TABLE Final_FragilePackagingRegistry (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        PackageFormat NVARCHAR(20),
        HandlingStatus VARCHAR(30)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_qty NVARCHAR(20), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ProductName, PackagingDetails FROM ##TempFragileBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_qty;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_FragilePackagingRegistry (ReportID, ItemName, PackageFormat, HandlingStatus)
        VALUES (@finalID, @t_pname, @t_qty, 'Bubble Wrap Required');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempFragileBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_FragilePackagingRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_qty;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessFragileStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempFragileBuffer') IS NOT NULL DROP TABLE ##TempFragileBuffer;
    CREATE TABLE ##TempFragileBuffer (
        TempID INT, 
        ProductName NVARCHAR(40),
        PackagingDetails NVARCHAR(20)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @qty NVARCHAR(20), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT HandlingID, ProductName, PackagingDetails FROM Table_Beverage_Bottles;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @qty;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempFragileBuffer VALUES (@newTempID, @pname, @qty);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Beverage_Bottles', 'HandlingID', CAST(@tid AS VARCHAR), '##TempFragileBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @qty;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeHandlingReport;
END;
GO

EXEC proc_ProcessFragileStaging;