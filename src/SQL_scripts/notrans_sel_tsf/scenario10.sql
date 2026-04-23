-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring out-of-stock Products for a supply chain replenishment registry.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_OutOfStockProducts AS
SELECT 
    ProductID, 
    ProductName, 
    SupplierID,
    CategoryID
FROM Products
WHERE UnitsInStock = 0; -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_OutOfStockProducts;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_OutOfStockProducts', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to a specific supplier).
IF OBJECT_ID('Table_UrgentRestock_Supplier1', 'U') IS NOT NULL DROP TABLE Table_UrgentRestock_Supplier1;
CREATE TABLE Table_UrgentRestock_Supplier1 (
    RestockID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @nextRestockID INT;
-- Filter: Only process out-of-stock items that need to be reordered from Supplier 1 (Exotic Liquids)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName 
    FROM vw_OutOfStockProducts 
    WHERE SupplierID = 1;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextRestockID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UrgentRestock_Supplier1 (RestockID, OriginalProductID, ProductName)
    VALUES (@nextRestockID, @v_pid, @v_pname);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_OutOfStockProducts', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_UrgentRestock_Supplier1', 'RestockID', CAST(@nextRestockID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeRestockReport AS
BEGIN
    IF OBJECT_ID('Final_ReplenishmentRegistry', 'U') IS NOT NULL DROP TABLE Final_ReplenishmentRegistry;
    CREATE TABLE Final_ReplenishmentRegistry (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        OrderPriority VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ProductName FROM ##TempRestockBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ReplenishmentRegistry (ReportID, ItemName, OrderPriority)
        VALUES (@finalID, @t_pname, 'Critical / Urgent');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempRestockBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ReplenishmentRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessRestockStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempRestockBuffer') IS NOT NULL DROP TABLE ##TempRestockBuffer;
    CREATE TABLE ##TempRestockBuffer (
        TempID INT, 
        ProductName NVARCHAR(40)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT RestockID, ProductName FROM Table_UrgentRestock_Supplier1;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempRestockBuffer VALUES (@newTempID, @pname);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_UrgentRestock_Supplier1', 'RestockID', CAST(@tid AS VARCHAR), '##TempRestockBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeRestockReport;
END;
GO

EXEC proc_ProcessRestockStaging;