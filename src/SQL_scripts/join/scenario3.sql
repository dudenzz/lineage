-- Section 1: Create View with a Three-Way Inner Join
-- Tests: Mapping a view record to three distinct source tables simultaneously.
CREATE OR ALTER VIEW vw_ProductFullCatalog AS
SELECT 
    p.ProductID, 
    p.ProductName, 
    c.CategoryName, 
    s.CompanyName AS SupplierName
FROM Products p
INNER JOIN Categories c ON p.CategoryID = c.CategoryID
INNER JOIN Suppliers s ON p.SupplierID = s.SupplierID;
GO

-- Log Row-Level Lineage for Three-Way Join
DECLARE @pid INT, @cid INT, @sid INT;
DECLARE ViewCursor CURSOR FOR 
    SELECT p.ProductID, c.CategoryID, s.SupplierID 
    FROM Products p
    JOIN Categories c ON p.CategoryID = c.CategoryID
    JOIN Suppliers s ON p.SupplierID = s.SupplierID;

OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid, @cid, @sid;
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Product Source
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ProductFullCatalog', 'ProductID', CAST(@pid AS VARCHAR));
    
    -- Category Source
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@cid AS VARCHAR), 'vw_ProductFullCatalog', 'ProductID', CAST(@pid AS VARCHAR));

    -- Supplier Source
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@sid AS VARCHAR), 'vw_ProductFullCatalog', 'ProductID', CAST(@pid AS VARCHAR));

    FETCH NEXT FROM ViewCursor INTO @pid, @cid, @sid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: View -> Table_InventoryStaging
-- Tests: Handing off joined metadata to a physical staging table.
IF OBJECT_ID('Table_InventoryStaging', 'U') IS NOT NULL DROP TABLE Table_InventoryStaging;
CREATE TABLE Table_InventoryStaging (
    StageID INT, 
    OriginalProductID INT, 
    FullDescription NVARCHAR(255)
);

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_cname NVARCHAR(15), @v_sname NVARCHAR(40), @nextStageID INT;
DECLARE TableCursor CURSOR FOR SELECT ProductID, ProductName, CategoryName, SupplierName FROM vw_ProductFullCatalog;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_cname, @v_sname;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextStageID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_InventoryStaging (StageID, OriginalProductID, FullDescription)
    VALUES (@nextStageID, @v_pid, @v_pname + ' (' + @v_cname + ') via ' + @v_sname);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ProductFullCatalog', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_InventoryStaging', 'StageID', CAST(@nextStageID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_cname, @v_sname;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Procedures for Procurement Reporting
CREATE OR ALTER PROCEDURE proc_FinalizeProcurementReport AS
BEGIN
    IF OBJECT_ID('Final_ProcurementAudit', 'U') IS NOT NULL DROP TABLE Final_ProcurementAudit;
    CREATE TABLE Final_ProcurementAudit (AuditID INT, DescriptionSnippet NVARCHAR(100));

    DECLARE @t_id INT, @t_desc NVARCHAR(255), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, FullDescription FROM ##TempProcurementBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_desc;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ProcurementAudit (AuditID, DescriptionSnippet)
        VALUES (@finalID, LEFT(@t_desc, 100));

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempProcurementBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ProcurementAudit', 'AuditID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_desc;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StagingProductJoins AS
BEGIN
    IF OBJECT_ID('tempdb..##TempProcurementBuffer') IS NOT NULL DROP TABLE ##TempProcurementBuffer;
    CREATE TABLE ##TempProcurementBuffer (TempID INT, FullDescription NVARCHAR(255));

    DECLARE @sid INT, @desc NVARCHAR(255), @newTempID INT;
    -- Filter for a specific category string that originated in the 'Categories' table
    DECLARE ProcCursor CURSOR FOR SELECT StageID, FullDescription FROM Table_InventoryStaging WHERE FullDescription LIKE '%Beverages%';

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @sid, @desc;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempProcurementBuffer VALUES (@newTempID, @desc);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_InventoryStaging', 'StageID', CAST(@sid AS VARCHAR), '##TempProcurementBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @sid, @desc;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeProcurementReport;
END;
GO

EXEC proc_StagingProductJoins;