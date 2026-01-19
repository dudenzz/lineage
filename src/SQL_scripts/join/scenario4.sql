-- Section 1: Create View with a Left Outer Join
-- Tests: Lineage through optional relationships where some records have no supplier.
CREATE OR ALTER VIEW vw_ProductSupplierMatch AS
SELECT 
    p.ProductID, 
    p.ProductName, 
    s.SupplierID,
    s.CompanyName AS SupplierName
FROM Products p
LEFT OUTER JOIN Suppliers s ON p.SupplierID = s.SupplierID;
GO

-- Log Row-Level Lineage for Left Join
DECLARE @pid INT, @sid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID, SupplierID FROM vw_ProductSupplierMatch;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid, @sid;
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Product is always a source
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ProductSupplierMatch', 'ProductID', CAST(@pid AS VARCHAR));
    
    -- Supplier is a source only if it exists (Left Join logic)
    IF @sid IS NOT NULL
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', CAST(@sid AS VARCHAR), 'vw_ProductSupplierMatch', 'ProductID', CAST(@pid AS VARCHAR));
    END;

    FETCH NEXT FROM ViewCursor INTO @pid, @sid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: View -> Table_SupplyChainStaging
IF OBJECT_ID('Table_SupplyChainStaging', 'U') IS NOT NULL DROP TABLE Table_SupplyChainStaging;
CREATE TABLE Table_SupplyChainStaging (
    SCID INT, 
    ProdID INT, 
    SourcingStatus VARCHAR(50)
);

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_sname NVARCHAR(40), @nextSCID INT;
DECLARE TableCursor CURSOR FOR SELECT ProductID, ProductName, SupplierName FROM vw_ProductSupplierMatch;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_sname;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextSCID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_SupplyChainStaging (SCID, ProdID, SourcingStatus)
    VALUES (@nextSCID, @v_pid, 
            CASE WHEN @v_sname IS NULL THEN 'ORPHAN: ' + @v_pname ELSE 'Verified: ' + @v_sname END);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ProductSupplierMatch', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_SupplyChainStaging', 'SCID', CAST(@nextSCID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_sname;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Procedures for Integrity Reporting
CREATE OR ALTER PROCEDURE proc_FinalizeIntegrityReport AS
BEGIN
    IF OBJECT_ID('Final_SupplyChainAudit', 'U') IS NOT NULL DROP TABLE Final_SupplyChainAudit;
    CREATE TABLE Final_SupplyChainAudit (AuditID INT, ProductID INT, AlertStatus VARCHAR(50));

    DECLARE @t_id INT, @t_pid INT, @t_status VARCHAR(50), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ProdID, SourcingStatus FROM ##TempIntegrityBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pid, @t_status;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_SupplyChainAudit (AuditID, ProductID, AlertStatus)
        VALUES (@finalID, @t_pid, @t_status);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempIntegrityBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_SupplyChainAudit', 'AuditID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pid, @t_status;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessOrphanedProducts AS
BEGIN
    IF OBJECT_ID('tempdb..##TempIntegrityBuffer') IS NOT NULL DROP TABLE ##TempIntegrityBuffer;
    CREATE TABLE ##TempIntegrityBuffer (TempID INT, ProdID INT, SourcingStatus VARCHAR(50));

    DECLARE @sid INT, @pid INT, @status VARCHAR(50), @newTempID INT;
    -- Filter specifically for products that failed the join (Orphans)
    DECLARE ProcCursor CURSOR FOR SELECT SCID, ProdID, SourcingStatus FROM Table_SupplyChainStaging WHERE SourcingStatus LIKE 'ORPHAN%';

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @sid, @pid, @status;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempIntegrityBuffer VALUES (@newTempID, @pid, @status);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_SupplyChainStaging', 'SCID', CAST(@sid AS VARCHAR), '##TempIntegrityBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @sid, @pid, @status;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeIntegrityReport;
END;
GO

EXEC proc_ProcessOrphanedProducts;