-- Section 1: Create a View based on a simple SELECT
-- Tests: Column subset selection lineage
CREATE OR ALTER VIEW vw_SupplierContactDetails AS
SELECT 
    SupplierID, 
    CompanyName, 
    ContactName, 
    Country
FROM Suppliers;
GO

-- Log Row-Level Lineage for View
DECLARE @sid INT;
DECLARE ViewCursor CURSOR FOR SELECT SupplierID FROM vw_SupplierContactDetails;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @sid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@sid AS VARCHAR), 'vw_SupplierContactDetails', 'SupplierID', CAST(@sid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @sid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table with a geographic filter
-- Tests: Lineage through exclusion (Only Country <> 'USA')
IF OBJECT_ID('Table_InternationalSuppliers', 'U') IS NOT NULL DROP TABLE Table_InternationalSuppliers;
CREATE TABLE Table_InternationalSuppliers (
    IntlID INT, 
    OriginalSupplierID INT, 
    OrgName NVARCHAR(40)
);

DECLARE @v_sid INT, @v_name NVARCHAR(40), @nextIntlID INT;
DECLARE TableCursor CURSOR FOR SELECT SupplierID, CompanyName FROM vw_SupplierContactDetails WHERE Country <> 'USA';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_sid, @v_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextIntlID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_InternationalSuppliers (IntlID, OriginalSupplierID, OrgName)
    VALUES (@nextIntlID, @v_sid, @v_name);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_SupplierContactDetails', 'SupplierID', CAST(@v_sid AS VARCHAR), 'Table_InternationalSuppliers', 'IntlID', CAST(@nextIntlID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_sid, @v_name;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeSupplierReport AS
BEGIN
    IF OBJECT_ID('Final_ExternalSupplierList', 'U') IS NOT NULL DROP TABLE Final_ExternalSupplierList;
    CREATE TABLE Final_ExternalSupplierList (ReportID INT, SupplierName NVARCHAR(40));

    DECLARE @t_id INT, @t_name NVARCHAR(40), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, OrgName FROM ##TempSupplierBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ExternalSupplierList (ReportID, SupplierName)
        VALUES (@finalID, @t_name);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempSupplierBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ExternalSupplierList', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessInternationalSuppliers AS
BEGIN
    IF OBJECT_ID('tempdb..##TempSupplierBuffer') IS NOT NULL DROP TABLE ##TempSupplierBuffer;
    CREATE TABLE ##TempSupplierBuffer (TempID INT, OrgName NVARCHAR(40));

    DECLARE @iid INT, @name NVARCHAR(40), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT IntlID, OrgName FROM Table_InternationalSuppliers;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @iid, @name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempSupplierBuffer VALUES (@newTempID, @name);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_InternationalSuppliers', 'IntlID', CAST(@iid AS VARCHAR), '##TempSupplierBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @iid, @name;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeSupplierReport;
END;
GO

EXEC proc_ProcessInternationalSuppliers;