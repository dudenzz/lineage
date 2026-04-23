-- Section 1: Create a View based on a simple SELECT
-- Scenario: Mirroring Shipper data for an external logistics partner registry.
-- Rule: Direct data copying only, no transformations.
CREATE OR ALTER VIEW vw_ShipperMirror AS
SELECT 
    ShipperID, 
    CompanyName, 
    Phone
FROM Shippers;
GO

-- Log Row-Level Lineage for View
DECLARE @shid INT;
DECLARE ViewCursor CURSOR FOR SELECT ShipperID FROM vw_ShipperMirror;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @shid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@shid AS VARCHAR), 'vw_ShipperMirror', 'ShipperID', CAST(@shid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @shid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
-- Tests: Lineage preservation during a filter (Specific Shippers only).
IF OBJECT_ID('Table_Active_Shippers', 'U') IS NOT NULL DROP TABLE Table_Active_Shippers;
CREATE TABLE Table_Active_Shippers (
    LocalShipperID INT, 
    OriginalShipperID INT, 
    CompanyName NVARCHAR(40),
    ContactPhone NVARCHAR(24)
);
GO

DECLARE @v_shid INT, @v_name NVARCHAR(40), @v_phone NVARCHAR(24), @nextLocalID INT;
-- Filter: Only process a subset of shippers (e.g., exclude 'Speedy Express' which is ID 1)
DECLARE TableCursor CURSOR FOR 
    SELECT ShipperID, CompanyName, Phone 
    FROM vw_ShipperMirror 
    WHERE ShipperID > 1;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_shid, @v_name, @v_phone;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLocalID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Active_Shippers (LocalShipperID, OriginalShipperID, CompanyName, ContactPhone)
    VALUES (@nextLocalID, @v_shid, @v_name, @v_phone);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ShipperMirror', 'ShipperID', CAST(@v_shid AS VARCHAR), 'Table_Active_Shippers', 'LocalShipperID', CAST(@nextLocalID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_shid, @v_name, @v_phone;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeShipperReport AS
BEGIN
    IF OBJECT_ID('Final_ShipperRegistryReport', 'U') IS NOT NULL DROP TABLE Final_ShipperRegistryReport;
    CREATE TABLE Final_ShipperRegistryReport (ReportID INT, ShipperName NVARCHAR(40), RegistryStatus VARCHAR(20));

    DECLARE @t_id INT, @t_name NVARCHAR(40), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CompanyName FROM ##TempShipperBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ShipperRegistryReport (ReportID, ShipperName, RegistryStatus)
        VALUES (@finalID, @t_name, 'Active Partner');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempShipperBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ShipperRegistryReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessShipperStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempShipperBuffer') IS NOT NULL DROP TABLE ##TempShipperBuffer;
    CREATE TABLE ##TempShipperBuffer (TempID INT, CompanyName NVARCHAR(40));

    DECLARE @tid INT, @name NVARCHAR(40), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT LocalShipperID, CompanyName FROM Table_Active_Shippers;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempShipperBuffer VALUES (@newTempID, @name);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Active_Shippers', 'LocalShipperID', CAST(@tid AS VARCHAR), '##TempShipperBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @name;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeShipperReport;
END;
GO

EXEC proc_ProcessShipperStaging;