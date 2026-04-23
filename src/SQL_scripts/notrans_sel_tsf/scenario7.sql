-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring North American Supplier data for a regional vendor compliance audit.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_NASuppliers AS
SELECT 
    SupplierID, 
    CompanyName, 
    ContactName,
    Country
FROM Suppliers
WHERE Country IN ('USA', 'Canada'); -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @sid INT;
DECLARE ViewCursor CURSOR FOR SELECT SupplierID FROM vw_NASuppliers;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @sid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@sid AS VARCHAR), 'vw_NASuppliers', 'SupplierID', CAST(@sid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @sid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Canada only).
IF OBJECT_ID('Table_Canadian_Suppliers', 'U') IS NOT NULL DROP TABLE Table_Canadian_Suppliers;
CREATE TABLE Table_Canadian_Suppliers (
    LocalSupplierID INT, 
    OriginalSupplierID INT, 
    CompanyName NVARCHAR(40),
    ContactName NVARCHAR(30)
);
GO

DECLARE @v_sid INT, @v_cname NVARCHAR(40), @v_contact NVARCHAR(30), @nextLocalID INT;
-- Filter: Only process suppliers located specifically in Canada
DECLARE TableCursor CURSOR FOR 
    SELECT SupplierID, CompanyName, ContactName 
    FROM vw_NASuppliers 
    WHERE Country = 'Canada';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_sid, @v_cname, @v_contact;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLocalID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Canadian_Suppliers (LocalSupplierID, OriginalSupplierID, CompanyName, ContactName)
    VALUES (@nextLocalID, @v_sid, @v_cname, @v_contact);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_NASuppliers', 'SupplierID', CAST(@v_sid AS VARCHAR), 'Table_Canadian_Suppliers', 'LocalSupplierID', CAST(@nextLocalID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_sid, @v_cname, @v_contact;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeSupplierReport AS
BEGIN
    IF OBJECT_ID('Final_CanadianSupplierRegistry', 'U') IS NOT NULL DROP TABLE Final_CanadianSupplierRegistry;
    CREATE TABLE Final_CanadianSupplierRegistry (
        ReportID INT, 
        VendorName NVARCHAR(40), 
        PointOfContact NVARCHAR(30),
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cname NVARCHAR(40), @t_contact NVARCHAR(30), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CompanyName, ContactName FROM ##TempSupplierBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cname, @t_contact;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_CanadianSupplierRegistry (ReportID, VendorName, PointOfContact, AuditStatus)
        VALUES (@finalID, @t_cname, @t_contact, 'Pending Audit');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempSupplierBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_CanadianSupplierRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cname, @t_contact;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessSupplierStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempSupplierBuffer') IS NOT NULL DROP TABLE ##TempSupplierBuffer;
    CREATE TABLE ##TempSupplierBuffer (
        TempID INT, 
        CompanyName NVARCHAR(40),
        ContactName NVARCHAR(30)
    );

    DECLARE @tid INT, @cname NVARCHAR(40), @contact NVARCHAR(30), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT LocalSupplierID, CompanyName, ContactName FROM Table_Canadian_Suppliers;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cname, @contact;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempSupplierBuffer VALUES (@newTempID, @cname, @contact);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Canadian_Suppliers', 'LocalSupplierID', CAST(@tid AS VARCHAR), '##TempSupplierBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cname, @contact;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeSupplierReport;
END;
GO

EXEC proc_ProcessSupplierStaging;