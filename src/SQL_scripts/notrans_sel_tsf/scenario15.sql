-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring Customer records with missing geographic data for a data quality cleanup queue.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_MissingRegionCustomers AS
SELECT 
    CustomerID, 
    CompanyName, 
    ContactName,
    Country
FROM Customers
WHERE Region IS NULL; -- Selection applied here (Data Quality Flag)
GO

-- Log Row-Level Lineage for View
DECLARE @cid NCHAR(5);
DECLARE ViewCursor CURSOR FOR SELECT CustomerID FROM vw_MissingRegionCustomers;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @cid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', @cid, 'vw_MissingRegionCustomers', 'CustomerID', @cid);
    FETCH NEXT FROM ViewCursor INTO @cid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to the UK only).
IF OBJECT_ID('Table_UK_DataCleanup', 'U') IS NOT NULL DROP TABLE Table_UK_DataCleanup;
CREATE TABLE Table_UK_DataCleanup (
    CleanupID INT, 
    OriginalCustomerID NCHAR(5), 
    CompanyName NVARCHAR(40),
    ContactName NVARCHAR(30)
);
GO

DECLARE @v_cid NCHAR(5), @v_cname NVARCHAR(40), @v_contact NVARCHAR(30), @nextCleanupID INT;
-- Filter: Only process data quality tickets for customers located in the UK
DECLARE TableCursor CURSOR FOR 
    SELECT CustomerID, CompanyName, ContactName 
    FROM vw_MissingRegionCustomers 
    WHERE Country = 'UK';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_cid, @v_cname, @v_contact;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextCleanupID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UK_DataCleanup (CleanupID, OriginalCustomerID, CompanyName, ContactName)
    VALUES (@nextCleanupID, @v_cid, @v_cname, @v_contact);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_MissingRegionCustomers', 'CustomerID', @v_cid, 'Table_UK_DataCleanup', 'CleanupID', CAST(@nextCleanupID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_cid, @v_cname, @v_contact;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeDataQualityReport AS
BEGIN
    IF OBJECT_ID('Final_CustomerDataAudit', 'U') IS NOT NULL DROP TABLE Final_CustomerDataAudit;
    CREATE TABLE Final_CustomerDataAudit (
        ReportID INT, 
        AffectedCompany NVARCHAR(40), 
        PointOfContact NVARCHAR(30),
        ResolutionStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cname NVARCHAR(40), @t_contact NVARCHAR(30), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CompanyName, ContactName FROM ##TempCleanupBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cname, @t_contact;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_CustomerDataAudit (ReportID, AffectedCompany, PointOfContact, ResolutionStatus)
        VALUES (@finalID, @t_cname, @t_contact, 'Pending Outreach');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempCleanupBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_CustomerDataAudit', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cname, @t_contact;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessCleanupStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempCleanupBuffer') IS NOT NULL DROP TABLE ##TempCleanupBuffer;
    CREATE TABLE ##TempCleanupBuffer (
        TempID INT, 
        CompanyName NVARCHAR(40),
        ContactName NVARCHAR(30)
    );

    DECLARE @tid INT, @cname NVARCHAR(40), @contact NVARCHAR(30), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT CleanupID, CompanyName, ContactName FROM Table_UK_DataCleanup;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cname, @contact;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempCleanupBuffer VALUES (@newTempID, @cname, @contact);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_UK_DataCleanup', 'CleanupID', CAST(@tid AS VARCHAR), '##TempCleanupBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cname, @contact;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeDataQualityReport;
END;
GO

EXEC proc_ProcessCleanupStaging;