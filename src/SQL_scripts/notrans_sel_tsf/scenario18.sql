-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring Customer decision-makers for a targeted B2B outreach campaign.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_DecisionMakers AS
SELECT 
    CustomerID, 
    CompanyName, 
    ContactName,
    ContactTitle,
    Country
FROM Customers
WHERE ContactTitle = 'Owner'; -- Selection applied here (Role filter)
GO

-- Log Row-Level Lineage for View
DECLARE @cid NCHAR(5);
DECLARE ViewCursor CURSOR FOR SELECT CustomerID FROM vw_DecisionMakers;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @cid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', @cid, 'vw_DecisionMakers', 'CustomerID', @cid);
    FETCH NEXT FROM ViewCursor INTO @cid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to the USA only).
IF OBJECT_ID('Table_USA_Owners', 'U') IS NOT NULL DROP TABLE Table_USA_Owners;
CREATE TABLE Table_USA_Owners (
    TargetID INT, 
    OriginalCustomerID NCHAR(5), 
    CompanyName NVARCHAR(40),
    ContactName NVARCHAR(30)
);
GO

DECLARE @v_cid NCHAR(5), @v_cname NVARCHAR(40), @v_contact NVARCHAR(30), @nextTargetID INT;
-- Filter: Only process decision-makers located in the USA
DECLARE TableCursor CURSOR FOR 
    SELECT CustomerID, CompanyName, ContactName 
    FROM vw_DecisionMakers 
    WHERE Country = 'USA';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_cid, @v_cname, @v_contact;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextTargetID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_USA_Owners (TargetID, OriginalCustomerID, CompanyName, ContactName)
    VALUES (@nextTargetID, @v_cid, @v_cname, @v_contact);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_DecisionMakers', 'CustomerID', @v_cid, 'Table_USA_Owners', 'TargetID', CAST(@nextTargetID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_cid, @v_cname, @v_contact;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeB2BCampaign AS
BEGIN
    IF OBJECT_ID('Final_B2BCampaignRegistry', 'U') IS NOT NULL DROP TABLE Final_B2BCampaignRegistry;
    CREATE TABLE Final_B2BCampaignRegistry (
        ReportID INT, 
        TargetCompany NVARCHAR(40), 
        KeyContact NVARCHAR(30),
        CampaignStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cname NVARCHAR(40), @t_contact NVARCHAR(30), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CompanyName, ContactName FROM ##TempOwnerBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cname, @t_contact;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_B2BCampaignRegistry (ReportID, TargetCompany, KeyContact, CampaignStatus)
        VALUES (@finalID, @t_cname, @t_contact, 'Direct Mail Queued');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempOwnerBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_B2BCampaignRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cname, @t_contact;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessOwnerStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempOwnerBuffer') IS NOT NULL DROP TABLE ##TempOwnerBuffer;
    CREATE TABLE ##TempOwnerBuffer (
        TempID INT, 
        CompanyName NVARCHAR(40),
        ContactName NVARCHAR(30)
    );

    DECLARE @tid INT, @cname NVARCHAR(40), @contact NVARCHAR(30), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT TargetID, CompanyName, ContactName FROM Table_USA_Owners;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cname, @contact;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempOwnerBuffer VALUES (@newTempID, @cname, @contact);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_USA_Owners', 'TargetID', CAST(@tid AS VARCHAR), '##TempOwnerBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cname, @contact;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeB2BCampaign;
END;
GO

EXEC proc_ProcessOwnerStaging;