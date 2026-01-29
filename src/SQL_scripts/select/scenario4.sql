-- Section 1: Create a View based on a simple SELECT
-- Tests: Simple column selection from a single source
CREATE OR ALTER VIEW vw_CustomerBase AS
SELECT 
    CustomerID, 
    CompanyName, 
    ContactName, 
    ContactTitle
FROM Customers;
GO

-- Log Row-Level Lineage for View
-- We use CustomerID as the PK for both source and target here.
DECLARE @cid NVARCHAR(5);
DECLARE ViewCursor CURSOR FOR SELECT CustomerID FROM vw_CustomerBase;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @cid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', @cid, 'vw_CustomerBase', 'CustomerID', @cid);
    FETCH NEXT FROM ViewCursor INTO @cid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table with a specific filter
-- Tests: Lineage when filtering by string literals ('Owner')
IF OBJECT_ID('Table_ExecutiveAccounts', 'U') IS NOT NULL DROP TABLE Table_ExecutiveAccounts;
CREATE TABLE Table_ExecutiveAccounts (
    ExecID INT, 
    SourceCustomerID NVARCHAR(5), 
    AccountName NVARCHAR(40)
);

DECLARE @v_cid NVARCHAR(5), @v_name NVARCHAR(40), @nextExecID INT;
DECLARE TableCursor CURSOR FOR SELECT CustomerID, CompanyName FROM vw_CustomerBase WHERE ContactTitle = 'Owner';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_cid, @v_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextExecID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_ExecutiveAccounts (ExecID, SourceCustomerID, AccountName)
    VALUES (@nextExecID, @v_cid, @v_name);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_CustomerBase', 'CustomerID', @v_cid, 'Table_ExecutiveAccounts', 'ExecID', CAST(@nextExecID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_cid, @v_name;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures for Final Mailing List
CREATE OR ALTER PROCEDURE proc_FinalizeMailingList AS
BEGIN
    IF OBJECT_ID('Final_OwnerMailingList', 'U') IS NOT NULL DROP TABLE Final_OwnerMailingList;
    CREATE TABLE Final_OwnerMailingList (MailID INT, RecipientCompany NVARCHAR(40));

    DECLARE @t_id INT, @t_comp NVARCHAR(40), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, AccountName FROM ##TempMailingBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_comp;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_OwnerMailingList (MailID, RecipientCompany)
        VALUES (@finalID, @t_comp);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempMailingBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_OwnerMailingList', 'MailID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_comp;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StagingExecutiveMails AS
BEGIN
    IF OBJECT_ID('tempdb..##TempMailingBuffer') IS NOT NULL DROP TABLE ##TempMailingBuffer;
    CREATE TABLE ##TempMailingBuffer (TempID INT, AccountName NVARCHAR(40));

    DECLARE @eid INT, @accName NVARCHAR(40), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT ExecID, AccountName FROM Table_ExecutiveAccounts;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @eid, @accName;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempMailingBuffer VALUES (@newTempID, @accName);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_ExecutiveAccounts', 'ExecID', CAST(@eid AS VARCHAR), '##TempMailingBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @eid, @accName;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeMailingList;
END;
GO

EXEC proc_StagingExecutiveMails;