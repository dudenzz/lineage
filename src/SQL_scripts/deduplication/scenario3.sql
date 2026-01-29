-- Section 1: Create a Unified View with Implicit Deduplication
-- Tests: Lineage through UNION (DISTINCT) across two different source tables.
-- The tool must track that a single row in the view could originate from either or both sources.
CREATE OR ALTER VIEW vw_UnifiedContactList AS
SELECT CompanyName, Phone, 'Customer' AS OriginalSource
FROM Customers
UNION
SELECT CompanyName, Phone, 'Shipper' AS OriginalSource
FROM Shippers;
GO

-- Log Lineage:
-- Tool must recognize both 'Customers' and 'Shippers' as sources.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Customers', 'CompanyName', CompanyName, 'vw_UnifiedContactList', 'CompanyName', CompanyName
FROM Customers;

INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Shippers', 'CompanyName', CompanyName, 'vw_UnifiedContactList', 'CompanyName', CompanyName
FROM Shippers;
GO

-- Section 2: Persistence to a Unique Contacts Table
-- Tests: Tracking 'Used for Creation' through a SET operation into a physical table.
IF OBJECT_ID('Table_Unique_Contacts', 'U') IS NOT NULL DROP TABLE Table_Unique_Contacts;
CREATE TABLE Table_Unique_Contacts (
    ContactID INT PRIMARY KEY,
    DisplayName NVARCHAR(40),
    ContactPhone NVARCHAR(24)
);

DECLARE @u_name NVARCHAR(40), @u_phone NVARCHAR(24), @nextContactID INT;
DECLARE ContactCursor CURSOR FOR SELECT CompanyName, Phone FROM vw_UnifiedContactList;

OPEN ContactCursor;
FETCH NEXT FROM ContactCursor INTO @u_name, @u_phone;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextContactID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Unique_Contacts (ContactID, DisplayName, ContactPhone)
    VALUES (@nextContactID, @u_name, @u_phone);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_UnifiedContactList', 'CompanyName', @u_name, 'Table_Unique_Contacts', 'ContactID', CAST(@nextContactID AS VARCHAR));

    FETCH NEXT FROM ContactCursor INTO @u_name, @u_phone;
END;
CLOSE ContactCursor; DEALLOCATE ContactCursor;
GO

-- Section 3: Stored Procedure for Directory Finalization
-- Tests: Ability to resolve the origin of a record when it could have come from multiple parents.
CREATE OR ALTER PROCEDURE proc_FinalizeGlobalDirectory AS
BEGIN
    IF OBJECT_ID('Final_Global_Directory', 'U') IS NOT NULL DROP TABLE Final_Global_Directory;
    CREATE TABLE Final_Global_Directory (DirID INT, EntryName NVARCHAR(40));

    INSERT INTO Final_Global_Directory (DirID, EntryName)
    SELECT 
        ContactID,
        UPPER(DisplayName)
    FROM Table_Unique_Contacts;

    -- Log Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Unique_Contacts', 'ContactID', CAST(ContactID AS VARCHAR), 'Final_Global_Directory', 'DirID', 'Standardized'
    FROM Table_Unique_Contacts;
END;
GO

EXEC proc_FinalizeGlobalDirectory;