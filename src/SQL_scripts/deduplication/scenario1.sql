-- Section 1: Identify and Rank Duplicates
-- Tests: Lineage through ROW_NUMBER() and PARTITION BY clauses.
-- The tool must link multiple potential source rows to a single surviving row.
WITH RankedCustomers AS (
    SELECT 
        CustomerID,
        CompanyName,
        ContactName,
        City,
        -- Rank records based on ID (assuming higher ID is more recent/accurate)
        ROW_NUMBER() OVER (
            PARTITION BY CompanyName, City 
            ORDER BY CustomerID DESC
        ) AS DuplicateRank
    FROM Customers
)
-- Move only the 'Golden Records' (Rank 1) into a temp table
SELECT * INTO #GoldenCustomers
FROM RankedCustomers
WHERE DuplicateRank = 1;

-- Log Lineage:
-- Tool must recognize 'Customers' as the source, even though some rows are discarded.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Customers', 'CustomerID', CAST(CustomerID AS VARCHAR), '#GoldenCustomers', 'CustomerID', CAST(CustomerID AS VARCHAR)
FROM #GoldenCustomers;
GO

-- Section 2: Persisting the Deduplicated Dataset
-- Tests: Lineage from a filtered CTE result to a physical "Clean" table.
IF OBJECT_ID('Table_Clean_Customers', 'U') IS NOT NULL DROP TABLE Table_Clean_Customers;
CREATE TABLE Table_Clean_Customers (
    CleanID INT PRIMARY KEY,
    OriginalCustomerID NCHAR(5),
    CustomerName NVARCHAR(40),
    DeDupTimestamp DATETIME
);

DECLARE @c_id NCHAR(5), @c_name NVARCHAR(40), @nextCleanID INT;
DECLARE DeDupCursor CURSOR FOR SELECT CustomerID, CompanyName FROM #GoldenCustomers;

OPEN DeDupCursor;
FETCH NEXT FROM DeDupCursor INTO @c_id, @c_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextCleanID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Clean_Customers (CleanID, OriginalCustomerID, CustomerName, DeDupTimestamp)
    VALUES (@nextCleanID, @c_id, @c_name, GETDATE());

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('#GoldenCustomers', 'CustomerID', CAST(@c_id AS VARCHAR), 'Table_Clean_Customers', 'CleanID', CAST(@nextCleanID AS VARCHAR));

    FETCH NEXT FROM DeDupCursor INTO @c_id, @c_name;
END;
CLOSE DeDupCursor; DEALLOCATE DeDupCursor;
GO

-- Section 3: Final Procedure for Customer Mailing List
-- Tests: Finalizing lineage where the target table represents a "Unique" mailing list.
CREATE OR ALTER PROCEDURE proc_GenerateUniqueMailingList AS
BEGIN
    IF OBJECT_ID('Final_Mailing_List', 'U') IS NOT NULL DROP TABLE Final_Mailing_List;
    CREATE TABLE Final_Mailing_List (MailID INT, UniqueCustName NVARCHAR(40));

    INSERT INTO Final_Mailing_List (MailID, UniqueCustName)
    SELECT 
        NEXT VALUE FOR GlobalIDSequence,
        CustomerName
    FROM Table_Clean_Customers;

    -- Log Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Clean_Customers', 'CleanID', CAST(CleanID AS VARCHAR), 'Final_Mailing_List', 'MailID', 'Verified'
    FROM Table_Clean_Customers;
END;
GO

EXEC proc_GenerateUniqueMailingList;