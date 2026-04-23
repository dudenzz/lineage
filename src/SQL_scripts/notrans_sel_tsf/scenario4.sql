-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring European Customer data for a localized marketing campaign registry.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_EuropeanCustomers AS
SELECT 
    CustomerID, 
    CompanyName, 
    Country
FROM Customers
WHERE Country IN ('Germany', 'France', 'UK', 'Spain', 'Italy'); -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @cid NCHAR(5);
DECLARE ViewCursor CURSOR FOR SELECT CustomerID FROM vw_EuropeanCustomers;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @cid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', @cid, 'vw_EuropeanCustomers', 'CustomerID', @cid);
    FETCH NEXT FROM ViewCursor INTO @cid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Germany only).
IF OBJECT_ID('Table_German_Customers', 'U') IS NOT NULL DROP TABLE Table_German_Customers;
CREATE TABLE Table_German_Customers (
    LocalCustID INT, 
    OriginalCustomerID NCHAR(5), 
    CompanyName NVARCHAR(40)
);
GO

DECLARE @v_cid NCHAR(5), @v_name NVARCHAR(40), @nextLocalID INT;
-- Filter: Only process customers located in Germany
DECLARE TableCursor CURSOR FOR 
    SELECT CustomerID, CompanyName 
    FROM vw_EuropeanCustomers 
    WHERE Country = 'Germany';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_cid, @v_name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLocalID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_German_Customers (LocalCustID, OriginalCustomerID, CompanyName)
    VALUES (@nextLocalID, @v_cid, @v_name);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_EuropeanCustomers', 'CustomerID', @v_cid, 'Table_German_Customers', 'LocalCustID', CAST(@nextLocalID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_cid, @v_name;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeCustomerReport AS
BEGIN
    IF OBJECT_ID('Final_EUMarketingRegistry', 'U') IS NOT NULL DROP TABLE Final_EUMarketingRegistry;
    CREATE TABLE Final_EUMarketingRegistry (
        ReportID INT, 
        TargetCompany NVARCHAR(40), 
        CampaignStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_name NVARCHAR(40), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, CompanyName FROM ##TempCustomerBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_EUMarketingRegistry (ReportID, TargetCompany, CampaignStatus)
        VALUES (@finalID, @t_name, 'Enrolled');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempCustomerBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_EUMarketingRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessCustomerStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempCustomerBuffer') IS NOT NULL DROP TABLE ##TempCustomerBuffer;
    CREATE TABLE ##TempCustomerBuffer (TempID INT, CompanyName NVARCHAR(40));

    DECLARE @tid INT, @name NVARCHAR(40), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT LocalCustID, CompanyName FROM Table_German_Customers;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempCustomerBuffer VALUES (@newTempID, @name);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_German_Customers', 'LocalCustID', CAST(@tid AS VARCHAR), '##TempCustomerBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @name;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeCustomerReport;
END;
GO

EXEC proc_ProcessCustomerStaging;