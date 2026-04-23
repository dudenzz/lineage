-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring US-based Employee data for a regional sales team directory.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_USEmployees AS
SELECT 
    EmployeeID, 
    FirstName, 
    LastName, 
    Title,
    Country
FROM Employees
WHERE Country = 'USA'; -- Selection applied here
GO

-- Log Row-Level Lineage for View
DECLARE @eid INT;
DECLARE ViewCursor CURSOR FOR SELECT EmployeeID FROM vw_USEmployees;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @eid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@eid AS VARCHAR), 'vw_USEmployees', 'EmployeeID', CAST(@eid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @eid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to Sales Representatives only).
IF OBJECT_ID('Table_US_SalesReps', 'U') IS NOT NULL DROP TABLE Table_US_SalesReps;
CREATE TABLE Table_US_SalesReps (
    DirectoryID INT, 
    OriginalEmployeeID INT, 
    FirstName NVARCHAR(10),
    LastName NVARCHAR(20)
);
GO

DECLARE @v_eid INT, @v_fname NVARCHAR(10), @v_lname NVARCHAR(20), @nextDirectoryID INT;
-- Filter: Only process employees who hold the title of 'Sales Representative'
DECLARE TableCursor CURSOR FOR 
    SELECT EmployeeID, FirstName, LastName 
    FROM vw_USEmployees 
    WHERE Title = 'Sales Representative';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_eid, @v_fname, @v_lname;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextDirectoryID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_US_SalesReps (DirectoryID, OriginalEmployeeID, FirstName, LastName)
    VALUES (@nextDirectoryID, @v_eid, @v_fname, @v_lname);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_USEmployees', 'EmployeeID', CAST(@v_eid AS VARCHAR), 'Table_US_SalesReps', 'DirectoryID', CAST(@nextDirectoryID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_eid, @v_fname, @v_lname;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeDirectoryReport AS
BEGIN
    IF OBJECT_ID('Final_SalesDirectory', 'U') IS NOT NULL DROP TABLE Final_SalesDirectory;
    CREATE TABLE Final_SalesDirectory (
        ReportID INT, 
        FullName NVARCHAR(35), 
        DirectoryStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_fname NVARCHAR(10), @t_lname NVARCHAR(20), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, FirstName, LastName FROM ##TempEmployeeBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_fname, @t_lname;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_SalesDirectory (ReportID, FullName, DirectoryStatus)
        VALUES (@finalID, @t_fname + ' ' + @t_lname, 'Published');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempEmployeeBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_SalesDirectory', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_fname, @t_lname;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessEmployeeStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempEmployeeBuffer') IS NOT NULL DROP TABLE ##TempEmployeeBuffer;
    CREATE TABLE ##TempEmployeeBuffer (
        TempID INT, 
        FirstName NVARCHAR(10),
        LastName NVARCHAR(20)
    );

    DECLARE @tid INT, @fname NVARCHAR(10), @lname NVARCHAR(20), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT DirectoryID, FirstName, LastName FROM Table_US_SalesReps;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @fname, @lname;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempEmployeeBuffer VALUES (@newTempID, @fname, @lname);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_US_SalesReps', 'DirectoryID', CAST(@tid AS VARCHAR), '##TempEmployeeBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @fname, @lname;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeDirectoryReport;
END;
GO

EXEC proc_ProcessEmployeeStaging;