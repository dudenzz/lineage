-- Section 1: Create a View based on a simple SELECT with Selection (Row Filtering)
-- Scenario: Mirroring veteran Employees for a corporate milestone recognition program.
-- Rule: Direct data copying with selection (WHERE clause), no transformations.
CREATE OR ALTER VIEW vw_VeteranEmployees AS
SELECT 
    EmployeeID, 
    FirstName, 
    LastName, 
    HireDate,
    City
FROM Employees
WHERE HireDate < '1993-01-01'; -- Selection applied here (Tenure threshold)
GO

-- Log Row-Level Lineage for View
DECLARE @eid INT;
DECLARE ViewCursor CURSOR FOR SELECT EmployeeID FROM vw_VeteranEmployees;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @eid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@eid AS VARCHAR), 'vw_VeteranEmployees', 'EmployeeID', CAST(@eid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @eid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during sequential selection (Filtering down to the London office only).
IF OBJECT_ID('Table_London_Veterans', 'U') IS NOT NULL DROP TABLE Table_London_Veterans;
CREATE TABLE Table_London_Veterans (
    AwardID INT, 
    OriginalEmployeeID INT, 
    FirstName NVARCHAR(10),
    LastName NVARCHAR(20),
    HireDate DATETIME
);
GO

DECLARE @v_eid INT, @v_fname NVARCHAR(10), @v_lname NVARCHAR(20), @v_hdate DATETIME, @nextAwardID INT;
-- Filter: Only process recognition for veteran employees based in London
DECLARE TableCursor CURSOR FOR 
    SELECT EmployeeID, FirstName, LastName, HireDate 
    FROM vw_VeteranEmployees 
    WHERE City = 'London';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_eid, @v_fname, @v_lname, @v_hdate;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAwardID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_London_Veterans (AwardID, OriginalEmployeeID, FirstName, LastName, HireDate)
    VALUES (@nextAwardID, @v_eid, @v_fname, @v_lname, @v_hdate);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_VeteranEmployees', 'EmployeeID', CAST(@v_eid AS VARCHAR), 'Table_London_Veterans', 'AwardID', CAST(@nextAwardID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_eid, @v_fname, @v_lname, @v_hdate;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeRecognitionReport AS
BEGIN
    IF OBJECT_ID('Final_MilestoneAwards', 'U') IS NOT NULL DROP TABLE Final_MilestoneAwards;
    CREATE TABLE Final_MilestoneAwards (
        ReportID INT, 
        FirstName NVARCHAR(10), 
        LastName NVARCHAR(20),
        OnboardingDate DATETIME,
        AwardStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_fname NVARCHAR(10), @t_lname NVARCHAR(20), @t_hdate DATETIME, @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, FirstName, LastName, HireDate FROM ##TempVeteranBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_fname, @t_lname, @t_hdate;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_MilestoneAwards (ReportID, FirstName, LastName, OnboardingDate, AwardStatus)
        VALUES (@finalID, @t_fname, @t_lname, @t_hdate, 'Plaque Ordered');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempVeteranBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_MilestoneAwards', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_fname, @t_lname, @t_hdate;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessVeteranStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempVeteranBuffer') IS NOT NULL DROP TABLE ##TempVeteranBuffer;
    CREATE TABLE ##TempVeteranBuffer (
        TempID INT, 
        FirstName NVARCHAR(10),
        LastName NVARCHAR(20),
        HireDate DATETIME
    );

    DECLARE @tid INT, @fname NVARCHAR(10), @lname NVARCHAR(20), @hdate DATETIME, @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT AwardID, FirstName, LastName, HireDate FROM Table_London_Veterans;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @fname, @lname, @hdate;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempVeteranBuffer VALUES (@newTempID, @fname, @lname, @hdate);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_London_Veterans', 'AwardID', CAST(@tid AS VARCHAR), '##TempVeteranBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @fname, @lname, @hdate;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeRecognitionReport;
END;
GO

EXEC proc_ProcessVeteranStaging;