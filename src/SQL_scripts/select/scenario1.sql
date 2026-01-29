-- Section 1: Create a View based on a simple SELECT
-- Tests: Basic view-to-table lineage without any joins or math.
CREATE OR ALTER VIEW vw_EmployeeBasicInfo AS
SELECT 
    EmployeeID, 
    LastName, 
    FirstName, 
    City, 
    Region
FROM Employees;
GO

-- Log Row-Level Lineage for View
DECLARE @eid INT;
DECLARE ViewCursor CURSOR FOR SELECT EmployeeID FROM vw_EmployeeBasicInfo;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @eid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@eid AS VARCHAR), 'vw_EmployeeBasicInfo', 'EmployeeID', CAST(@eid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @eid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
-- Tests: Tool's ability to track lineage when records are filtered out.
IF OBJECT_ID('Table_US_Employees', 'U') IS NOT NULL DROP TABLE Table_US_Employees;
CREATE TABLE Table_US_Employees (
    USEmpID INT, 
    OriginalID INT, 
    FullName NVARCHAR(100)
);

DECLARE @v_eid INT, @v_fname NVARCHAR(10), @v_lname NVARCHAR(20), @nextUSID INT;
DECLARE TableCursor CURSOR FOR SELECT EmployeeID, FirstName, LastName FROM vw_EmployeeBasicInfo WHERE Region = 'WA';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_eid, @v_fname, @v_lname;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextUSID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_US_Employees (USEmpID, OriginalID, FullName)
    VALUES (@nextUSID, @v_eid, @v_fname + ' ' + @v_lname);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_EmployeeBasicInfo', 'EmployeeID', CAST(@v_eid AS VARCHAR), 'Table_US_Employees', 'USEmpID', CAST(@nextUSID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_eid, @v_fname, @v_lname;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeActiveStaffReport AS
BEGIN
    IF OBJECT_ID('Final_ActiveStaffReport', 'U') IS NOT NULL DROP TABLE Final_ActiveStaffReport;
    CREATE TABLE Final_ActiveStaffReport (ReportID INT, EmployeeName NVARCHAR(100), Status VARCHAR(10));

    DECLARE @t_id INT, @t_name NVARCHAR(100), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, FullName FROM ##TempStaffBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ActiveStaffReport (ReportID, EmployeeName, Status)
        VALUES (@finalID, @t_name, 'Active');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempStaffBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ActiveStaffReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_name;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessStaffStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempStaffBuffer') IS NOT NULL DROP TABLE ##TempStaffBuffer;
    CREATE TABLE ##TempStaffBuffer (TempID INT, FullName NVARCHAR(100));

    DECLARE @tid INT, @name NVARCHAR(100), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT USEmpID, FullName FROM Table_US_Employees;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempStaffBuffer VALUES (@newTempID, @name);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_US_Employees', 'USEmpID', CAST(@tid AS VARCHAR), '##TempStaffBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @name;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeActiveStaffReport;
END;
GO

EXEC proc_ProcessStaffStaging;