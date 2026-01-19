-- Section 1: Create View with a Self-Join
-- Tests: Can the tool identify a single table acting as two distinct sources for one row?
CREATE OR ALTER VIEW vw_EmployeeHierarchy AS
SELECT 
    e.EmployeeID AS SubordinateID, 
    e.LastName AS SubordinateName, 
    m.EmployeeID AS ManagerID, 
    m.LastName AS ManagerName
FROM Employees e
LEFT OUTER JOIN Employees m ON e.ReportsTo = m.EmployeeID;
GO

-- Log Row-Level Lineage for Self-Join
DECLARE @eid INT, @mid INT;
DECLARE ViewCursor CURSOR FOR SELECT SubordinateID, ManagerID FROM vw_EmployeeHierarchy;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @eid, @mid;
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Role 1: The Subordinate
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@eid AS VARCHAR), 'vw_EmployeeHierarchy', 'SubordinateID', CAST(@eid AS VARCHAR));
    
    -- Role 2: The Manager (if exists)
    IF @mid IS NOT NULL
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', CAST(@mid AS VARCHAR), 'vw_EmployeeHierarchy', 'SubordinateID', CAST(@eid AS VARCHAR));
    END;

    FETCH NEXT FROM ViewCursor INTO @eid, @mid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: View -> Table_SupervisionStaging
IF OBJECT_ID('Table_SupervisionStaging', 'U') IS NOT NULL DROP TABLE Table_SupervisionStaging;
CREATE TABLE Table_SupervisionStaging (
    RecordID INT, 
    EmpID INT, 
    SupervisorName NVARCHAR(20)
);

DECLARE @v_eid INT, @v_mname NVARCHAR(20), @nextRecID INT;
DECLARE TableCursor CURSOR FOR SELECT SubordinateID, ManagerName FROM vw_EmployeeHierarchy;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_eid, @v_mname;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextRecID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_SupervisionStaging (RecordID, EmpID, SupervisorName)
    VALUES (@nextRecID, @v_eid, ISNULL(@v_mname, 'TOP LEVEL'));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_EmployeeHierarchy', 'SubordinateID', CAST(@v_eid AS VARCHAR), 'Table_SupervisionStaging', 'RecordID', CAST(@nextRecID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_eid, @v_mname;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Procedures for Management Audit
CREATE OR ALTER PROCEDURE proc_FinalizeSupervisionReport AS
BEGIN
    IF OBJECT_ID('Final_SupervisionAudit', 'U') IS NOT NULL DROP TABLE Final_SupervisionAudit;
    CREATE TABLE Final_SupervisionAudit (AuditID INT, StaffID INT, ReportingLine NVARCHAR(50));

    DECLARE @t_id INT, @t_eid INT, @t_mname NVARCHAR(20), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, EmpID, SupervisorName FROM ##TempSupervisionBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_eid, @t_mname;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_SupervisionAudit (AuditID, StaffID, ReportingLine)
        VALUES (@finalID, @t_eid, 'Managed By: ' + @t_mname);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempSupervisionBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_SupervisionAudit', 'AuditID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_eid, @t_mname;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessHierarchyStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempSupervisionBuffer') IS NOT NULL DROP TABLE ##TempSupervisionBuffer;
    CREATE TABLE ##TempSupervisionBuffer (TempID INT, EmpID INT, SupervisorName NVARCHAR(20));

    DECLARE @rid INT, @eid INT, @mname NVARCHAR(20), @newTempID INT;
    -- Filter out top-level management to focus on mid-level supervision
    DECLARE ProcCursor CURSOR FOR SELECT RecordID, EmpID, SupervisorName FROM Table_SupervisionStaging WHERE SupervisorName <> 'TOP LEVEL';

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @rid, @eid, @mname;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempSupervisionBuffer VALUES (@newTempID, @eid, @mname);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_SupervisionStaging', 'RecordID', CAST(@rid AS VARCHAR), '##TempSupervisionBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @rid, @eid, @mname;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeSupervisionReport;
END;
GO

EXEC proc_ProcessHierarchyStaging;