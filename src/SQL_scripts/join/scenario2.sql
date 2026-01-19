-- Section 1: Create View with an Inner Join
-- Tests: Mapping a single record in the view to two different physical source tables.
CREATE OR ALTER VIEW vw_OrderEmployeeContext AS
SELECT 
    o.OrderID, 
    e.EmployeeID, 
    e.LastName, 
    o.OrderDate, 
    o.ShipCountry
FROM Orders o
INNER JOIN Employees e ON o.EmployeeID = e.EmployeeID;
GO

-- Log Row-Level Lineage for Join
DECLARE @oid INT, @eid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID, EmployeeID FROM vw_OrderEmployeeContext;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid, @eid;
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Record from Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_OrderEmployeeContext', 'OrderID', CAST(@oid AS VARCHAR));
    
    -- Record from Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@eid AS VARCHAR), 'vw_OrderEmployeeContext', 'OrderID', CAST(@oid AS VARCHAR));

    FETCH NEXT FROM ViewCursor INTO @oid, @eid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: View -> Table_RegionalAssignments
-- Tests: Carrying forward join-derived data into a physical table.
IF OBJECT_ID('Table_RegionalAssignments', 'U') IS NOT NULL DROP TABLE Table_RegionalAssignments;
CREATE TABLE Table_RegionalAssignments (
    AssignmentID INT, 
    SourceOrderID INT, 
    EmpName NVARCHAR(20), 
    Country NVARCHAR(15)
);

DECLARE @v_oid INT, @v_name NVARCHAR(20), @v_country NVARCHAR(15), @nextAssignID INT;
DECLARE TableCursor CURSOR FOR SELECT OrderID, LastName, ShipCountry FROM vw_OrderEmployeeContext;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_name, @v_country;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAssignID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_RegionalAssignments (AssignmentID, SourceOrderID, EmpName, Country)
    VALUES (@nextAssignID, @v_oid, @v_name, @v_country);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_OrderEmployeeContext', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_RegionalAssignments', 'AssignmentID', CAST(@nextAssignID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_name, @v_country;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Procedures for Management Review
CREATE OR ALTER PROCEDURE proc_FinalizeManagementReview AS
BEGIN
    IF OBJECT_ID('Final_ManagerialSalesAudit', 'U') IS NOT NULL DROP TABLE Final_ManagerialSalesAudit;
    CREATE TABLE Final_ManagerialSalesAudit (AuditID INT, SalesRep NVARCHAR(20), Location NVARCHAR(15));

    DECLARE @t_id INT, @t_name NVARCHAR(20), @t_loc NVARCHAR(15), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, EmpName, Country FROM ##TempJoinBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_name, @t_loc;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ManagerialSalesAudit (AuditID, SalesRep, Location)
        VALUES (@finalID, @t_name, @t_loc);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempJoinBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ManagerialSalesAudit', 'AuditID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_name, @t_loc;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StagingRegionalJoin AS
BEGIN
    IF OBJECT_ID('tempdb..##TempJoinBuffer') IS NOT NULL DROP TABLE ##TempJoinBuffer;
    CREATE TABLE ##TempJoinBuffer (TempID INT, EmpName NVARCHAR(20), Country NVARCHAR(15));

    DECLARE @aid INT, @name NVARCHAR(20), @country NVARCHAR(15), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT AssignmentID, EmpName, Country FROM Table_RegionalAssignments WHERE Country = 'UK';

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @aid, @name, @country;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempJoinBuffer VALUES (@newTempID, @name, @country);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_RegionalAssignments', 'AssignmentID', CAST(@aid AS VARCHAR), '##TempJoinBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @aid, @name, @country;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeManagementReview;
END;
GO

EXEC proc_StagingRegionalJoin;