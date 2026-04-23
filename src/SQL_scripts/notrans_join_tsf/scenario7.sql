-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Employee-Supervisor Reporting & Hierarchical Accountability Ledger.
-- Rule: Combine inputs via SELF JOIN to map organizational structure. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for both the subordinate and the manager.

IF OBJECT_ID('Table_ManagementReportingLedger', 'U') IS NOT NULL DROP TABLE Table_ManagementReportingLedger;
CREATE TABLE Table_ManagementReportingLedger (
    ReportingAuditID INT,
    SubordinateID INT,       -- Native Projection from Employees (as Subordinate)
    ManagerID INT,           -- Native Projection from Employees (as Manager)
    SubordinateName NVARCHAR(20), -- Native Projection (LastName)
    ManagerName NVARCHAR(20),     -- Native Projection (LastName)
    SubordinateTitle NVARCHAR(30), -- Native Projection
    ManagerTitle NVARCHAR(30)      -- Native Projection
);
GO

DECLARE @v_SubordinateID INT,
        @v_ManagerID INT,
        @v_SubName NVARCHAR(20),
        @v_MgrName NVARCHAR(20),
        @v_SubTitle NVARCHAR(30),
        @v_MgrTitle NVARCHAR(30),
        @nextReportingAuditID INT;

-- Cursor using SELF JOIN for strict projection of the management hierarchy.
-- Selection: Only pairs where the manager is a 'Vice President, Sales' or 'Sales Manager'.
-- All attributes are native; no string concatenation for full names is performed.
DECLARE ReportingCursor CURSOR FOR 
    SELECT 
        E.EmployeeID AS SubordinateID, 
        M.EmployeeID AS ManagerID, 
        E.LastName AS SubordinateName, 
        M.LastName AS ManagerName,
        E.Title AS SubordinateTitle,
        M.Title AS ManagerTitle
    FROM Employees E
    INNER JOIN Employees M ON E.ReportsTo = M.EmployeeID
    WHERE M.Title IN ('Vice President, Sales', 'Sales Manager'); -- Selection

OPEN ReportingCursor;
FETCH NEXT FROM ReportingCursor INTO 
    @v_SubordinateID, @v_ManagerID, @v_SubName, @v_MgrName, @v_SubTitle, @v_MgrTitle;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextReportingAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_ManagementReportingLedger (
        ReportingAuditID, SubordinateID, ManagerID, SubordinateName, ManagerName, SubordinateTitle, ManagerTitle
    )
    VALUES (
        @nextReportingAuditID, @v_SubordinateID, @v_ManagerID, @v_SubName, @v_MgrName, @v_SubTitle, @v_MgrTitle
    );

    -- Log Dual-Source Lineage (Self-Join Relationship)
    -- Record source for the Subordinate record
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_SubordinateID AS VARCHAR), 'Table_ManagementReportingLedger', 'ReportingAuditID', CAST(@nextReportingAuditID AS VARCHAR));
    
    -- Record source for the Manager record
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_ManagerID AS VARCHAR), 'Table_ManagementReportingLedger', 'ReportingAuditID', CAST(@nextReportingAuditID AS VARCHAR));
    
    FETCH NEXT FROM ReportingCursor INTO 
        @v_SubordinateID, @v_ManagerID, @v_SubName, @v_MgrName, @v_SubTitle, @v_MgrTitle;
END;

CLOSE ReportingCursor; 
DEALLOCATE ReportingCursor;
GO