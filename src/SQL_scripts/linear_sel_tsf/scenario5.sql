-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Quality Assurance (QA) Testing Hours and Certification Costs for Products.
CREATE OR ALTER VIEW vw_ProductQAMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    ReorderLevel,
    -- Linear Transformation 1: Estimated QA Testing Hours f(x) = 0.5x + 2.0
    CAST((ReorderLevel * 0.5) + 2.0 AS DECIMAL(10,2)) AS TestingHours,
    -- Linear Transformation 2: Base Certification Cost f(x) = 0.1x + 100.00
    CAST((UnitPrice * 0.1) + 100.00 AS DECIMAL(10,2)) AS CertificationCost
FROM Products
WHERE ReorderLevel > 0; -- Filter: Only products that have an active reorder level
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_ProductQAMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ProductQAMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_QASchedule', 'U') IS NOT NULL DROP TABLE Table_QASchedule;
CREATE TABLE Table_QASchedule (
    QAScheduleID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    ReorderLevel SMALLINT,
    TestingHours DECIMAL(10,2),
    CertificationCost DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_rl SMALLINT, @v_hours DECIMAL(10,2), @v_cost DECIMAL(10,2), @nextQAID INT;

-- Filter: Only process QA for a specific category (e.g., Category 2 - Condiments)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, ReorderLevel, TestingHours, CertificationCost 
    FROM vw_ProductQAMetrics 
    WHERE CategoryID = 2;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_rl, @v_hours, @v_cost;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextQAID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_QASchedule (QAScheduleID, OriginalProductID, ProductName, ReorderLevel, TestingHours, CertificationCost)
    VALUES (@nextQAID, @v_pid, @v_pname, @v_rl, @v_hours, @v_cost);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ProductQAMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_QASchedule', 'QAScheduleID', CAST(@nextQAID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_rl, @v_hours, @v_cost;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeQAReport AS
BEGIN
    IF OBJECT_ID('Final_QAComplianceReport', 'U') IS NOT NULL DROP TABLE Final_QAComplianceReport;
    CREATE TABLE Final_QAComplianceReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalHoursAllocated DECIMAL(10,2), 
        FinalBudget DECIMAL(10,2),
        ComplianceStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_hours DECIMAL(10,2), @t_cost DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, TestingHours, CertificationCost 
        FROM ##TempQABuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_hours, @t_cost;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_QAComplianceReport (ReportID, ProductName, TotalHoursAllocated, FinalBudget, ComplianceStatus)
        VALUES (@finalID, @t_pname, @t_hours, @t_cost, 'Scheduled');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempQABuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_QAComplianceReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_hours, @t_cost;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageQAMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempQABuffer') IS NOT NULL DROP TABLE ##TempQABuffer;
    CREATE TABLE ##TempQABuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        TestingHours DECIMAL(10,2),
        CertificationCost DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @hours DECIMAL(10,2), @cost DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT QAScheduleID, ProductName, TestingHours, CertificationCost 
        FROM Table_QASchedule;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @hours, @cost;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempQABuffer VALUES (@newTempID, @pname, @hours, @cost);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_QASchedule', 'QAScheduleID', CAST(@tid AS VARCHAR), '##TempQABuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @hours, @cost;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeQAReport;
END;
GO

EXEC proc_StageQAMetrics;