-- Section 1: Create View with Grouping
-- Grain: One row per EmployeeID. This tests many-to-one lineage tracking.
CREATE VIEW vw_EmployeeOrderVolume AS
SELECT 
    EmployeeID, 
    COUNT(OrderID) AS TotalOrders,
    MAX(OrderDate) AS LastOrderDate
FROM Orders
GROUP BY EmployeeID;
GO

-- Log row-level lineage for the Grouping
-- Multiple OrderIDs feed into a single EmployeeID record in the view.
DECLARE @e_id INT;
DECLARE ViewCursor CURSOR FOR SELECT EmployeeID FROM vw_EmployeeOrderVolume;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @e_id;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Orders', 'OrderID', CAST(OrderID AS VARCHAR), 'vw_EmployeeOrderVolume', 'EmployeeID', CAST(@e_id AS VARCHAR)
    FROM Orders WHERE EmployeeID = @e_id;

    FETCH NEXT FROM ViewCursor INTO @e_id;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: View -> Table_EmployeeTiers
IF OBJECT_ID('Table_EmployeeTiers', 'U') IS NOT NULL DROP TABLE Table_EmployeeTiers;
CREATE TABLE Table_EmployeeTiers (
    TierRecordID INT, 
    EmployeeID INT, 
    EfficiencyRating VARCHAR(20)
);

DECLARE @emp_id INT, @ord_count INT, @nextTierID INT;
DECLARE TableCursor CURSOR FOR SELECT EmployeeID, TotalOrders FROM vw_EmployeeOrderVolume;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @emp_id, @ord_count;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextTierID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_EmployeeTiers (TierRecordID, EmployeeID, EfficiencyRating)
    VALUES (@nextTierID, @emp_id, 
            CASE WHEN @ord_count > 100 THEN 'High Velocity' ELSE 'Standard' END);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_EmployeeOrderVolume', 'EmployeeID', CAST(@emp_id AS VARCHAR), 'Table_EmployeeTiers', 'TierRecordID', CAST(@nextTierID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @emp_id, @ord_count;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Procedures using ##TempBonusBuffer
CREATE OR ALTER PROCEDURE proc_FinalizeBonusReport AS
BEGIN
    IF OBJECT_ID('Final_BonusAudit', 'U') IS NOT NULL DROP TABLE Final_BonusAudit;
    CREATE TABLE Final_BonusAudit (AuditID INT, EmployeeID INT, BonusAmount INT);

    DECLARE @t_id INT, @t_emp INT, @auditID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, EmployeeID FROM ##TempBonusBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_emp;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @auditID = NEXT VALUE FOR GlobalIDSequence;
        
        -- Logic: High velocity employees get a flat bonus of 500
        INSERT INTO Final_BonusAudit (AuditID, EmployeeID, BonusAmount)
        VALUES (@auditID, @t_emp, 500);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempBonusBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_BonusAudit', 'AuditID', CAST(@auditID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_emp;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessBonusEligibility AS
BEGIN
    IF OBJECT_ID('tempdb..##TempBonusBuffer') IS NOT NULL DROP TABLE ##TempBonusBuffer;
    CREATE TABLE ##TempBonusBuffer (TempID INT, EmployeeID INT);

    DECLARE @tid INT, @eid INT, @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT TierRecordID, EmployeeID FROM Table_EmployeeTiers WHERE EfficiencyRating = 'High Velocity';

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @eid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempBonusBuffer VALUES (@newTempID, @eid);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_EmployeeTiers', 'TierRecordID', CAST(@tid AS VARCHAR), '##TempBonusBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @eid;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeBonusReport;
END;
GO

EXEC proc_ProcessBonusEligibility;