-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Agent Commissions and Delivery Bonuses based on Freight.
CREATE OR ALTER VIEW vw_AgentCommissions AS
SELECT 
    OrderID,
    -- Copied Columns
    EmployeeID,
    CustomerID,
    Freight,
    -- Linear Transformation 1: Base Commission ($) f(x) = 0.05x + 10.00
    CAST((Freight * 0.05) + 10.00 AS DECIMAL(10,2)) AS BaseCommission,
    -- Linear Transformation 2: Delivery Bonus ($) f(x) = 0.02x + 5.00
    CAST((Freight * 0.02) + 5.00 AS DECIMAL(10,2)) AS DeliveryBonus
FROM Orders
WHERE EmployeeID IS NOT NULL; -- Filter: Only orders associated with a sales agent
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_AgentCommissions;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_AgentCommissions', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_USCommissions', 'U') IS NOT NULL DROP TABLE Table_USCommissions;
CREATE TABLE Table_USCommissions (
    CommissionID INT, 
    OriginalOrderID INT, 
    EmployeeID INT,
    BaseCommission DECIMAL(10,2),
    DeliveryBonus DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_eid INT, @v_comm DECIMAL(10,2), @v_bonus DECIMAL(10,2), @nextCommID INT;

-- Filter: Only process commissions for a specific group of Employee IDs (e.g., US-based reps)
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, EmployeeID, BaseCommission, DeliveryBonus 
    FROM vw_AgentCommissions 
    WHERE EmployeeID IN (1, 2, 3, 4, 8);

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_eid, @v_comm, @v_bonus;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextCommID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_USCommissions (CommissionID, OriginalOrderID, EmployeeID, BaseCommission, DeliveryBonus)
    VALUES (@nextCommID, @v_oid, @v_eid, @v_comm, @v_bonus);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_AgentCommissions', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_USCommissions', 'CommissionID', CAST(@nextCommID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_eid, @v_comm, @v_bonus;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizePayrollReport AS
BEGIN
    IF OBJECT_ID('Final_PayrollPayouts', 'U') IS NOT NULL DROP TABLE Final_PayrollPayouts;
    CREATE TABLE Final_PayrollPayouts (
        ReportID INT, 
        EmployeeID INT, 
        TotalPayout DECIMAL(10,2), 
        BonusApplied DECIMAL(10,2),
        PaymentStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_eid INT, @t_comm DECIMAL(10,2), @t_bonus DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, EmployeeID, BaseCommission, DeliveryBonus 
        FROM ##TempCommissionBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_eid, @t_comm, @t_bonus;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_PayrollPayouts (ReportID, EmployeeID, TotalPayout, BonusApplied, PaymentStatus)
        VALUES (@finalID, @t_eid, @t_comm, @t_bonus, 'Cleared');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempCommissionBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_PayrollPayouts', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_eid, @t_comm, @t_bonus;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageCommissions AS
BEGIN
    IF OBJECT_ID('tempdb..##TempCommissionBuffer') IS NOT NULL DROP TABLE ##TempCommissionBuffer;
    CREATE TABLE ##TempCommissionBuffer (
        TempID INT, 
        EmployeeID INT, 
        BaseCommission DECIMAL(10,2),
        DeliveryBonus DECIMAL(10,2)
    );

    DECLARE @tid INT, @eid INT, @comm DECIMAL(10,2), @bonus DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT CommissionID, EmployeeID, BaseCommission, DeliveryBonus 
        FROM Table_USCommissions;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @eid, @comm, @bonus;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempCommissionBuffer VALUES (@newTempID, @eid, @comm, @bonus);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_USCommissions', 'CommissionID', CAST(@tid AS VARCHAR), '##TempCommissionBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @eid, @comm, @bonus;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizePayrollReport;
END;
GO

EXEC proc_StageCommissions;