-- Section 1: Create a View with Selection and Non-Linear Transformations
-- Scenario: Calculating Pending Capital tied up in reorders and Space Allocation Indices for incoming shipments.
-- Rule: Use selection, non-linear mathematical functions (POWER), and the bilinear transformation (A * B).
CREATE OR ALTER VIEW vw_PendingRestockMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitPrice,
    UnitsOnOrder,
    -- Non-linear Transformation 1 (Bilinear): Pending Capital f(A,B) = A * B
    CAST((UnitPrice * UnitsOnOrder) AS DECIMAL(10,2)) AS PendingCapital,
    -- Non-linear Transformation 2 (Exponential): Space Allocation Index f(x) = x^1.25
    CAST(POWER(UnitsOnOrder, 1.25) AS DECIMAL(10,2)) AS SpaceAllocationIndex
FROM Products
WHERE UnitsOnOrder > 0; -- Selection applied here (Only items currently being restocked)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_PendingRestockMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_PendingRestockMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High-value capital lockups only).
IF OBJECT_ID('Table_CriticalRestockCapital', 'U') IS NOT NULL DROP TABLE Table_CriticalRestockCapital;
CREATE TABLE Table_CriticalRestockCapital (
    RestockLogID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    PendingCapital DECIMAL(10,2),
    SpaceAllocationIndex DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_capital DECIMAL(10,2), @v_space DECIMAL(10,2), @nextLogID INT;

-- Filter: Only process financial metrics for shipments tying up more than $500 in capital
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, PendingCapital, SpaceAllocationIndex 
    FROM vw_PendingRestockMetrics 
    WHERE PendingCapital > 500.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_capital, @v_space;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLogID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_CriticalRestockCapital (RestockLogID, OriginalProductID, ProductName, PendingCapital, SpaceAllocationIndex)
    VALUES (@nextLogID, @v_pid, @v_pname, @v_capital, @v_space);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_PendingRestockMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_CriticalRestockCapital', 'RestockLogID', CAST(@nextLogID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_capital, @v_space;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeCapitalReport AS
BEGIN
    IF OBJECT_ID('Final_RestockFinancials', 'U') IS NOT NULL DROP TABLE Final_RestockFinancials;
    CREATE TABLE Final_RestockFinancials (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        CapitalCommitted DECIMAL(10,2), 
        LogisticsIndex DECIMAL(10,2),
        FinanceStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_capital DECIMAL(10,2), @t_space DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, PendingCapital, SpaceAllocationIndex 
        FROM ##TempCapitalBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_capital, @t_space;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_RestockFinancials (ReportID, ItemName, CapitalCommitted, LogisticsIndex, FinanceStatus)
        VALUES (@finalID, @t_pname, @t_capital, @t_space, 'Funds Reserved');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempCapitalBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_RestockFinancials', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_capital, @t_space;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageCapitalMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempCapitalBuffer') IS NOT NULL DROP TABLE ##TempCapitalBuffer;
    CREATE TABLE ##TempCapitalBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        PendingCapital DECIMAL(10,2),
        SpaceAllocationIndex DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @capital DECIMAL(10,2), @space DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT RestockLogID, ProductName, PendingCapital, SpaceAllocationIndex 
        FROM Table_CriticalRestockCapital;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @capital, @space;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempCapitalBuffer VALUES (@newTempID, @pname, @capital, @space);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_CriticalRestockCapital', 'RestockLogID', CAST(@tid AS VARCHAR), '##TempCapitalBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @capital, @space;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeCapitalReport;
END;
GO

EXEC proc_StageCapitalMetrics;