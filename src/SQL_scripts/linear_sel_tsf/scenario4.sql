-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Estimated Restock Times and Expedited Shipping Costs for pending reorders.
CREATE OR ALTER VIEW vw_ReorderLogistics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitsOnOrder,
    -- Linear Transformation 1: Expedited Shipping Cost f(x) = 2.50x + 50.00
    CAST((UnitsOnOrder * 2.50) + 50.00 AS DECIMAL(10,2)) AS ExpeditedCost,
    -- Linear Transformation 2: Est. Restock Time (Days) f(x) = 0.1x + 3.0
    CAST((UnitsOnOrder * 0.1) + 3.0 AS INT) AS RestockDays
FROM Products
WHERE UnitsOnOrder > 0; -- Filter: Only items currently being reordered
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_ReorderLogistics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ReorderLogistics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_ActiveReorders', 'U') IS NOT NULL DROP TABLE Table_ActiveReorders;
CREATE TABLE Table_ActiveReorders (
    ActiveReorderID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitsOnOrder SMALLINT,
    ExpeditedCost DECIMAL(10,2),
    RestockDays INT
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_uoo SMALLINT, @v_cost DECIMAL(10,2), @v_days INT, @nextReorderID INT;

-- Filter: Only process logistics for a specific supplier region/ID (e.g., Supplier 3)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitsOnOrder, ExpeditedCost, RestockDays 
    FROM vw_ReorderLogistics 
    WHERE SupplierID = 3;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uoo, @v_cost, @v_days;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextReorderID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_ActiveReorders (ActiveReorderID, OriginalProductID, ProductName, UnitsOnOrder, ExpeditedCost, RestockDays)
    VALUES (@nextReorderID, @v_pid, @v_pname, @v_uoo, @v_cost, @v_days);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ReorderLogistics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_ActiveReorders', 'ActiveReorderID', CAST(@nextReorderID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uoo, @v_cost, @v_days;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeReorderReport AS
BEGIN
    IF OBJECT_ID('Final_ReorderLogisticsReport', 'U') IS NOT NULL DROP TABLE Final_ReorderLogisticsReport;
    CREATE TABLE Final_ReorderLogisticsReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalExpeditedCost DECIMAL(10,2), 
        ExpectedWaitDays INT,
        ApprovalStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_cost DECIMAL(10,2), @t_days INT, @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, ExpeditedCost, RestockDays 
        FROM ##TempReorderBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_cost, @t_days;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ReorderLogisticsReport (ReportID, ProductName, TotalExpeditedCost, ExpectedWaitDays, ApprovalStatus)
        VALUES (@finalID, @t_pname, @t_cost, @t_days, 'Pending');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempReorderBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ReorderLogisticsReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_cost, @t_days;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageReorderMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempReorderBuffer') IS NOT NULL DROP TABLE ##TempReorderBuffer;
    CREATE TABLE ##TempReorderBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        ExpeditedCost DECIMAL(10,2),
        RestockDays INT
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @cost DECIMAL(10,2), @days INT, @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT ActiveReorderID, ProductName, ExpeditedCost, RestockDays 
        FROM Table_ActiveReorders;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @cost, @days;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempReorderBuffer VALUES (@newTempID, @pname, @cost, @days);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_ActiveReorders', 'ActiveReorderID', CAST(@tid AS VARCHAR), '##TempReorderBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @cost, @days;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeReorderReport;
END;
GO

EXEC proc_StageReorderMetrics;