-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Robotic Handling Complexity and Bin Allocation Volume for automated warehouse picking.
-- Rule: Use selection and strictly univariate non-linear transformations (LOG, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_RoboticFulfillmentMetrics AS
SELECT 
    OrderID,
    ProductID,
    -- Copied Columns
    UnitPrice,
    Quantity,
    Discount,
    -- Non-linear Transformation 1 (Logarithmic): Robotic Handling Complexity f(x) = 15 * ln(x)
    -- Models diminishing returns on complexity as robots pick larger continuous batches of the same item.
    CAST(LOG(Quantity) * 15.00 AS DECIMAL(10,2)) AS HandlingComplexityScore,
    -- Non-linear Transformation 2 (Power): Bin Allocation Volume f(x) = x^1.12
    -- Models how physical space requirements scale non-linearly due to packaging and stacking inefficiencies.
    CAST(POWER(Quantity, 1.12) AS DECIMAL(10,2)) AS BinAllocationVolume
FROM [Order Details]
WHERE Quantity >= 100; -- Selection applied here (Only bulk lines eligible for automated robotic picking)
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT, @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID, ProductID FROM vw_RoboticFulfillmentMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid, @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('[Order Details]', 'OrderID_ProductID', CAST(@oid AS VARCHAR) + '_' + CAST(@pid AS VARCHAR), 'vw_RoboticFulfillmentMetrics', 'OrderID_ProductID', CAST(@oid AS VARCHAR) + '_' + CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid, @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High-value items only).
IF OBJECT_ID('Table_Automated_Premium_Picking', 'U') IS NOT NULL DROP TABLE Table_Automated_Premium_Picking;
CREATE TABLE Table_Automated_Premium_Picking (
    QueueLogID INT, 
    OriginalOrderID INT, 
    OriginalProductID INT,
    HandlingComplexityScore DECIMAL(10,2),
    BinAllocationVolume DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_pid INT, @v_complex DECIMAL(10,2), @v_volume DECIMAL(10,2), @nextQueueID INT;

-- Filter: Only process robotics metrics for order lines where the individual item price exceeds $50
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, ProductID, HandlingComplexityScore, BinAllocationVolume 
    FROM vw_RoboticFulfillmentMetrics 
    WHERE UnitPrice > 50.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_pid, @v_complex, @v_volume;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextQueueID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Automated_Premium_Picking (QueueLogID, OriginalOrderID, OriginalProductID, HandlingComplexityScore, BinAllocationVolume)
    VALUES (@nextQueueID, @v_oid, @v_pid, @v_complex, @v_volume);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_RoboticFulfillmentMetrics', 'OrderID_ProductID', CAST(@v_oid AS VARCHAR) + '_' + CAST(@v_pid AS VARCHAR), 'Table_Automated_Premium_Picking', 'QueueLogID', CAST(@nextQueueID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_pid, @v_complex, @v_volume;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeRoboticQueue AS
BEGIN
    IF OBJECT_ID('Final_WarehouseAutomationStatus', 'U') IS NOT NULL DROP TABLE Final_WarehouseAutomationStatus;
    CREATE TABLE Final_WarehouseAutomationStatus (
        ReportID INT, 
        TargetOrderID INT, 
        CalculatedStrain DECIMAL(10,2), 
        RequiredCubicSpace DECIMAL(10,2),
        FulfillmentStatus VARCHAR(25)
    );

    DECLARE @t_id INT, @t_oid INT, @t_complex DECIMAL(10,2), @t_volume DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, OriginalOrderID, HandlingComplexityScore, BinAllocationVolume 
        FROM ##TempRoboticBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_oid, @t_complex, @t_volume;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_WarehouseAutomationStatus (ReportID, TargetOrderID, CalculatedStrain, RequiredCubicSpace, FulfillmentStatus)
        VALUES (@finalID, @t_oid, @t_complex, @t_volume, 'Routed to Auto-Picker');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempRoboticBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_WarehouseAutomationStatus', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_oid, @t_complex, @t_volume;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageRoboticMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempRoboticBuffer') IS NOT NULL DROP TABLE ##TempRoboticBuffer;
    CREATE TABLE ##TempRoboticBuffer (
        TempID INT, 
        OriginalOrderID INT, 
        HandlingComplexityScore DECIMAL(10,2),
        BinAllocationVolume DECIMAL(10,2)
    );

    DECLARE @tid INT, @oid INT, @complex DECIMAL(10,2), @volume DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT QueueLogID, OriginalOrderID, HandlingComplexityScore, BinAllocationVolume 
        FROM Table_Automated_Premium_Picking;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @oid, @complex, @volume;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempRoboticBuffer VALUES (@newTempID, @oid, @complex, @volume);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Automated_Premium_Picking', 'QueueLogID', CAST(@tid AS VARCHAR), '##TempRoboticBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @oid, @complex, @volume;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeRoboticQueue;
END;
GO

EXEC proc_StageRoboticMetrics;