-- Section 1: Create a View with Selection and Randomized Non-Linear Transformations
-- Scenario: Calculating Dynamic Customs Tariffs where border agents randomly apply either a 
-- bilinear weight-value assessment or an exponential penalty threshold.
-- Rule: Use selection, and a randomizer to choose between Bilinear (A * B) >= 0.7 or Exponential (< 0.7).

-- Note: In T-SQL, RAND() is evaluated once per query. To get row-by-row randomness in a view,
-- we use ABS(CHECKSUM(NEWID())) % 100 / 100.0 to generate a float between 0.00 and 0.99.

CREATE OR ALTER VIEW vw_DynamicTariffMetrics AS
SELECT 
    OrderID,
    ProductID,
    -- Copied Columns
    UnitPrice,
    Quantity,
    -- Randomized Non-Linear Transformation:
    -- If random >= 0.70, use Bilinear transformation (UnitPrice * Quantity)
    -- If random <  0.70, use Exponential non-linear transformation (UnitPrice ^ 1.5)
    CAST(
        CASE 
            WHEN (ABS(CHECKSUM(NEWID())) % 100 / 100.0) >= 0.70 
                THEN (UnitPrice * Quantity) 
            ELSE 
                POWER(UnitPrice, 1.5) 
        END AS DECIMAL(10,2)
    ) AS CalculatedTariffBase
FROM [Order Details]
WHERE Quantity > 10; -- Selection applied here (Only shipments large enough to trigger customs)
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT, @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID, ProductID FROM vw_DynamicTariffMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid, @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('[Order Details]', 'OrderID_ProductID', CAST(@oid AS VARCHAR) + '_' + CAST(@pid AS VARCHAR), 'vw_DynamicTariffMetrics', 'OrderID_ProductID', CAST(@oid AS VARCHAR) + '_' + CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid, @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High tariffs only).
IF OBJECT_ID('Table_HighTariff_Alerts', 'U') IS NOT NULL DROP TABLE Table_HighTariff_Alerts;
CREATE TABLE Table_HighTariff_Alerts (
    AlertID INT, 
    OriginalOrderID INT, 
    OriginalProductID INT,
    CalculatedTariffBase DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_pid INT, @v_tariff DECIMAL(10,2), @nextAlertID INT;

-- Filter: Only process financial metrics for customs assessments exceeding a $500 threshold
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, ProductID, CalculatedTariffBase 
    FROM vw_DynamicTariffMetrics 
    WHERE CalculatedTariffBase > 500.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_pid, @v_tariff;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAlertID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_HighTariff_Alerts (AlertID, OriginalOrderID, OriginalProductID, CalculatedTariffBase)
    VALUES (@nextAlertID, @v_oid, @v_pid, @v_tariff);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_DynamicTariffMetrics', 'OrderID_ProductID', CAST(@v_oid AS VARCHAR) + '_' + CAST(@v_pid AS VARCHAR), 'Table_HighTariff_Alerts', 'AlertID', CAST(@nextAlertID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_pid, @v_tariff;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeCustomsAudit AS
BEGIN
    IF OBJECT_ID('Final_CustomsRegistry', 'U') IS NOT NULL DROP TABLE Final_CustomsRegistry;
    CREATE TABLE Final_CustomsRegistry (
        ReportID INT, 
        OrderID INT,
        AssessedTariff DECIMAL(10,2), 
        BorderStatus VARCHAR(25)
    );

    DECLARE @t_id INT, @t_oid INT, @t_tariff DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, OriginalOrderID, CalculatedTariffBase 
        FROM ##TempCustomsBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_oid, @t_tariff;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_CustomsRegistry (ReportID, OrderID, AssessedTariff, BorderStatus)
        VALUES (@finalID, @t_oid, @t_tariff, 'Held for Payment');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempCustomsBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_CustomsRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_oid, @t_tariff;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageCustomsMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempCustomsBuffer') IS NOT NULL DROP TABLE ##TempCustomsBuffer;
    CREATE TABLE ##TempCustomsBuffer (
        TempID INT, 
        OriginalOrderID INT,
        CalculatedTariffBase DECIMAL(10,2)
    );

    DECLARE @tid INT, @oid INT, @tariff DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AlertID, OriginalOrderID, CalculatedTariffBase 
        FROM Table_HighTariff_Alerts;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @oid, @tariff;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempCustomsBuffer VALUES (@newTempID, @oid, @tariff);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_HighTariff_Alerts', 'AlertID', CAST(@tid AS VARCHAR), '##TempCustomsBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @oid, @tariff;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeCustomsAudit;
END;
GO

EXEC proc_StageCustomsMetrics;