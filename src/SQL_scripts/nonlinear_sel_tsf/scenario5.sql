-- Section 1: Create a View with Selection and Non-Linear Transformations
-- Scenario: Calculating Discount Elasticity and Scaled Order Value for bulk purchase analysis.
-- Rule: Use selection and non-linear transformations (Exponential and Bilinear).
CREATE OR ALTER VIEW vw_BulkOrderMetrics AS
SELECT 
    OrderID,
    ProductID,
    -- Copied Columns
    UnitPrice,
    Quantity,
    Discount,
    -- Non-linear Transformation 1 (Bilinear): Gross Line Value f(A,B) = A * B
    CAST((UnitPrice * Quantity) AS DECIMAL(10,2)) AS GrossLineValue,
    -- Non-linear Transformation 2 (Exponential): Discount Impact Factor f(x) = e^(-5x)
    -- This represents the diminishing perceived value of incremental discounts.
    CAST(EXP(-Discount * 5.0) AS DECIMAL(10,4)) AS DiscountImpactFactor
FROM [Order Details]
WHERE Quantity >= 50; -- Selection applied here (Bulk orders only)
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT, @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID, ProductID FROM vw_BulkOrderMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid, @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('[Order Details]', 'OrderID_ProductID', CAST(@oid AS VARCHAR) + '_' + CAST(@pid AS VARCHAR), 'vw_BulkOrderMetrics', 'OrderID_ProductID', CAST(@oid AS VARCHAR) + '_' + CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid, @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High-value bulk lines only).
IF OBJECT_ID('Table_HighValue_BulkLines', 'U') IS NOT NULL DROP TABLE Table_HighValue_BulkLines;
CREATE TABLE Table_HighValue_BulkLines (
    AuditLogID INT, 
    OriginalOrderID INT, 
    OriginalProductID INT,
    GrossLineValue DECIMAL(10,2),
    DiscountImpactFactor DECIMAL(10,4)
);
GO

DECLARE @v_oid INT, @v_pid INT, @v_gross DECIMAL(10,2), @v_impact DECIMAL(10,4), @nextAuditID INT;

-- Filter: Only process financial metrics for bulk lines with a gross value exceeding $2,000
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, ProductID, GrossLineValue, DiscountImpactFactor 
    FROM vw_BulkOrderMetrics 
    WHERE GrossLineValue > 2000.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_pid, @v_gross, @v_impact;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_HighValue_BulkLines (AuditLogID, OriginalOrderID, OriginalProductID, GrossLineValue, DiscountImpactFactor)
    VALUES (@nextAuditID, @v_oid, @v_pid, @v_gross, @v_impact);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_BulkOrderMetrics', 'OrderID_ProductID', CAST(@v_oid AS VARCHAR) + '_' + CAST(@v_pid AS VARCHAR), 'Table_HighValue_BulkLines', 'AuditLogID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_pid, @v_gross, @v_impact;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeBulkAudit AS
BEGIN
    IF OBJECT_ID('Final_BulkPricingAnalysis', 'U') IS NOT NULL DROP TABLE Final_BulkPricingAnalysis;
    CREATE TABLE Final_BulkPricingAnalysis (
        ReportID INT, 
        OrderID INT,
        CalculatedGross DECIMAL(10,2), 
        ElasticityScore DECIMAL(10,4),
        AuditStatus VARCHAR(25)
    );

    DECLARE @t_id INT, @t_oid INT, @t_gross DECIMAL(10,2), @t_impact DECIMAL(10,4), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, OriginalOrderID, GrossLineValue, DiscountImpactFactor 
        FROM ##TempBulkBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_oid, @t_gross, @t_impact;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_BulkPricingAnalysis (ReportID, OrderID, CalculatedGross, ElasticityScore, AuditStatus)
        VALUES (@finalID, @t_oid, @t_gross, @t_impact, 'Pricing Verified');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempBulkBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_BulkPricingAnalysis', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_oid, @t_gross, @t_impact;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageBulkMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempBulkBuffer') IS NOT NULL DROP TABLE ##TempBulkBuffer;
    CREATE TABLE ##TempBulkBuffer (
        TempID INT, 
        OriginalOrderID INT,
        GrossLineValue DECIMAL(10,2),
        DiscountImpactFactor DECIMAL(10,4)
    );

    DECLARE @tid INT, @oid INT, @gross DECIMAL(10,2), @impact DECIMAL(10,4), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AuditLogID, OriginalOrderID, GrossLineValue, DiscountImpactFactor 
        FROM Table_HighValue_BulkLines;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @oid, @gross, @impact;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempBulkBuffer VALUES (@newTempID, @oid, @gross, @impact);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_HighValue_BulkLines', 'AuditLogID', CAST(@tid AS VARCHAR), '##TempBulkBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @oid, @gross, @impact;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeBulkAudit;
END;
GO

EXEC proc_StageBulkMetrics;