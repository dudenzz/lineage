-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Perceived Value Indices and Wholesale Resistance Scores for premium products.
-- Rule: Use selection and strictly univariate non-linear transformations (SQRT, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_ProductValuationMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitPrice,
    -- Non-linear Transformation 1 (Square Root): Perceived Value Index f(x) = sqrt(x) * 3.14
    -- Models psychological pricing where perceived value tapers off as actual price scales.
    CAST(SQRT(UnitPrice) * 3.14 AS DECIMAL(10,2)) AS PerceivedValueIndex,
    -- Non-linear Transformation 2 (Power): Wholesale Resistance Score f(x) = x^1.08
    -- Models the exponential difficulty in securing bulk wholesale orders for increasingly expensive items.
    CAST(POWER(UnitPrice, 1.08) AS DECIMAL(10,2)) AS WholesaleResistanceScore
FROM Products
WHERE UnitPrice > 20.00; -- Selection applied here (Exclude low-tier/cheap goods)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_ProductValuationMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ProductValuationMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (Condiments only).
IF OBJECT_ID('Table_Premium_Condiments', 'U') IS NOT NULL DROP TABLE Table_Premium_Condiments;
CREATE TABLE Table_Premium_Condiments (
    ValuationAuditID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    PerceivedValueIndex DECIMAL(10,2),
    WholesaleResistanceScore DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_value DECIMAL(10,2), @v_resist DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process pricing psychology metrics for Category 2 (Condiments)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, PerceivedValueIndex, WholesaleResistanceScore 
    FROM vw_ProductValuationMetrics 
    WHERE CategoryID = 2;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_value, @v_resist;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Premium_Condiments (ValuationAuditID, OriginalProductID, ProductName, PerceivedValueIndex, WholesaleResistanceScore)
    VALUES (@nextAuditID, @v_pid, @v_pname, @v_value, @v_resist);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ProductValuationMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Premium_Condiments', 'ValuationAuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_value, @v_resist;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeValuationReport AS
BEGIN
    IF OBJECT_ID('Final_PricingPsychologyAudit', 'U') IS NOT NULL DROP TABLE Final_PricingPsychologyAudit;
    CREATE TABLE Final_PricingPsychologyAudit (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        ConsumerPerception DECIMAL(10,2), 
        BulkSalesFriction DECIMAL(10,2),
        AuditStatus VARCHAR(25)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_value DECIMAL(10,2), @t_resist DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, PerceivedValueIndex, WholesaleResistanceScore 
        FROM ##TempValuationBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_value, @t_resist;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_PricingPsychologyAudit (ReportID, ItemName, ConsumerPerception, BulkSalesFriction, AuditStatus)
        VALUES (@finalID, @t_pname, @t_value, @t_resist, 'Pricing Validated');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempValuationBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_PricingPsychologyAudit', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_value, @t_resist;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageValuationMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempValuationBuffer') IS NOT NULL DROP TABLE ##TempValuationBuffer;
    CREATE TABLE ##TempValuationBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        PerceivedValueIndex DECIMAL(10,2),
        WholesaleResistanceScore DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @value DECIMAL(10,2), @resist DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT ValuationAuditID, ProductName, PerceivedValueIndex, WholesaleResistanceScore 
        FROM Table_Premium_Condiments;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @value, @resist;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempValuationBuffer VALUES (@newTempID, @pname, @value, @resist);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Premium_Condiments', 'ValuationAuditID', CAST(@tid AS VARCHAR), '##TempValuationBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @value, @resist;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeValuationReport;
END;
GO

EXEC proc_StageValuationMetrics;