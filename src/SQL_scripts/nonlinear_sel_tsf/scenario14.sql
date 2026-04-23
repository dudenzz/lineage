-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Market Expansion Coefficients and Competitive Saturation Scores for international shipping.
-- Rule: Use selection and strictly univariate non-linear transformations (SQRT, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_GlobalMarketExpansionMetrics AS
SELECT 
    OrderID,
    -- Copied Columns
    CustomerID,
    ShipCountry,
    Freight,
    -- Non-linear Transformation 1 (Square Root): Expansion Coefficient f(x) = sqrt(x) * 2.75
    -- Models how potential for market growth scales with current shipping volume.
    CAST(SQRT(Freight) * 2.75 AS DECIMAL(10,2)) AS ExpansionCoefficient,
    -- Non-linear Transformation 2 (Power): Competitive Saturation Score f(x) = x^1.12
    -- Models the increasing barrier to entry as shipping density in a region rises.
    CAST(POWER(Freight, 1.12) AS DECIMAL(10,2)) AS SaturationScore
FROM Orders
WHERE ShipCountry IN ('Brazil', 'Mexico', 'Argentina', 'Venezuela'); -- Selection applied here (Latin American Market Focus)
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_GlobalMarketExpansionMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_GlobalMarketExpansionMetrics', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High-volume shipments to Brazil only).
IF OBJECT_ID('Table_Brazil_Growth_Strategy', 'U') IS NOT NULL DROP TABLE Table_Brazil_Growth_Strategy;
CREATE TABLE Table_Brazil_Growth_Strategy (
    StrategyLogID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    ExpansionCoefficient DECIMAL(10,2),
    SaturationScore DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_coeff DECIMAL(10,2), @v_sat DECIMAL(10,2), @nextStrategyID INT;

-- Filter: Only process strategic metrics for orders going to Brazil where the Expansion Coefficient is significant (> 20)
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, ExpansionCoefficient, SaturationScore 
    FROM vw_GlobalMarketExpansionMetrics 
    WHERE ShipCountry = 'Brazil' AND ExpansionCoefficient > 20.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_coeff, @v_sat;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextStrategyID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Brazil_Growth_Strategy (StrategyLogID, OriginalOrderID, CustomerID, ExpansionCoefficient, SaturationScore)
    VALUES (@nextStrategyID, @v_oid, @v_cid, @v_coeff, @v_sat);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_GlobalMarketExpansionMetrics', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_Brazil_Growth_Strategy', 'StrategyLogID', CAST(@nextStrategyID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_coeff, @v_sat;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeMarketStrategy AS
BEGIN
    IF OBJECT_ID('Final_InternationalExpansionAudit', 'U') IS NOT NULL DROP TABLE Final_InternationalExpansionAudit;
    CREATE TABLE Final_InternationalExpansionAudit (
        ReportID INT, 
        AccountRef NCHAR(5), 
        MarketGrowthFactor DECIMAL(10,2), 
        CompetitiveIntensity DECIMAL(10,2),
        StrategyStatus VARCHAR(30)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_coeff DECIMAL(10,2), @t_sat DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, CustomerID, ExpansionCoefficient, SaturationScore 
        FROM ##TempExpansionBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_coeff, @t_sat;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_InternationalExpansionAudit (ReportID, AccountRef, MarketGrowthFactor, CompetitiveIntensity, StrategyStatus)
        VALUES (@finalID, @t_cid, @t_coeff, @t_sat, 'Prioritize Investment');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempExpansionBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_InternationalExpansionAudit', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_coeff, @t_sat;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageExpansionMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempExpansionBuffer') IS NOT NULL DROP TABLE ##TempExpansionBuffer;
    CREATE TABLE ##TempExpansionBuffer (
        TempID INT, 
        CustomerID NCHAR(5), 
        ExpansionCoefficient DECIMAL(10,2),
        SaturationScore DECIMAL(10,2)
    );

    DECLARE @tid INT, @cid NCHAR(5), @coeff DECIMAL(10,2), @sat DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT StrategyLogID, CustomerID, ExpansionCoefficient, SaturationScore 
        FROM Table_Brazil_Growth_Strategy;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @coeff, @sat;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempExpansionBuffer VALUES (@newTempID, @cid, @coeff, @sat);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Brazil_Growth_Strategy', 'StrategyLogID', CAST(@tid AS VARCHAR), '##TempExpansionBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @coeff, @sat;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeMarketStrategy;
END;
GO

EXEC proc_StageExpansionMetrics;