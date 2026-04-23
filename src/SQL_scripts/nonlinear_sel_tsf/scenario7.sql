-- Section 1: Create a View with Selection and Non-Linear Transformations (No Bilinear, No Randomization)
-- Scenario: Calculating Product Obsolescence and Price Elasticity Indices for inventory forecasting.
-- Rule: Use selection and strictly univariate non-linear transformations (e.g., POWER, LOG). No bilinear (A*B) logic.
CREATE OR ALTER VIEW vw_ProductObsolescenceMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitPrice,
    UnitsInStock,
    -- Non-linear Transformation 1 (Power/Exponential): Elasticity Index f(x) = x^1.15
    CAST(POWER(UnitPrice, 1.15) AS DECIMAL(10,2)) AS ElasticityIndex,
    -- Non-linear Transformation 2 (Logarithmic): Obsolescence Score f(x) = 5 * ln(x + 2)
    CAST(LOG(UnitsInStock + 2.0) * 5.00 AS DECIMAL(10,2)) AS ObsolescenceScore
FROM Products
WHERE UnitsInStock > 10; -- Selection applied here (Only evaluate items with standing inventory)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_ProductObsolescenceMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ProductObsolescenceMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High obsolescence risk only).
IF OBJECT_ID('Table_HighRisk_Inventory', 'U') IS NOT NULL DROP TABLE Table_HighRisk_Inventory;
CREATE TABLE Table_HighRisk_Inventory (
    RiskLogID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    ElasticityIndex DECIMAL(10,2),
    ObsolescenceScore DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_elasticity DECIMAL(10,2), @v_obsolescence DECIMAL(10,2), @nextRiskID INT;

-- Filter: Only process financial metrics for inventory where the logarithmic obsolescence score exceeds 15.00
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, ElasticityIndex, ObsolescenceScore 
    FROM vw_ProductObsolescenceMetrics 
    WHERE ObsolescenceScore > 15.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_elasticity, @v_obsolescence;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextRiskID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_HighRisk_Inventory (RiskLogID, OriginalProductID, ProductName, ElasticityIndex, ObsolescenceScore)
    VALUES (@nextRiskID, @v_pid, @v_pname, @v_elasticity, @v_obsolescence);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ProductObsolescenceMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_HighRisk_Inventory', 'RiskLogID', CAST(@nextRiskID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_elasticity, @v_obsolescence;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeObsolescenceReport AS
BEGIN
    IF OBJECT_ID('Final_InventoryDepreciation', 'U') IS NOT NULL DROP TABLE Final_InventoryDepreciation;
    CREATE TABLE Final_InventoryDepreciation (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        MarketElasticity DECIMAL(10,2), 
        CalculatedDecay DECIMAL(10,2),
        ActionStatus VARCHAR(25)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_elasticity DECIMAL(10,2), @t_obsolescence DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, ElasticityIndex, ObsolescenceScore 
        FROM ##TempObsolescenceBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_elasticity, @t_obsolescence;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_InventoryDepreciation (ReportID, ItemName, MarketElasticity, CalculatedDecay, ActionStatus)
        VALUES (@finalID, @t_pname, @t_elasticity, @t_obsolescence, 'Marked for Clearance');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempObsolescenceBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_InventoryDepreciation', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_elasticity, @t_obsolescence;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageObsolescenceMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempObsolescenceBuffer') IS NOT NULL DROP TABLE ##TempObsolescenceBuffer;
    CREATE TABLE ##TempObsolescenceBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        ElasticityIndex DECIMAL(10,2),
        ObsolescenceScore DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @elasticity DECIMAL(10,2), @obsolescence DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT RiskLogID, ProductName, ElasticityIndex, ObsolescenceScore 
        FROM Table_HighRisk_Inventory;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @elasticity, @obsolescence;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempObsolescenceBuffer VALUES (@newTempID, @pname, @elasticity, @obsolescence);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_HighRisk_Inventory', 'RiskLogID', CAST(@tid AS VARCHAR), '##TempObsolescenceBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @elasticity, @obsolescence;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeObsolescenceReport;
END;
GO

EXEC proc_StageObsolescenceMetrics;