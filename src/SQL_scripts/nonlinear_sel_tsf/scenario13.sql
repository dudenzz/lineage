-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Market Volatility Indices and Shelf-Life Attrition Scores for highly perishable seafood.
-- Rule: Use selection and strictly univariate non-linear transformations (SQRT, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_SupplyVolatilityMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitsInStock,
    UnitsOnOrder,
    -- Non-linear Transformation 1 (Square Root): Market Volatility Index f(x) = sqrt(x) * 4.5
    -- Models how supply chain volatility scales with the volume of units currently in transit/on order.
    CAST(SQRT(UnitsOnOrder) * 4.50 AS DECIMAL(10,2)) AS VolatilityIndex,
    -- Non-linear Transformation 2 (Power): Shelf-Life Attrition Score f(x) = x^1.03
    -- Models the exponentially compounding risk of spoilage as standing inventory increases.
    CAST(POWER(UnitsInStock, 1.03) AS DECIMAL(10,2)) AS AttritionScore
FROM Products
WHERE CategoryID = 8; -- Selection applied here (Category 8 is Seafood, highly perishable)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_SupplyVolatilityMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_SupplyVolatilityMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High transit volumes only).
IF OBJECT_ID('Table_Seafood_TransitRisk', 'U') IS NOT NULL DROP TABLE Table_Seafood_TransitRisk;
CREATE TABLE Table_Seafood_TransitRisk (
    RiskLogID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    VolatilityIndex DECIMAL(10,2),
    AttritionScore DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_volatility DECIMAL(10,2), @v_attrition DECIMAL(10,2), @nextRiskID INT;

-- Filter: Only process supply chain metrics for seafood items that have a significant inbound order volume
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, VolatilityIndex, AttritionScore 
    FROM vw_SupplyVolatilityMetrics 
    WHERE UnitsOnOrder > 20;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_volatility, @v_attrition;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextRiskID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Seafood_TransitRisk (RiskLogID, OriginalProductID, ProductName, VolatilityIndex, AttritionScore)
    VALUES (@nextRiskID, @v_pid, @v_pname, @v_volatility, @v_attrition);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_SupplyVolatilityMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Seafood_TransitRisk', 'RiskLogID', CAST(@nextRiskID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_volatility, @v_attrition;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeSupplyReport AS
BEGIN
    IF OBJECT_ID('Final_PerishablesAudit', 'U') IS NOT NULL DROP TABLE Final_PerishablesAudit;
    CREATE TABLE Final_PerishablesAudit (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        SupplyInstability DECIMAL(10,2), 
        ProjectedWaste DECIMAL(10,2),
        AuditStatus VARCHAR(25)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_volatility DECIMAL(10,2), @t_attrition DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, VolatilityIndex, AttritionScore 
        FROM ##TempVolatilityBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_volatility, @t_attrition;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_PerishablesAudit (ReportID, ItemName, SupplyInstability, ProjectedWaste, AuditStatus)
        VALUES (@finalID, @t_pname, @t_volatility, @t_attrition, 'Review Cold Storage');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempVolatilityBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_PerishablesAudit', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_volatility, @t_attrition;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageVolatilityMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempVolatilityBuffer') IS NOT NULL DROP TABLE ##TempVolatilityBuffer;
    CREATE TABLE ##TempVolatilityBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        VolatilityIndex DECIMAL(10,2),
        AttritionScore DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @volatility DECIMAL(10,2), @attrition DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT RiskLogID, ProductName, VolatilityIndex, AttritionScore 
        FROM Table_Seafood_TransitRisk;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @volatility, @attrition;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempVolatilityBuffer VALUES (@newTempID, @pname, @volatility, @attrition);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Seafood_TransitRisk', 'RiskLogID', CAST(@tid AS VARCHAR), '##TempVolatilityBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @volatility, @attrition;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeSupplyReport;
END;
GO

EXEC proc_StageVolatilityMetrics;