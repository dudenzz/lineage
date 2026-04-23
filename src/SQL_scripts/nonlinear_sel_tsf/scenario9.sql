-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Storage Cost Factors and Degradation Risk Indices for highly-stocked perishable goods.
-- Rule: Use selection and strictly univariate non-linear transformations (POWER, LOG). No bilinear (A*B).
CREATE OR ALTER VIEW vw_DairyDegradationMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitsInStock,
    -- Non-linear Transformation 1 (Power): Storage Cost Factor f(x) = x^1.05
    -- Models how refrigeration and space costs scale non-linearly with larger quantities of dairy.
    CAST(POWER(UnitsInStock, 1.05) AS DECIMAL(10,2)) AS StorageCostFactor,
    -- Non-linear Transformation 2 (Logarithmic): Degradation Risk Index f(x) = 8 * ln(x + 10)
    -- Models the spoilage risk curve as inventory grows.
    CAST(LOG(UnitsInStock + 10.0) * 8.00 AS DECIMAL(10,2)) AS DegradationRiskIndex
FROM Products
WHERE CategoryID = 4 AND UnitsInStock > 50; -- Selection applied here (Dairy products with significant stock)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_DairyDegradationMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_DairyDegradationMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High risk only).
IF OBJECT_ID('Table_CriticalDairy_Stock', 'U') IS NOT NULL DROP TABLE Table_CriticalDairy_Stock;
CREATE TABLE Table_CriticalDairy_Stock (
    RiskAuditID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    StorageCostFactor DECIMAL(10,2),
    DegradationRiskIndex DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_storage DECIMAL(10,2), @v_risk DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process environmental metrics for inventory where the spoilage risk index exceeds 35
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, StorageCostFactor, DegradationRiskIndex 
    FROM vw_DairyDegradationMetrics 
    WHERE DegradationRiskIndex > 35.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_storage, @v_risk;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_CriticalDairy_Stock (RiskAuditID, OriginalProductID, ProductName, StorageCostFactor, DegradationRiskIndex)
    VALUES (@nextAuditID, @v_pid, @v_pname, @v_storage, @v_risk);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_DairyDegradationMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_CriticalDairy_Stock', 'RiskAuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_storage, @v_risk;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeSpoilageReport AS
BEGIN
    IF OBJECT_ID('Final_DairySpoilageRegistry', 'U') IS NOT NULL DROP TABLE Final_DairySpoilageRegistry;
    CREATE TABLE Final_DairySpoilageRegistry (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        ProjectedStorageOverhead DECIMAL(10,2), 
        SpoilageVulnerability DECIMAL(10,2),
        AuditStatus VARCHAR(25)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_storage DECIMAL(10,2), @t_risk DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, StorageCostFactor, DegradationRiskIndex 
        FROM ##TempDairyBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_storage, @t_risk;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_DairySpoilageRegistry (ReportID, ItemName, ProjectedStorageOverhead, SpoilageVulnerability, AuditStatus)
        VALUES (@finalID, @t_pname, @t_storage, @t_risk, 'Recommend Discounting');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempDairyBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_DairySpoilageRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_storage, @t_risk;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageDairyMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempDairyBuffer') IS NOT NULL DROP TABLE ##TempDairyBuffer;
    CREATE TABLE ##TempDairyBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        StorageCostFactor DECIMAL(10,2),
        DegradationRiskIndex DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @storage DECIMAL(10,2), @risk DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT RiskAuditID, ProductName, StorageCostFactor, DegradationRiskIndex 
        FROM Table_CriticalDairy_Stock;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @storage, @risk;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempDairyBuffer VALUES (@newTempID, @pname, @storage, @risk);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_CriticalDairy_Stock', 'RiskAuditID', CAST(@tid AS VARCHAR), '##TempDairyBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @storage, @risk;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeSpoilageReport;
END;
GO

EXEC proc_StageDairyMetrics;