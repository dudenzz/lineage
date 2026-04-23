-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Seismic Stress Factors and Logarithmic Load Balancing Indices for structural equipment.
-- Rule: Use selection and strictly univariate non-linear transformations (SQRT, LOG). No bilinear (A*B).
CREATE OR ALTER VIEW vw_StructuralStressMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitPrice,
    -- Non-linear Transformation 1 (Square Root): Seismic Stress Factor f(x) = sqrt(x) * 6.28
    -- Models how mechanical stress requirements scale with the weight/cost of structural components.
    CAST(SQRT(UnitPrice) * 6.28 AS DECIMAL(10,2)) AS SeismicStressFactor,
    -- Non-linear Transformation 2 (Logarithmic): Load Balancing Index f(x) = 12 * ln(x + 1)
    -- Models the distribution efficiency as mass/unit value increases.
    CAST(LOG(UnitPrice + 1.0) * 12.00 AS DECIMAL(10,2)) AS LoadBalancingIndex
FROM Products
WHERE SupplierID IN (16, 17, 18); -- Selection applied here (Focus on heavy machinery and structural suppliers)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_StructuralStressMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_StructuralStressMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (Critical stress thresholds only).
IF OBJECT_ID('Table_Critical_Stress_Audit', 'U') IS NOT NULL DROP TABLE Table_Critical_Stress_Audit;
CREATE TABLE Table_Critical_Stress_Audit (
    AuditLogID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    SeismicStressFactor DECIMAL(10,2),
    LoadBalancingIndex DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_stress DECIMAL(10,2), @v_load DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process metrics for components where the Seismic Stress Factor exceeds a critical limit (> 40)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, SeismicStressFactor, LoadBalancingIndex 
    FROM vw_StructuralStressMetrics 
    WHERE SeismicStressFactor > 40.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_stress, @v_load;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Critical_Stress_Audit (AuditLogID, OriginalProductID, ProductName, SeismicStressFactor, LoadBalancingIndex)
    VALUES (@nextAuditID, @v_pid, @v_pname, @v_stress, @v_load);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_StructuralStressMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Critical_Stress_Audit', 'AuditLogID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_stress, @v_load;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeStructuralReport AS
BEGIN
    IF OBJECT_ID('Final_StructuralSafetyRegistry', 'U') IS NOT NULL DROP TABLE Final_StructuralSafetyRegistry;
    CREATE TABLE Final_StructuralSafetyRegistry (
        ReportID INT, 
        ComponentName NVARCHAR(40), 
        AssessedSeismicRisk DECIMAL(10,2), 
        StabilityIndex DECIMAL(10,2),
        AuditStatus VARCHAR(30)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_stress DECIMAL(10,2), @t_load DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, SeismicStressFactor, LoadBalancingIndex 
        FROM ##TempStructuralBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_stress, @t_load;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_StructuralSafetyRegistry (ReportID, ComponentName, AssessedSeismicRisk, StabilityIndex, AuditStatus)
        VALUES (@finalID, @t_pname, @t_stress, @t_load, 'Certified for High-Stress');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempStructuralBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_StructuralSafetyRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_stress, @t_load;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageStructuralMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempStructuralBuffer') IS NOT NULL DROP TABLE ##TempStructuralBuffer;
    CREATE TABLE ##TempStructuralBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        SeismicStressFactor DECIMAL(10,2),
        LoadBalancingIndex DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @stress DECIMAL(10,2), @load DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AuditLogID, ProductName, SeismicStressFactor, LoadBalancingIndex 
        FROM Table_Critical_Stress_Audit;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @stress, @load;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempStructuralBuffer VALUES (@newTempID, @pname, @stress, @load);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Critical_Stress_Audit', 'AuditLogID', CAST(@tid AS VARCHAR), '##TempStructuralBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @stress, @load;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeStructuralReport;
END;
GO

EXEC proc_StageStructuralMetrics;