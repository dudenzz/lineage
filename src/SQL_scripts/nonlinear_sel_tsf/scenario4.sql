-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Thermal Conductance and Atomic Decay Factors for high-grade cooling isotopes.
-- Rule: Use selection and strictly univariate non-linear transformations (SQRT, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_CryogenicMaterialMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitPrice,
    -- Non-linear Transformation 1 (Power): Thermal Conductance Factor f(x) = x^1.55
    -- Models how heat transfer efficiency scales non-linearly with material quality (proxied by price).
    CAST(POWER(UnitPrice, 1.55) / 100.0 AS DECIMAL(10,2)) AS ConductanceFactor,
    -- Non-linear Transformation 2 (Square Root): Atomic Decay Index f(x) = sqrt(x) * 4.33
    -- Models the stabilization rate of isotopes used in cryogenic storage.
    CAST(SQRT(UnitPrice) * 4.33 AS DECIMAL(10,2)) AS DecayIndex
FROM Products
WHERE SupplierID IN (1, 4, 7); -- Selection applied here (Focus on specialized chemical/isotope suppliers)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_CryogenicMaterialMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_CryogenicMaterialMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High-conductance materials only).
IF OBJECT_ID('Table_Cryo_Storage_Audit', 'U') IS NOT NULL DROP TABLE Table_Cryo_Storage_Audit;
CREATE TABLE Table_Cryo_Storage_Audit (
    AuditID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    ConductanceFactor DECIMAL(10,2),
    DecayIndex DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_cond DECIMAL(10,2), @v_decay DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process metrics for materials where the Conductance Factor exceeds 75.00
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, ConductanceFactor, DecayIndex 
    FROM vw_CryogenicMaterialMetrics 
    WHERE ConductanceFactor > 75.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_cond, @v_decay;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Cryo_Storage_Audit (AuditID, OriginalProductID, ProductName, ConductanceFactor, DecayIndex)
    VALUES (@nextAuditID, @v_pid, @v_pname, @v_cond, @v_decay);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_CryogenicMaterialMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Cryo_Storage_Audit', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_cond, @v_decay;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeCryoAuditReport AS
BEGIN
    IF OBJECT_ID('Final_ThermodynamicMaterialRegistry', 'U') IS NOT NULL DROP TABLE Final_ThermodynamicMaterialRegistry;
    CREATE TABLE Final_ThermodynamicMaterialRegistry (
        ReportID INT, 
        MaterialName NVARCHAR(40), 
        ThermalEfficiency DECIMAL(10,2), 
        IsotopeStability DECIMAL(10,2),
        AuditStatus VARCHAR(30)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_cond DECIMAL(10,2), @t_decay DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, ConductanceFactor, DecayIndex 
        FROM ##TempCryoBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_cond, @t_decay;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ThermodynamicMaterialRegistry (ReportID, MaterialName, ThermalEfficiency, IsotopeStability, AuditStatus)
        VALUES (@finalID, @t_pname, @t_cond, @t_decay, 'Cryo-Certified');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempCryoBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ThermodynamicMaterialRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_cond, @t_decay;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageCryoMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempCryoBuffer') IS NOT NULL DROP TABLE ##TempCryoBuffer;
    CREATE TABLE ##TempCryoBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        ConductanceFactor DECIMAL(10,2),
        DecayIndex DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @cond DECIMAL(10,2), @decay DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AuditID, ProductName, ConductanceFactor, DecayIndex 
        FROM Table_Cryo_Storage_Audit;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @cond, @decay;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempCryoBuffer VALUES (@newTempID, @pname, @cond, @decay);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Cryo_Storage_Audit', 'AuditID', CAST(@tid AS VARCHAR), '##TempCryoBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @cond, @decay;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeCryoAuditReport;
END;
GO

EXEC proc_StageCryoMetrics;