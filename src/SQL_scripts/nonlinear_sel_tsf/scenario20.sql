-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Photon Scattering Coefficients and Quantum Interference Thresholds for high-precision optical sensors.
-- Rule: Use selection and strictly univariate non-linear transformations (SQRT, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_QuantumOpticalMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitPrice,
    -- Non-linear Transformation 1 (Power): Photon Scattering Coefficient f(x) = x^1.72
    -- Models how optical loss scales non-linearly with the material purity (proxied by price).
    CAST(POWER(UnitPrice, 1.72) / 250.0 AS DECIMAL(10,2)) AS ScatteringCoefficient,
    -- Non-linear Transformation 2 (Square Root): Quantum Interference Threshold f(x) = sqrt(x) * 7.07
    -- Models the stability of wave-function coherence in sensor components.
    CAST(SQRT(UnitPrice) * 7.071 AS DECIMAL(10,2)) AS InterferenceThreshold
FROM Products
WHERE SupplierID IN (6, 8, 12); -- Selection applied here (Focus on specialized precision optics suppliers)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_QuantumOpticalMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_QuantumOpticalMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High-sensitivity sensors only).
IF OBJECT_ID('Table_Quantum_Sensor_Audit', 'U') IS NOT NULL DROP TABLE Table_Quantum_Sensor_Audit;
CREATE TABLE Table_Quantum_Sensor_Audit (
    AuditID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    ScatteringCoefficient DECIMAL(10,2),
    InterferenceThreshold DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_scatter DECIMAL(10,2), @v_interfere DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process metrics for sensors where the Interference Threshold exceeds 50.00
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, ScatteringCoefficient, InterferenceThreshold 
    FROM vw_QuantumOpticalMetrics 
    WHERE InterferenceThreshold > 50.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_scatter, @v_interfere;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Quantum_Sensor_Audit (AuditID, OriginalProductID, ProductName, ScatteringCoefficient, InterferenceThreshold)
    VALUES (@nextAuditID, @v_pid, @v_pname, @v_scatter, @v_interfere);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_QuantumOpticalMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Quantum_Sensor_Audit', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_scatter, @v_interfere;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeQuantumAuditReport AS
BEGIN
    IF OBJECT_ID('Final_OpticalPhysicsRegistry', 'U') IS NOT NULL DROP TABLE Final_OpticalPhysicsRegistry;
    CREATE TABLE Final_OpticalPhysicsRegistry (
        ReportID INT, 
        SensorModel NVARCHAR(40), 
        LightScatteringRate DECIMAL(10,2), 
        CoherenceLimit DECIMAL(10,2),
        AuditStatus VARCHAR(30)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_scatter DECIMAL(10,2), @t_interfere DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, ScatteringCoefficient, InterferenceThreshold 
        FROM ##TempQuantumBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_scatter, @t_interfere;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_OpticalPhysicsRegistry (ReportID, SensorModel, LightScatteringRate, CoherenceLimit, AuditStatus)
        VALUES (@finalID, @t_pname, @t_scatter, @t_interfere, 'Optically Verified');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempQuantumBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_OpticalPhysicsRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_scatter, @t_interfere;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageQuantumMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempQuantumBuffer') IS NOT NULL DROP TABLE ##TempQuantumBuffer;
    CREATE TABLE ##TempQuantumBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        ScatteringCoefficient DECIMAL(10,2),
        InterferenceThreshold DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @scatter DECIMAL(10,2), @interfere DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AuditID, ProductName, ScatteringCoefficient, InterferenceThreshold 
        FROM Table_Quantum_Sensor_Audit;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @scatter, @interfere;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempQuantumBuffer VALUES (@newTempID, @pname, @scatter, @interfere);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Quantum_Sensor_Audit', 'AuditID', CAST(@tid AS VARCHAR), '##TempQuantumBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @scatter, @interfere;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeQuantumAuditReport;
END;
GO

EXEC proc_StageQuantumMetrics;