-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Kinetic Energy Impact and Signal Attenuation Factors for aerospace telemetry.
-- Rule: Use selection and strictly univariate non-linear transformations (SQRT, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_AerospaceTelemetryMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitPrice,
    -- Non-linear Transformation 1 (Power): Kinetic Energy Impact f(x) = x^2.0
    -- Models energy scaling as a function of component mass/velocity (proxied by price tier).
    CAST(POWER(UnitPrice, 2.0) / 1000.0 AS DECIMAL(10,2)) AS KineticImpactScore,
    -- Non-linear Transformation 2 (Square Root): Signal Attenuation Factor f(x) = sqrt(x) * 1.41
    -- Models signal loss through shielding materials.
    CAST(SQRT(UnitPrice) * 1.414 AS DECIMAL(10,2)) AS AttenuationFactor
FROM Products
WHERE CategoryID = 2; -- Selection applied here (Focus on specialized electronic/condiment hardware in this schema)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_AerospaceTelemetryMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_AerospaceTelemetryMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High impact risk only).
IF OBJECT_ID('Table_Telemetry_Safety_Audit', 'U') IS NOT NULL DROP TABLE Table_Telemetry_Safety_Audit;
CREATE TABLE Table_Telemetry_Safety_Audit (
    AuditID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    KineticImpactScore DECIMAL(10,2),
    AttenuationFactor DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_impact DECIMAL(10,2), @v_attenuation DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process telemetry metrics for components with a kinetic impact score exceeding 50.00
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, KineticImpactScore, AttenuationFactor 
    FROM vw_AerospaceTelemetryMetrics 
    WHERE KineticImpactScore > 50.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_impact, @v_attenuation;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Telemetry_Safety_Audit (AuditID, OriginalProductID, ProductName, KineticImpactScore, AttenuationFactor)
    VALUES (@nextAuditID, @v_pid, @v_pname, @v_impact, @v_attenuation);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_AerospaceTelemetryMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Telemetry_Safety_Audit', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_impact, @v_attenuation;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeTelemetryReport AS
BEGIN
    IF OBJECT_ID('Final_TelemetryMissionRegistry', 'U') IS NOT NULL DROP TABLE Final_TelemetryMissionRegistry;
    CREATE TABLE Final_TelemetryMissionRegistry (
        ReportID INT, 
        ComponentName NVARCHAR(40), 
        EnergyImpactRating DECIMAL(10,2), 
        SignalLossRating DECIMAL(10,2),
        AuditStatus VARCHAR(30)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_impact DECIMAL(10,2), @t_attenuation DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, KineticImpactScore, AttenuationFactor 
        FROM ##TempTelemetryBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_impact, @t_attenuation;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_TelemetryMissionRegistry (ReportID, ComponentName, EnergyImpactRating, SignalLossRating, AuditStatus)
        VALUES (@finalID, @t_pname, @t_impact, @t_attenuation, 'Telemetry Cleared');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempTelemetryBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_TelemetryMissionRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_impact, @t_attenuation;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageTelemetryMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempTelemetryBuffer') IS NOT NULL DROP TABLE ##TempTelemetryBuffer;
    CREATE TABLE ##TempTelemetryBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        KineticImpactScore DECIMAL(10,2),
        AttenuationFactor DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @impact DECIMAL(10,2), @attenuation DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AuditID, ProductName, KineticImpactScore, AttenuationFactor 
        FROM Table_Telemetry_Safety_Audit;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @impact, @attenuation;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempTelemetryBuffer VALUES (@newTempID, @pname, @impact, @attenuation);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Telemetry_Safety_Audit', 'AuditID', CAST(@tid AS VARCHAR), '##TempTelemetryBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @impact, @attenuation;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeTelemetryReport;
END;
GO

EXEC proc_StageTelemetryMetrics;