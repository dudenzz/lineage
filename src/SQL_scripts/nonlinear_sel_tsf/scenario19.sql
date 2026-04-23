-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Bio-Acoustic Resonance and Fluid Viscosity Resistance for specialized pharmaceutical processing.
-- Rule: Use selection and strictly univariate non-linear transformations (SQRT, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_BioPharmaProcessingMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitPrice,
    -- Non-linear Transformation 1 (Power): Acoustic Resonance Factor f(x) = x^1.65
    -- Models how ultrasonic cleaning intensity must scale with the material grade/cost.
    CAST(POWER(UnitPrice, 1.65) / 50.0 AS DECIMAL(10,2)) AS ResonanceFactor,
    -- Non-linear Transformation 2 (Square Root): Fluid Viscosity Index f(x) = sqrt(x) * 5.25
    -- Models the resistance of chemical buffers based on concentration levels (proxied by price).
    CAST(SQRT(UnitPrice) * 5.25 AS DECIMAL(10,2)) AS ViscosityIndex
FROM Products
WHERE CategoryID = 2; -- Selection applied here (Focusing on the 'Condiments' category as a proxy for liquid reagents)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_BioPharmaProcessingMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_BioPharmaProcessingMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High viscosity reagents only).
IF OBJECT_ID('Table_Viscous_Reagent_Audit', 'U') IS NOT NULL DROP TABLE Table_Viscous_Reagent_Audit;
CREATE TABLE Table_Viscous_Reagent_Audit (
    BatchLogID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    ResonanceFactor DECIMAL(10,2),
    ViscosityIndex DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_res DECIMAL(10,2), @v_visc DECIMAL(10,2), @nextBatchID INT;

-- Filter: Only process processing metrics for reagents where the Viscosity Index exceeds 30.00
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, ResonanceFactor, ViscosityIndex 
    FROM vw_BioPharmaProcessingMetrics 
    WHERE ViscosityIndex > 30.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_res, @v_visc;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextBatchID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Viscous_Reagent_Audit (BatchLogID, OriginalProductID, ProductName, ResonanceFactor, ViscosityIndex)
    VALUES (@nextBatchID, @v_pid, @v_pname, @v_res, @v_visc);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_BioPharmaProcessingMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Viscous_Reagent_Audit', 'BatchLogID', CAST(@nextBatchID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_res, @v_visc;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizePharmaProcessReport AS
BEGIN
    IF OBJECT_ID('Final_LabProcessingRegistry', 'U') IS NOT NULL DROP TABLE Final_LabProcessingRegistry;
    CREATE TABLE Final_LabProcessingRegistry (
        ReportID INT, 
        ReagentName NVARCHAR(40), 
        SonicTreatmentLevel DECIMAL(10,2), 
        FlowResistanceRating DECIMAL(10,2),
        AuditStatus VARCHAR(30)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_res DECIMAL(10,2), @t_visc DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, ResonanceFactor, ViscosityIndex 
        FROM ##TempPharmaBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_res, @t_visc;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_LabProcessingRegistry (ReportID, ReagentName, SonicTreatmentLevel, FlowResistanceRating, AuditStatus)
        VALUES (@finalID, @t_pname, @t_res, @t_visc, 'Validated for Infusion');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempPharmaBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_LabProcessingRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_res, @t_visc;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StagePharmaMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempPharmaBuffer') IS NOT NULL DROP TABLE ##TempPharmaBuffer;
    CREATE TABLE ##TempPharmaBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        ResonanceFactor DECIMAL(10,2),
        ViscosityIndex DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @res DECIMAL(10,2), @visc DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT BatchLogID, ProductName, ResonanceFactor, ViscosityIndex 
        FROM Table_Viscous_Reagent_Audit;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @res, @visc;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempPharmaBuffer VALUES (@newTempID, @pname, @res, @visc);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Viscous_Reagent_Audit', 'BatchLogID', CAST(@tid AS VARCHAR), '##TempPharmaBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @res, @visc;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizePharmaProcessReport;
END;
GO

EXEC proc_StagePharmaMetrics;