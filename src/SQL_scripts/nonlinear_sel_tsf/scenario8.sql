-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Delivery Complexity and Carbon Footprint Scores for regional logistics.
-- Rule: Use selection and strictly univariate non-linear transformations (POWER, LOG). No bilinear (A*B).
CREATE OR ALTER VIEW vw_LogisticsEmissionsMetrics AS
SELECT 
    OrderID,
    -- Copied Columns
    CustomerID,
    ShipCity,
    ShipCountry,
    Freight,
    -- Non-linear Transformation 1 (Exponential/Power): Carbon Footprint Score f(x) = x^1.25
    -- Models how emissions scale non-linearly with heavier freight weights.
    CAST(POWER(Freight, 1.25) AS DECIMAL(10,2)) AS CarbonFootprintScore,
    -- Non-linear Transformation 2 (Logarithmic): Delivery Complexity Index f(x) = 10 * ln(x + 5)
    -- Models diminishing returns on complexity as freight weight increases.
    CAST(LOG(Freight + 5.0) * 10.00 AS DECIMAL(10,2)) AS ComplexityIndex
FROM Orders
WHERE Freight > 10.00; -- Selection applied here (Exclude negligible lightweight shipments)
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_LogisticsEmissionsMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_LogisticsEmissionsMetrics', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High-emission European shipments).
IF OBJECT_ID('Table_HighEmission_Deliveries', 'U') IS NOT NULL DROP TABLE Table_HighEmission_Deliveries;
CREATE TABLE Table_HighEmission_Deliveries (
    LogisticsAuditID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    CarbonFootprintScore DECIMAL(10,2),
    ComplexityIndex DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_carbon DECIMAL(10,2), @v_complex DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process environmental metrics for deliveries in Germany with a high carbon score
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, CarbonFootprintScore, ComplexityIndex 
    FROM vw_LogisticsEmissionsMetrics 
    WHERE ShipCountry = 'Germany' AND CarbonFootprintScore > 100.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_carbon, @v_complex;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_HighEmission_Deliveries (LogisticsAuditID, OriginalOrderID, CustomerID, CarbonFootprintScore, ComplexityIndex)
    VALUES (@nextAuditID, @v_oid, @v_cid, @v_carbon, @v_complex);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_LogisticsEmissionsMetrics', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_HighEmission_Deliveries', 'LogisticsAuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_carbon, @v_complex;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeCarbonReport AS
BEGIN
    IF OBJECT_ID('Final_CarbonAuditRegistry', 'U') IS NOT NULL DROP TABLE Final_CarbonAuditRegistry;
    CREATE TABLE Final_CarbonAuditRegistry (
        ReportID INT, 
        ClientAccount NCHAR(5), 
        CalculatedEmissions DECIMAL(10,2), 
        LogisticalStrain DECIMAL(10,2),
        AuditStatus VARCHAR(25)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_carbon DECIMAL(10,2), @t_complex DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, CustomerID, CarbonFootprintScore, ComplexityIndex 
        FROM ##TempEmissionsBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_carbon, @t_complex;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_CarbonAuditRegistry (ReportID, ClientAccount, CalculatedEmissions, LogisticalStrain, AuditStatus)
        VALUES (@finalID, @t_cid, @t_carbon, @t_complex, 'Flagged for Offset');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempEmissionsBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_CarbonAuditRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_carbon, @t_complex;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageEmissionsMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempEmissionsBuffer') IS NOT NULL DROP TABLE ##TempEmissionsBuffer;
    CREATE TABLE ##TempEmissionsBuffer (
        TempID INT, 
        CustomerID NCHAR(5), 
        CarbonFootprintScore DECIMAL(10,2),
        ComplexityIndex DECIMAL(10,2)
    );

    DECLARE @tid INT, @cid NCHAR(5), @carbon DECIMAL(10,2), @complex DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT LogisticsAuditID, CustomerID, CarbonFootprintScore, ComplexityIndex 
        FROM Table_HighEmission_Deliveries;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @carbon, @complex;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempEmissionsBuffer VALUES (@newTempID, @cid, @carbon, @complex);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_HighEmission_Deliveries', 'LogisticsAuditID', CAST(@tid AS VARCHAR), '##TempEmissionsBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @carbon, @complex;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeCarbonReport;
END;
GO

EXEC proc_StageEmissionsMetrics;