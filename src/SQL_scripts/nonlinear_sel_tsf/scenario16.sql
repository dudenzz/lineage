-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Aerodynamic Drag Coefficients and Thermal Dissipation Indices for specialized shipping containers.
-- Rule: Use selection and strictly univariate non-linear transformations (SQRT, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_AerospaceShippingMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitPrice,
    -- Non-linear Transformation 1 (Power): Aerodynamic Drag Coefficient f(x) = x^1.45
    -- Models how wind resistance costs scale non-linearly with the frontal surface area (proxied by price/size).
    CAST(POWER(UnitPrice, 1.45) / 100.0 AS DECIMAL(10,2)) AS DragCoefficient,
    -- Non-linear Transformation 2 (Square Root): Thermal Dissipation Index f(x) = sqrt(x) * 9.81
    -- Models the heat radiation efficiency of high-grade alloy containers.
    CAST(SQRT(UnitPrice) * 9.81 AS DECIMAL(10,2)) AS ThermalIndex
FROM Products
WHERE CategoryID = 6; -- Selection applied here (Meat/Poultry, requiring high-spec climate-controlled containers)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_AerospaceShippingMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_AerospaceShippingMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High-drag scenarios).
IF OBJECT_ID('Table_HighVelocity_Logistics', 'U') IS NOT NULL DROP TABLE Table_HighVelocity_Logistics;
CREATE TABLE Table_HighVelocity_Logistics (
    LogisticsID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    DragCoefficient DECIMAL(10,2),
    ThermalIndex DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_drag DECIMAL(10,2), @v_thermal DECIMAL(10,2), @nextLogID INT;

-- Filter: Only process logistics metrics for high-drag containers that require specialized airflow routing
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, DragCoefficient, ThermalIndex 
    FROM vw_AerospaceShippingMetrics 
    WHERE DragCoefficient > 15.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_drag, @v_thermal;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLogID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_HighVelocity_Logistics (LogisticsID, OriginalProductID, ProductName, DragCoefficient, ThermalIndex)
    VALUES (@nextLogID, @v_pid, @v_pname, @v_drag, @v_thermal);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_AerospaceShippingMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_HighVelocity_Logistics', 'LogisticsID', CAST(@nextLogID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_drag, @v_thermal;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeShippingPhysicsReport AS
BEGIN
    IF OBJECT_ID('Final_ContainerPhysicsAudit', 'U') IS NOT NULL DROP TABLE Final_ContainerPhysicsAudit;
    CREATE TABLE Final_ContainerPhysicsAudit (
        ReportID INT, 
        ContainerType NVARCHAR(40), 
        WindResistanceScore DECIMAL(10,2), 
        CoolingEfficiency DECIMAL(10,2),
        AuditStatus VARCHAR(30)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_drag DECIMAL(10,2), @t_thermal DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, DragCoefficient, ThermalIndex 
        FROM ##TempPhysicsBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_drag, @t_thermal;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ContainerPhysicsAudit (ReportID, ContainerType, WindResistanceScore, CoolingEfficiency, AuditStatus)
        VALUES (@finalID, @t_pname, @t_drag, @t_thermal, 'Verified for Air-Freight');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempPhysicsBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ContainerPhysicsAudit', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_drag, @t_thermal;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageShippingPhysics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempPhysicsBuffer') IS NOT NULL DROP TABLE ##TempPhysicsBuffer;
    CREATE TABLE ##TempPhysicsBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        DragCoefficient DECIMAL(10,2),
        ThermalIndex DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @drag DECIMAL(10,2), @thermal DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT LogisticsID, ProductName, DragCoefficient, ThermalIndex 
        FROM Table_HighVelocity_Logistics;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @drag, @thermal;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempPhysicsBuffer VALUES (@newTempID, @pname, @drag, @thermal);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_HighVelocity_Logistics', 'LogisticsID', CAST(@tid AS VARCHAR), '##TempPhysicsBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @drag, @thermal;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeShippingPhysicsReport;
END;
GO

EXEC proc_StageShippingPhysics;