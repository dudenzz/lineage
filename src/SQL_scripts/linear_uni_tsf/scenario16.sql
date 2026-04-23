-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Building an Enterprise Asset Lifecycle & Utilization Ledger.
-- Rule: UNION ALL between different tables (Suppliers and Employees), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Strategic Supplier Weight" based on node depth and "Staff Utilization Index" based on organizational seniority.

IF OBJECT_ID('Table_AssetLifecycleLedger', 'U') IS NOT NULL DROP TABLE Table_AssetLifecycleLedger;
CREATE TABLE Table_AssetLifecycleLedger (
    LifecycleID INT,
    AssetSector VARCHAR(25),
    SourcePKID INT,
    ReferenceName NVARCHAR(40),
    UtilizationScore DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_AssetSector VARCHAR(25),
        @v_SourcePKID INT,
        @v_ReferenceName NVARCHAR(40),
        @v_UtilizationScore DECIMAL(18, 2),
        @nextLifecycleID INT;

-- Linear Transformation Constants:
-- 1. Strategic Partners (Suppliers): Weight is based on ID (proxy for legacy integration) plus a flat partnership bonus (y = 0.40 * SupplierID + 60.00)
-- 2. Human Capital (Employees): Utilization index based on ID (proxy for system seniority) plus a baseline capacity factor (y = 0.95 * EmployeeID + 15.00)
DECLARE @SupplierLegacyScalar DECIMAL(10,2) = 0.40;
DECLARE @SupplierStrategicBase DECIMAL(10,2) = 60.00;
DECLARE @EmployeeSeniorityScalar DECIMAL(10,2) = 0.95;
DECLARE @EmployeeCapacityBase DECIMAL(10,2) = 15.00;

-- Cursor combining DIFFERENT tables (Suppliers and Employees) via UNION ALL
DECLARE LifecycleCursor CURSOR FOR 
    -- Branch 1: Suppliers (Supply Chain Strategic Weight)
    -- Selection: Only suppliers located in Germany or Italy (European core partners)
    -- Transformation: Linear weight scaling (y = 0.40 * x + 60.00)
    SELECT 
        'StrategicPartner' AS AssetSector, 
        SupplierID AS SourcePKID, 
        CompanyName AS ReferenceName, 
        (CAST(SupplierID AS DECIMAL(18,2)) * @SupplierLegacyScalar) + @SupplierStrategicBase AS UtilizationScore 
    FROM Suppliers 
    WHERE Country IN ('Germany', 'Italy') -- Selection
    
    UNION ALL

    -- Branch 2: Employees (Personnel Utilization Audit)
    -- Selection: Only employees based in the USA region ('WA')
    -- Transformation: Linear utilization scaling based on system tenure proxy (y = 0.95 * x + 15.00)
    SELECT 
        'HumanCapital' AS AssetSector, 
        EmployeeID AS SourcePKID, 
        LastName AS ReferenceName, 
        (CAST(EmployeeID AS DECIMAL(18,2)) * @EmployeeSeniorityScalar) + @EmployeeCapacityBase AS UtilizationScore 
    FROM Employees
    WHERE Region = 'WA'; -- Selection

OPEN LifecycleCursor;
FETCH NEXT FROM LifecycleCursor INTO @v_AssetSector, @v_SourcePKID, @v_ReferenceName, @v_UtilizationScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLifecycleID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_AssetLifecycleLedger (LifecycleID, AssetSector, SourcePKID, ReferenceName, UtilizationScore)
    VALUES (@nextLifecycleID, @v_AssetSector, @v_SourcePKID, @v_ReferenceName, @v_UtilizationScore);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AssetSector = 'StrategicPartner'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', CAST(@v_SourcePKID AS VARCHAR), 'Table_AssetLifecycleLedger', 'LifecycleID', CAST(@nextLifecycleID AS VARCHAR));
    END
    ELSE IF @v_AssetSector = 'HumanCapital'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', CAST(@v_SourcePKID AS VARCHAR), 'Table_AssetLifecycleLedger', 'LifecycleID', CAST(@nextLifecycleID AS VARCHAR));
    END
    
    FETCH NEXT FROM LifecycleCursor INTO @v_AssetSector, @v_SourcePKID, @v_ReferenceName, @v_UtilizationScore;
END;

CLOSE LifecycleCursor; 
DEALLOCATE LifecycleCursor;
GO