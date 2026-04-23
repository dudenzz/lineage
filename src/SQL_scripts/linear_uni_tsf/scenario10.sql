-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Resource Availability & Overhead Ledger.
-- Rule: UNION ALL between different tables (Employees and Products), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Resource Allocation Costs" for staff support and warehouse storage maintenance.

IF OBJECT_ID('Table_ResourceOverheadLedger', 'U') IS NOT NULL DROP TABLE Table_ResourceOverheadLedger;
CREATE TABLE Table_ResourceOverheadLedger (
    OverheadID INT,
    ResourceType VARCHAR(25),
    SourcePKID INT,
    ResourceLabel NVARCHAR(40),
    OverheadCost MONEY -- Linearly transformed column
);
GO

DECLARE @v_ResourceType VARCHAR(25),
        @v_SourcePKID INT,
        @v_ResourceLabel NVARCHAR(40),
        @v_OverheadCost MONEY,
        @nextOverheadID INT;

-- Linear Transformation Constants:
-- 1. Staff Support (Employees): Monthly overhead based on ID (seniority proxy) plus a base desk fee (y = 12.00 * EmployeeID + 250.00)
-- 2. Storage Maintenance (Products): Per-unit storage overhead based on ReorderLevel (y = 0.85 * ReorderLevel + 5.00)
DECLARE @StaffSupportScalar MONEY = 12.00;
DECLARE @StaffBaseFee MONEY = 250.00;
DECLARE @StorageMaintenanceScalar MONEY = 0.85;
DECLARE @StorageBaseFee MONEY = 5.00;

-- Cursor combining DIFFERENT tables (Employees and Products) via UNION ALL
DECLARE OverheadCursor CURSOR FOR 
    -- Branch 1: Employees (Administrative Overhead)
    -- Selection: Only employees with a specific Title ('Sales Representative')
    -- Transformation: Linear cost scaling based on ID and fixed base fee (y = 12.00 * x + 250.00)
    SELECT 
        'StaffSupport' AS ResourceType, 
        EmployeeID AS SourcePKID, 
        LastName AS ResourceLabel, 
        (CAST(EmployeeID AS MONEY) * @StaffSupportScalar) + @StaffBaseFee AS OverheadCost 
    FROM Employees 
    WHERE Title = 'Sales Representative' -- Selection
    
    UNION ALL

    -- Branch 2: Products (Inventory Overhead)
    -- Selection: Only products currently in stock (UnitsInStock > 0)
    -- Transformation: Linear maintenance cost based on reorder thresholds (y = 0.85 * x + 5.00)
    SELECT 
        'StorageMaintenance' AS ResourceType, 
        ProductID AS SourcePKID, 
        ProductName AS ResourceLabel, 
        (CAST(ReorderLevel AS MONEY) * @StorageMaintenanceScalar) + @StorageBaseFee AS OverheadCost 
    FROM Products
    WHERE UnitsInStock > 0; -- Selection

OPEN OverheadCursor;
FETCH NEXT FROM OverheadCursor INTO @v_ResourceType, @v_SourcePKID, @v_ResourceLabel, @v_OverheadCost;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextOverheadID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_ResourceOverheadLedger (OverheadID, ResourceType, SourcePKID, ResourceLabel, OverheadCost)
    VALUES (@nextOverheadID, @v_ResourceType, @v_SourcePKID, @v_ResourceLabel, @v_OverheadCost);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_ResourceType = 'StaffSupport'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', CAST(@v_SourcePKID AS VARCHAR), 'Table_ResourceOverheadLedger', 'OverheadID', CAST(@nextOverheadID AS VARCHAR));
    END
    ELSE IF @v_ResourceType = 'StorageMaintenance'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_SourcePKID AS VARCHAR), 'Table_ResourceOverheadLedger', 'OverheadID', CAST(@nextOverheadID AS VARCHAR));
    END
    
    FETCH NEXT FROM OverheadCursor INTO @v_ResourceType, @v_SourcePKID, @v_ResourceLabel, @v_OverheadCost;
END;

CLOSE OverheadCursor; 
DEALLOCATE OverheadCursor;
GO