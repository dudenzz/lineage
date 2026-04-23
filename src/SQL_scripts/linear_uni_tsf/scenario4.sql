-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Environmental Logistics Impact Ledger for shipping and warehouse operations.
-- Rule: UNION ALL between different tables (Employees and Orders), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating a "Carbon Offset Fee" for employee business travel versus heavy freight logistics.

IF OBJECT_ID('Table_CarbonImpactLedger', 'U') IS NOT NULL DROP TABLE Table_CarbonImpactLedger;
CREATE TABLE Table_CarbonImpactLedger (
    ImpactID INT,
    ImpactSector VARCHAR(25),
    SourcePKID INT,
    EntityLabel NVARCHAR(40),
    CarbonOffsetFee MONEY -- Linearly transformed column
);
GO

DECLARE @v_ImpactSector VARCHAR(25),
        @v_SourcePKID INT,
        @v_EntityLabel NVARCHAR(40),
        @v_CarbonOffsetFee MONEY,
        @nextImpactID INT;

-- Linear Transformation Constants (Offset Models):
-- 1. Personnel (Employees): Fixed daily commute offset based on tenure constant (y = 0.50 * EmployeeID + 10.00)
-- 2. Logistics (Orders): Freight-based carbon tax with flat processing fee (y = 0.12 * Freight + 5.00)
DECLARE @PersonnelScalar MONEY = 0.50;
DECLARE @PersonnelBaseOffset MONEY = 10.00;
DECLARE @LogisticsScalar MONEY = 0.12;
DECLARE @LogisticsFlatFee MONEY = 5.00;

-- Cursor combining DIFFERENT tables (Employees and Orders) via UNION ALL
DECLARE CarbonCursor CURSOR FOR 
    -- Branch 1: Employees (Commute/Travel Impact)
    -- Selection: Only employees based in London (Regional policy)
    -- Transformation: Linear scale based on ID as a proxy for tenure + base offset
    SELECT 
        'PersonnelCommute' AS ImpactSector, 
        EmployeeID AS SourcePKID, 
        LastName AS EntityLabel, 
        (CAST(EmployeeID AS MONEY) * @PersonnelScalar) + @PersonnelBaseOffset AS CarbonOffsetFee 
    FROM Employees 
    WHERE City = 'London' -- Selection
    
    UNION ALL

    -- Branch 2: Orders (Freight Impact)
    -- Selection: Heavy shipments only (Freight > 50)
    -- Transformation: Linear carbon tax on freight weight (y = 0.12 * x + 5.00)
    SELECT 
        'LogisticsFreight' AS ImpactSector, 
        OrderID AS SourcePKID, 
        ShipName AS EntityLabel, 
        (Freight * @LogisticsScalar) + @LogisticsFlatFee AS CarbonOffsetFee 
    FROM Orders
    WHERE Freight > 50; -- Selection

OPEN CarbonCursor;
FETCH NEXT FROM CarbonCursor INTO @v_ImpactSector, @v_SourcePKID, @v_EntityLabel, @v_CarbonOffsetFee;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextImpactID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the projected, filtered, and linearly transformed record
    INSERT INTO Table_CarbonImpactLedger (ImpactID, ImpactSector, SourcePKID, EntityLabel, CarbonOffsetFee)
    VALUES (@nextImpactID, @v_ImpactSector, @v_SourcePKID, @v_EntityLabel, @v_CarbonOffsetFee);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_ImpactSector = 'PersonnelCommute'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', CAST(@v_SourcePKID AS VARCHAR), 'Table_CarbonImpactLedger', 'ImpactID', CAST(@nextImpactID AS VARCHAR));
    END
    ELSE IF @v_ImpactSector = 'LogisticsFreight'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', CAST(@v_SourcePKID AS VARCHAR), 'Table_CarbonImpactLedger', 'ImpactID', CAST(@nextImpactID AS VARCHAR));
    END
    
    FETCH NEXT FROM CarbonCursor INTO @v_ImpactSector, @v_SourcePKID, @v_EntityLabel, @v_CarbonOffsetFee;
END;

CLOSE CarbonCursor; 
DEALLOCATE CarbonCursor;
GO