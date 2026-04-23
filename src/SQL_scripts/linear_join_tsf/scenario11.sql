-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Employee-Territory Sales Performance & Coverage Index.
-- Rule: Combine inputs via INNER JOIN (Multi-table). Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_TerritorySalesCoverageLedger', 'U') IS NOT NULL DROP TABLE Table_TerritorySalesCoverageLedger;
CREATE TABLE Table_TerritorySalesCoverageLedger (
    CoverageAuditID INT,
    EmployeeID INT,           -- Native Projection from Employees
    TerritoryID NVARCHAR(20), -- Native Projection from Territories
    OrderID INT,              -- Native Projection from Orders
    LastName NVARCHAR(20),    -- Native Projection from Employees
    WeightedRegionalFreight MONEY, -- Linearly transformed column (Orders.Freight)
    CoverageIntensityScore DECIMAL(18,2), -- Linearly transformed column (Employees.EmployeeID)
    TerritoryCapacityIndex DECIMAL(18,2)  -- Linearly transformed column (Orders.Freight)
);
GO

DECLARE @v_EmployeeID INT,
        @v_TerritoryID NVARCHAR(20),
        @v_OrderID INT,
        @v_LastName NVARCHAR(20),
        @v_WeightedRegionalFreight MONEY,
        @v_CoverageIntensityScore DECIMAL(18,2),
        @v_TerritoryCapacityIndex DECIMAL(18,2),
        @nextCoverageAuditID INT;

-- Linear Transformation Constants:
-- 1. Weighted Regional Freight: Adjusts freight for specific regional logistical complexity (y = 1.22 * Freight + 40.00)
-- 2. Coverage Intensity Score: Calculated representative focus based on seniority proxy (y = 0.95 * EmployeeID + 150.00)
-- 3. Territory Capacity Index: Derived from order volume logistics (y = 0.30 * Freight + 25.00)
DECLARE @RegionalScalar DECIMAL(10,2) = 1.22;
DECLARE @RegionalBase MONEY = 40.00;
DECLARE @IntensityScalar DECIMAL(10,2) = 0.95;
DECLARE @IntensityBase DECIMAL(10,2) = 150.00;
DECLARE @CapacityScalar DECIMAL(10,2) = 0.30;
DECLARE @CapacityBase DECIMAL(10,2) = 25.00;

-- Cursor using JOIN to integrate Personnel assignments with active Order logistics.
-- Selection: Only employees assigned to territories in the 'Northern' or 'Southern' regions with freight > 30.00.
DECLARE CoverageCursor CURSOR FOR 
    SELECT 
        E.EmployeeID, 
        T.TerritoryID, 
        O.OrderID,
        E.LastName, 
        (O.Freight * @RegionalScalar) + @RegionalBase AS WeightedRegionalFreight,
        (CAST(E.EmployeeID AS DECIMAL(18,2)) * @IntensityScalar) + @IntensityBase AS CoverageIntensityScore,
        (CAST(O.Freight AS DECIMAL(18,2)) * @CapacityScalar) + @CapacityBase AS TerritoryCapacityIndex
    FROM Employees E
    INNER JOIN EmployeeTerritories ET ON E.EmployeeID = ET.EmployeeID
    INNER JOIN Territories T ON ET.TerritoryID = T.TerritoryID
    INNER JOIN Orders O ON E.EmployeeID = O.EmployeeID
    WHERE T.RegionID IN (1, 3) AND O.Freight > 30.00; -- Selection (Region 1: North, 3: South)

OPEN CoverageCursor;
FETCH NEXT FROM CoverageCursor INTO 
    @v_EmployeeID, @v_TerritoryID, @v_OrderID, @v_LastName, @v_WeightedRegionalFreight, @v_CoverageIntensityScore, @v_TerritoryCapacityIndex;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextCoverageAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_TerritorySalesCoverageLedger (
        CoverageAuditID, EmployeeID, TerritoryID, OrderID, LastName, WeightedRegionalFreight, CoverageIntensityScore, TerritoryCapacityIndex
    )
    VALUES (
        @nextCoverageAuditID, @v_EmployeeID, @v_TerritoryID, @v_OrderID, @v_LastName, @v_WeightedRegionalFreight, @v_CoverageIntensityScore, @v_TerritoryCapacityIndex
    );

    -- Log Triple-Source Lineage
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_TerritorySalesCoverageLedger', 'CoverageAuditID', CAST(@nextCoverageAuditID AS VARCHAR));
    
    -- Record source for Territories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', CAST(@v_TerritoryID AS VARCHAR), 'Table_TerritorySalesCoverageLedger', 'CoverageAuditID', CAST(@nextCoverageAuditID AS VARCHAR));

    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_TerritorySalesCoverageLedger', 'CoverageAuditID', CAST(@nextCoverageAuditID AS VARCHAR));
    
    FETCH NEXT FROM CoverageCursor INTO 
        @v_EmployeeID, @v_TerritoryID, @v_OrderID, @v_LastName, @v_WeightedRegionalFreight, @v_CoverageIntensityScore, @v_TerritoryCapacityIndex;
END;

CLOSE CoverageCursor; 
DEALLOCATE CoverageCursor;
GO