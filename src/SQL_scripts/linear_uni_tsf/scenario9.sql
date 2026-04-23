-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Operational Risk & Reliability Ledger.
-- Rule: UNION ALL between different tables (Shippers and Products), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Quantifying "Reliability Scores" for logistical partners and "Criticality Ratings" for low-stock items.

IF OBJECT_ID('Table_OperationalReliabilityLedger', 'U') IS NOT NULL DROP TABLE Table_OperationalReliabilityLedger;
CREATE TABLE Table_OperationalReliabilityLedger (
    ReliabilityID INT,
    AuditContext VARCHAR(25),
    SourcePKID INT,
    EntityName NVARCHAR(40),
    CalculatedScore DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_AuditContext VARCHAR(25),
        @v_SourcePKID INT,
        @v_EntityName NVARCHAR(40),
        @v_CalculatedScore DECIMAL(18, 2),
        @nextReliabilityID INT;

-- Linear Transformation Constants:
-- 1. Logistics (Shippers): Reliability is a flat baseline for established partners (y = 0 * x + 95.00)
-- 2. Inventory (Products): Criticality increases linearly as ReorderLevel rises (y = 1.25 * ReorderLevel + 10.00)
DECLARE @LogisticsBaselineScore DECIMAL(10,2) = 95.00;
DECLARE @InventoryCriticalityScalar DECIMAL(10,2) = 1.25;
DECLARE @InventoryBaseCriticality DECIMAL(10,2) = 10.00;

-- Cursor combining DIFFERENT tables (Shippers and Products) via UNION ALL
DECLARE ReliabilityCursor CURSOR FOR 
    -- Branch 1: Shippers (Logistics Reliability)
    -- Selection: All shippers (Core infrastructure audit)
    -- Transformation: Constant linear projection (y = 0 * x + 95.00)
    SELECT 
        'LogisticsReliability' AS AuditContext, 
        ShipperID AS SourcePKID, 
        CompanyName AS EntityName, 
        @LogisticsBaselineScore AS CalculatedScore 
    FROM Shippers
    
    UNION ALL

    -- Branch 2: Products (Supply Chain Criticality)
    -- Selection: Only products with a ReorderLevel greater than 20 (High-churn items)
    -- Transformation: Linear criticality scaling (y = 1.25 * x + 10.00)
    SELECT 
        'SupplyCriticality' AS AuditContext, 
        ProductID AS SourcePKID, 
        ProductName AS EntityName, 
        (CAST(ReorderLevel AS DECIMAL(18,2)) * @InventoryCriticalityScalar) + @InventoryBaseCriticality AS CalculatedScore 
    FROM Products
    WHERE ReorderLevel > 20; -- Selection

OPEN ReliabilityCursor;
FETCH NEXT FROM ReliabilityCursor INTO @v_AuditContext, @v_SourcePKID, @v_EntityName, @v_CalculatedScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextReliabilityID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_OperationalReliabilityLedger (ReliabilityID, AuditContext, SourcePKID, EntityName, CalculatedScore)
    VALUES (@nextReliabilityID, @v_AuditContext, @v_SourcePKID, @v_EntityName, @v_CalculatedScore);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AuditContext = 'LogisticsReliability'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', CAST(@v_SourcePKID AS VARCHAR), 'Table_OperationalReliabilityLedger', 'ReliabilityID', CAST(@nextReliabilityID AS VARCHAR));
    END
    ELSE IF @v_AuditContext = 'SupplyCriticality'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_SourcePKID AS VARCHAR), 'Table_OperationalReliabilityLedger', 'ReliabilityID', CAST(@nextReliabilityID AS VARCHAR));
    END
    
    FETCH NEXT FROM ReliabilityCursor INTO @v_AuditContext, @v_SourcePKID, @v_EntityName, @v_CalculatedScore;
END;

CLOSE ReliabilityCursor; 
DEALLOCATE ReliabilityCursor;
GO