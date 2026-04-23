-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Infrastructure & Distribution Resilience Ledger.
-- Rule: UNION ALL between different tables (Shippers and Products), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Resilience Ratings" for logistics carriers and "Storage Overhead Impacts" for bulk inventory.

IF OBJECT_ID('Table_InfrastructureResilienceLedger', 'U') IS NOT NULL DROP TABLE Table_InfrastructureResilienceLedger;
CREATE TABLE Table_InfrastructureResilienceLedger (
    EntryID INT,
    AssetType VARCHAR(25),
    SourcePKID INT,
    IdentifierLabel NVARCHAR(40),
    ResilienceMetric DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_AssetType VARCHAR(25),
        @v_SourcePKID INT,
        @v_IdentifierLabel NVARCHAR(40),
        @v_ResilienceMetric DECIMAL(18, 2),
        @nextEntryID INT;

-- Linear Transformation Constants:
-- 1. Distribution (Shippers): Resilience score is a fixed baseline for approved partners (y = 0 * x + 88.50)
-- 2. Storage (Products): Impact score scales linearly with ReorderLevel (y = 1.45 * ReorderLevel + 12.00)
DECLARE @LogisticsResilienceBase DECIMAL(10,2) = 88.50;
DECLARE @StorageImpactScalar DECIMAL(10,2) = 1.45;
DECLARE @StorageBaseOffset DECIMAL(10,2) = 12.00;

-- Cursor combining DIFFERENT tables (Shippers and Products) via UNION ALL
DECLARE ResilienceCursor CURSOR FOR 
    -- Branch 1: Shippers (Logistics Network Reliability)
    -- Selection: All available shippers
    -- Transformation: Constant linear projection (y = 0 * x + 88.50)
    SELECT 
        'LogisticsResilience' AS AssetType, 
        ShipperID AS SourcePKID, 
        CompanyName AS IdentifierLabel, 
        @LogisticsResilienceBase AS ResilienceMetric 
    FROM Shippers
    
    UNION ALL

    -- Branch 2: Products (Warehouse Capacity Impact)
    -- Selection: Only products with a high reorder threshold (ReorderLevel > 25)
    -- Transformation: Linear impact scaling (y = 1.45 * x + 12.00)
    SELECT 
        'StorageImpact' AS AssetType, 
        ProductID AS SourcePKID, 
        ProductName AS IdentifierLabel, 
        (CAST(ReorderLevel AS DECIMAL(18,2)) * @StorageImpactScalar) + @StorageBaseOffset AS ResilienceMetric 
    FROM Products
    WHERE ReorderLevel > 25; -- Selection

OPEN ResilienceCursor;
FETCH NEXT FROM ResilienceCursor INTO @v_AssetType, @v_SourcePKID, @v_IdentifierLabel, @v_ResilienceMetric;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextEntryID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_InfrastructureResilienceLedger (EntryID, AssetType, SourcePKID, IdentifierLabel, ResilienceMetric)
    VALUES (@nextEntryID, @v_AssetType, @v_SourcePKID, @v_IdentifierLabel, @v_ResilienceMetric);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AssetType = 'LogisticsResilience'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', CAST(@v_SourcePKID AS VARCHAR), 'Table_InfrastructureResilienceLedger', 'EntryID', CAST(@nextEntryID AS VARCHAR));
    END
    ELSE IF @v_AssetType = 'StorageImpact'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_SourcePKID AS VARCHAR), 'Table_InfrastructureResilienceLedger', 'EntryID', CAST(@nextEntryID AS VARCHAR));
    END
    
    FETCH NEXT FROM ResilienceCursor INTO @v_AssetType, @v_SourcePKID, @v_IdentifierLabel, @v_ResilienceMetric;
END;

CLOSE ResilienceCursor; 
DEALLOCATE ResilienceCursor;
GO