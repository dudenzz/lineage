-- Section: Create a Physical Table combining binary data using UNION with Strict Projection
-- Scenario: Building a Centralized Asset Staging table to migrate all system images (BLOBs) to a CDN.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no data manipulation.
IF OBJECT_ID('Table_AssetMigrationStaging', 'U') IS NOT NULL DROP TABLE Table_AssetMigrationStaging;
CREATE TABLE Table_AssetMigrationStaging (
    AssetID INT,
    AssetDomain VARCHAR(20),
    OriginalRecordID NVARCHAR(15),
    BinaryPayload IMAGE -- Using legacy IMAGE type to match Northwind's default schema, or VARBINARY(MAX)
);
GO

DECLARE @v_AssetDomain VARCHAR(20),
        @v_OriginalRecordID NVARCHAR(15),
        @v_BinaryPayload VARBINARY(MAX),
        @nextAssetID INT;

-- Cursor using UNION ALL for strict projection across two distinct tables containing binary assets.
-- Selecting exact, existing binary columns only. CAST is strictly used for the structural 
-- data-type alignment of the ID column required by the UNION operator.
DECLARE AssetMigrationCursor CURSOR FOR 
    SELECT 
        'CategoryLogo' AS AssetDomain, 
        CAST(CategoryID AS NVARCHAR(15)) AS OriginalRecordID, 
        Picture AS BinaryPayload 
    FROM Categories
    UNION ALL
    SELECT 
        'EmployeePortrait' AS AssetDomain, 
        CAST(EmployeeID AS NVARCHAR(15)) AS OriginalRecordID, 
        Photo AS BinaryPayload 
    FROM Employees;

OPEN AssetMigrationCursor;
FETCH NEXT FROM AssetMigrationCursor INTO @v_AssetDomain, @v_OriginalRecordID, @v_BinaryPayload;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextAssetID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected binary record
    INSERT INTO Table_AssetMigrationStaging (AssetID, AssetDomain, OriginalRecordID, BinaryPayload)
    VALUES (@nextAssetID, @v_AssetDomain, @v_OriginalRecordID, @v_BinaryPayload);

    -- Conditionally log Row-Level Lineage based on which table the binary asset originated from
    IF @v_AssetDomain = 'CategoryLogo'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Categories', 'CategoryID', @v_OriginalRecordID, 'Table_AssetMigrationStaging', 'AssetID', CAST(@nextAssetID AS VARCHAR));
    END
    ELSE IF @v_AssetDomain = 'EmployeePortrait'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_OriginalRecordID, 'Table_AssetMigrationStaging', 'AssetID', CAST(@nextAssetID AS VARCHAR));
    END
    
    FETCH NEXT FROM AssetMigrationCursor INTO @v_AssetDomain, @v_OriginalRecordID, @v_BinaryPayload;
END;

CLOSE AssetMigrationCursor; 
DEALLOCATE AssetMigrationCursor;
GO