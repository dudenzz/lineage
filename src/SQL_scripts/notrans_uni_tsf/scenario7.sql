-- Section: Create a Physical Table combining multiple distinct entities using UNION with Strict Projection
-- Scenario: Building a Global Search Autocomplete Index for a unified application search bar.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no string manipulations.
IF OBJECT_ID('Table_GlobalSearchIndex', 'U') IS NOT NULL DROP TABLE Table_GlobalSearchIndex;
CREATE TABLE Table_GlobalSearchIndex (
    SearchID INT,
    EntityDomain VARCHAR(20),
    OriginalEntityID NVARCHAR(15),
    SearchableName NVARCHAR(40)
);
GO

DECLARE @v_EntityDomain VARCHAR(20),
        @v_OriginalEntityID NVARCHAR(15),
        @v_SearchableName NVARCHAR(40),
        @nextSearchID INT;

-- Cursor using UNION ALL for strict projection across three completely distinct tables.
-- Selecting exact, existing columns only. CAST is strictly used for structural 
-- data-type alignment across the UNION, maintaining the raw integrity of the data.
DECLARE SearchIndexCursor CURSOR FOR 
    SELECT 
        'Product' AS EntityDomain, 
        CAST(ProductID AS NVARCHAR(15)) AS OriginalEntityID, 
        ProductName AS SearchableName 
    FROM Products
    UNION ALL
    SELECT 
        'Category' AS EntityDomain, 
        CAST(CategoryID AS NVARCHAR(15)) AS OriginalEntityID, 
        CategoryName AS SearchableName 
    FROM Categories
    UNION ALL
    SELECT 
        'Shipper' AS EntityDomain, 
        CAST(ShipperID AS NVARCHAR(15)) AS OriginalEntityID, 
        CompanyName AS SearchableName 
    FROM Shippers;

OPEN SearchIndexCursor;
FETCH NEXT FROM SearchIndexCursor INTO @v_EntityDomain, @v_OriginalEntityID, @v_SearchableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextSearchID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected search record
    INSERT INTO Table_GlobalSearchIndex (SearchID, EntityDomain, OriginalEntityID, SearchableName)
    VALUES (@nextSearchID, @v_EntityDomain, @v_OriginalEntityID, @v_SearchableName);

    -- Conditionally log Row-Level Lineage based on which table the search term originated from
    IF @v_EntityDomain = 'Product'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', @v_OriginalEntityID, 'Table_GlobalSearchIndex', 'SearchID', CAST(@nextSearchID AS VARCHAR));
    END
    ELSE IF @v_EntityDomain = 'Category'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Categories', 'CategoryID', @v_OriginalEntityID, 'Table_GlobalSearchIndex', 'SearchID', CAST(@nextSearchID AS VARCHAR));
    END
    ELSE IF @v_EntityDomain = 'Shipper'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', @v_OriginalEntityID, 'Table_GlobalSearchIndex', 'SearchID', CAST(@nextSearchID AS VARCHAR));
    END
    
    FETCH NEXT FROM SearchIndexCursor INTO @v_EntityDomain, @v_OriginalEntityID, @v_SearchableName;
END;

CLOSE SearchIndexCursor; 
DEALLOCATE SearchIndexCursor;
GO