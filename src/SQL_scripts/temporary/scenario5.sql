-- Section 1: Dynamic Data Extraction
-- Tests: Ability to parse EXEC/sp_executesql to find hidden 'Used for Creation' predicates.
DECLARE @SourceTableName NVARCHAR(50) = 'Suppliers';
DECLARE @DynamicSQL NVARCHAR(MAX);

-- Creating the temp table structure first
CREATE TABLE #DynamicBuffer (
    ExtractedID INT,
    ExtractedName NVARCHAR(100),
    ExtractionDate DATETIME
);

-- Building a string that moves data from a variable table name into the temp table
SET @DynamicSQL = N'INSERT INTO #DynamicBuffer (ExtractedID, ExtractedName, ExtractionDate) ' +
                  N'SELECT SupplierID, CompanyName, GETDATE() FROM ' + QUOTENAME(@SourceTableName) + 
                  N' WHERE Country = ''UK''';

EXEC sp_executesql @DynamicSQL;

-- Log Lineage: Tool must resolve @SourceTableName to 'Suppliers'
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Suppliers', 'SupplierID', CAST(ExtractedID AS VARCHAR), '#DynamicBuffer', 'ExtractedID', CAST(ExtractedID AS VARCHAR)
FROM #DynamicBuffer;
GO

-- Section 2: Secondary Transformation via Temporary Table
-- Tests: Lineage from a dynamically populated temp table to another temp table.
SELECT 
    ExtractedID, 
    UPPER(ExtractedName) AS NormalizedName 
INTO #CleanedBuffer
FROM #DynamicBuffer;

INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT '#DynamicBuffer', 'ExtractedID', CAST(ExtractedID AS VARCHAR), '#CleanedBuffer', 'ExtractedID', CAST(ExtractedID AS VARCHAR)
FROM #CleanedBuffer;
GO

-- Section 3: Final Persistence to Physical Table
-- Tests: Completing the chain from Dynamic Source -> Temp -> Temp -> Physical.
IF OBJECT_ID('Final_UK_Supplier_Registry', 'U') IS NOT NULL DROP TABLE Final_UK_Supplier_Registry;
CREATE TABLE Final_UK_Supplier_Registry (
    RegistryID INT PRIMARY KEY,
    SupplierName NVARCHAR(100),
    Verified BIT DEFAULT 1
);

DECLARE @name NVARCHAR(100), @sid INT, @regID INT;
DECLARE FinalCursor CURSOR FOR SELECT ExtractedID, NormalizedName FROM #CleanedBuffer;

OPEN FinalCursor;
FETCH NEXT FROM FinalCursor INTO @sid, @name;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @regID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Final_UK_Supplier_Registry (RegistryID, SupplierName)
    VALUES (@regID, @name);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('#CleanedBuffer', 'ExtractedID', CAST(@sid AS VARCHAR), 'Final_UK_Supplier_Registry', 'RegistryID', CAST(@regID AS VARCHAR));

    FETCH NEXT FROM FinalCursor INTO @sid, @name;
END;
CLOSE FinalCursor; DEALLOCATE FinalCursor;

DROP TABLE #DynamicBuffer;
DROP TABLE #CleanedBuffer;
GO