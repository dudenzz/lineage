-- Section: Create a Physical Table combining textual data using UNION with Strict Projection
-- Scenario: Building an Enterprise Text Corpus to feed into a full-text search indexing tool.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no string manipulations.
IF OBJECT_ID('Table_EnterpriseTextCorpus', 'U') IS NOT NULL DROP TABLE Table_EnterpriseTextCorpus;
CREATE TABLE Table_EnterpriseTextCorpus (
    CorpusID INT,
    SourceDomain VARCHAR(20),
    OriginalRecordID NVARCHAR(10),
    TextContent NVARCHAR(MAX) 
);
GO

DECLARE @v_SourceDomain VARCHAR(20),
        @v_OriginalRecordID NVARCHAR(10),
        @v_TextContent NVARCHAR(MAX),
        @nextCorpusID INT;

-- Cursor using UNION ALL for strict projection across two distinct tables.
-- We are selecting existing long-form text columns. 
-- CAST is used exclusively for structural data-type alignment required by the UNION operator, 
-- but no data transformation, concatenation, or logical mutation is applied to the text itself.
DECLARE TextCorpusCursor CURSOR FOR 
    SELECT 
        'Category' AS SourceDomain, 
        CAST(CategoryID AS NVARCHAR(10)) AS OriginalRecordID, 
        CAST(Description AS NVARCHAR(MAX)) AS TextContent
    FROM Categories
    UNION ALL
    SELECT 
        'Employee' AS SourceDomain, 
        CAST(EmployeeID AS NVARCHAR(10)) AS OriginalRecordID, 
        CAST(Notes AS NVARCHAR(MAX)) AS TextContent
    FROM Employees;

OPEN TextCorpusCursor;
FETCH NEXT FROM TextCorpusCursor INTO @v_SourceDomain, @v_OriginalRecordID, @v_TextContent;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextCorpusID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the projected text record
    INSERT INTO Table_EnterpriseTextCorpus (CorpusID, SourceDomain, OriginalRecordID, TextContent)
    VALUES (@nextCorpusID, @v_SourceDomain, @v_OriginalRecordID, @v_TextContent);

    -- Conditionally log Row-Level Lineage based on which table the descriptive text originated from
    IF @v_SourceDomain = 'Category'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Categories', 'CategoryID', @v_OriginalRecordID, 'Table_EnterpriseTextCorpus', 'CorpusID', CAST(@nextCorpusID AS VARCHAR));
    END
    ELSE IF @v_SourceDomain = 'Employee'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_OriginalRecordID, 'Table_EnterpriseTextCorpus', 'CorpusID', CAST(@nextCorpusID AS VARCHAR));
    END
    
    FETCH NEXT FROM TextCorpusCursor INTO @v_SourceDomain, @v_OriginalRecordID, @v_TextContent;
END;

CLOSE TextCorpusCursor; 
DEALLOCATE TextCorpusCursor;
GO