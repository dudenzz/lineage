-- Section 1: Table-Valued Function for String Splitting
-- (Simulates splitting a comma-separated list of Tags from a Product description)
CREATE OR ALTER FUNCTION dbo.fn_SplitTags(@String NVARCHAR(MAX))
RETURNS TABLE AS RETURN (
    SELECT value AS Tag FROM STRING_SPLIT(@String, ' ')
);
GO

-- Section 2: Materializing the Expanded Tag Cloud
-- Tests: Lineage from a single text column through a function into a new object.
IF OBJECT_ID('Table_Product_Search_Tags', 'U') IS NOT NULL DROP TABLE Table_Product_Search_Tags;

CREATE TABLE Table_Product_Search_Tags (
    ProductID INT,
    TagValue NVARCHAR(50)
);

INSERT INTO Table_Product_Search_Tags (ProductID, TagValue)
SELECT p.ProductID, t.Tag
FROM Products p
CROSS APPLY dbo.fn_SplitTags(p.ProductName) AS t;

-- Log Lineage:
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Products', 'ProductName', 'Function-Expansion', 'Table_Product_Search_Tags', 'ProductID', 'Tag-List'
FROM Table_Product_Search_Tags;
GO