-- Section: Create a unified physical table using UNION from multiple sources
-- Scenario: Building a Global Contact Directory combining Customers and Suppliers.
-- Rule: Direct data copying from multiple tables using UNION ALL, no transformations.
IF OBJECT_ID('Table_GlobalContactDirectory', 'U') IS NOT NULL DROP TABLE Table_GlobalContactDirectory;
CREATE TABLE Table_GlobalContactDirectory (
    DirectoryID INT, 
    SourceType VARCHAR(20), -- Used to identify the origin of the record
    OriginalID NVARCHAR(10), -- Accommodates both NCHAR(5) CustomerID and INT SupplierID
    CompanyName NVARCHAR(40),
    ContactName NVARCHAR(30),
    City NVARCHAR(15),
    Country NVARCHAR(15)
);
GO

-- Log Row-Level Lineage for the UNION operation
DECLARE @v_SourceType VARCHAR(20), 
        @v_OriginalID NVARCHAR(10), 
        @v_CompanyName NVARCHAR(40), 
        @v_ContactName NVARCHAR(30), 
        @v_City NVARCHAR(15), 
        @v_Country NVARCHAR(15), 
        @nextDirectoryID INT;

-- Use UNION ALL to combine the inputs without applying any data transformations
DECLARE UnionCursor CURSOR FOR 
    SELECT 
        'Customer' AS SourceType, 
        CAST(CustomerID AS NVARCHAR(10)) AS OriginalID, 
        CompanyName, 
        ContactName, 
        City, 
        Country 
    FROM Customers
    UNION ALL
    SELECT 
        'Supplier' AS SourceType, 
        CAST(SupplierID AS NVARCHAR(10)) AS OriginalID, 
        CompanyName, 
        ContactName, 
        City, 
        Country 
    FROM Suppliers;

OPEN UnionCursor;
FETCH NEXT FROM UnionCursor INTO @v_SourceType, @v_OriginalID, @v_CompanyName, @v_ContactName, @v_City, @v_Country;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextDirectoryID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the combined record into the target table
    INSERT INTO Table_GlobalContactDirectory (DirectoryID, SourceType, OriginalID, CompanyName, ContactName, City, Country)
    VALUES (@nextDirectoryID, @v_SourceType, @v_OriginalID, @v_CompanyName, @v_ContactName, @v_City, @v_Country);

    -- Route the lineage tracking based on the origin of the row
    IF @v_SourceType = 'Customer'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_OriginalID, 'Table_GlobalContactDirectory', 'DirectoryID', CAST(@nextDirectoryID AS VARCHAR));
    END
    ELSE IF @v_SourceType = 'Supplier'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_OriginalID, 'Table_GlobalContactDirectory', 'DirectoryID', CAST(@nextDirectoryID AS VARCHAR));
    END
    
    FETCH NEXT FROM UnionCursor INTO @v_SourceType, @v_OriginalID, @v_CompanyName, @v_ContactName, @v_City, @v_Country;
END;

CLOSE UnionCursor; 
DEALLOCATE UnionCursor;
GO