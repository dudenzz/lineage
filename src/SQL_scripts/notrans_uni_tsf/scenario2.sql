-- Section: Create a Physical Table combining multiple sources using UNION with Strict Projection
-- Scenario: Compiling a unified master list of all geographical locations (Cities, Regions, Countries) associated with the business.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no data transformations.
IF OBJECT_ID('Table_GlobalLocations', 'U') IS NOT NULL DROP TABLE Table_GlobalLocations;
CREATE TABLE Table_GlobalLocations (
    LocationID INT,
    SourceTable VARCHAR(20),
    OriginalID NVARCHAR(15), 
    City NVARCHAR(15),
    Region NVARCHAR(15),
    PostalCode NVARCHAR(10),
    Country NVARCHAR(15)
);
GO

DECLARE @v_SourceTable VARCHAR(20), 
        @v_OriginalID NVARCHAR(15), 
        @v_City NVARCHAR(15), 
        @v_Region NVARCHAR(15), 
        @v_PostalCode NVARCHAR(10),
        @v_Country NVARCHAR(15), 
        @nextLocationID INT;

-- Cursor using UNION ALL for strict projection across three different tables.
-- Note: CAST is used solely for structural type-matching across the UNION, which is standard SQL requirement, but no data transformation/manipulation occurs.
DECLARE UnionLocationCursor CURSOR FOR 
    SELECT 
        'Customers' AS SourceTable, 
        CAST(CustomerID AS NVARCHAR(15)) AS OriginalID, 
        City, 
        Region, 
        PostalCode, 
        Country 
    FROM Customers
    UNION ALL
    SELECT 
        'Suppliers' AS SourceTable, 
        CAST(SupplierID AS NVARCHAR(15)) AS OriginalID, 
        City, 
        Region, 
        PostalCode, 
        Country 
    FROM Suppliers
    UNION ALL
    SELECT 
        'Employees' AS SourceTable, 
        CAST(EmployeeID AS NVARCHAR(15)) AS OriginalID, 
        City, 
        Region, 
        PostalCode, 
        Country 
    FROM Employees;

OPEN UnionLocationCursor;
FETCH NEXT FROM UnionLocationCursor INTO @v_SourceTable, @v_OriginalID, @v_City, @v_Region, @v_PostalCode, @v_Country;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextLocationID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the projected record
    INSERT INTO Table_GlobalLocations (LocationID, SourceTable, OriginalID, City, Region, PostalCode, Country)
    VALUES (@nextLocationID, @v_SourceTable, @v_OriginalID, @v_City, @v_Region, @v_PostalCode, @v_Country);

    -- Conditionally log Row-Level Lineage based on which table the UNION pulled this specific row from
    IF @v_SourceTable = 'Customers'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_OriginalID, 'Table_GlobalLocations', 'LocationID', CAST(@nextLocationID AS VARCHAR));
    END
    ELSE IF @v_SourceTable = 'Suppliers'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_OriginalID, 'Table_GlobalLocations', 'LocationID', CAST(@nextLocationID AS VARCHAR));
    END
    ELSE IF @v_SourceTable = 'Employees'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_OriginalID, 'Table_GlobalLocations', 'LocationID', CAST(@nextLocationID AS VARCHAR));
    END
    
    FETCH NEXT FROM UnionLocationCursor INTO @v_SourceTable, @v_OriginalID, @v_City, @v_Region, @v_PostalCode, @v_Country;
END;

CLOSE UnionLocationCursor; 
DEALLOCATE UnionLocationCursor;
GO