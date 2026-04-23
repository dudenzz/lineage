-- Section: Create a Physical Table combining multiple distinct business entities using UNION with Strict Projection
-- Scenario: Consolidating a master phone directory of all external corporate entities (Customers, Suppliers, Shippers).
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no data transformations.
IF OBJECT_ID('Table_ExternalPhoneDirectory', 'U') IS NOT NULL DROP TABLE Table_ExternalPhoneDirectory;
CREATE TABLE Table_ExternalPhoneDirectory (
    DirectoryID INT,
    EntityType VARCHAR(20),
    SourceEntityID NVARCHAR(15),
    EntityName NVARCHAR(40),
    PhoneNumber NVARCHAR(24)
);
GO

DECLARE @v_EntityType VARCHAR(20),
        @v_SourceEntityID NVARCHAR(15),
        @v_EntityName NVARCHAR(40),
        @v_PhoneNumber NVARCHAR(24),
        @nextDirectoryID INT;

-- Cursor using UNION ALL for strict projection across three distinct entity tables.
-- Notice that we are only selecting existing columns (with aliases for alignment). 
-- No string manipulation or formatting is applied to the phone numbers.
DECLARE PhoneDirCursor CURSOR FOR 
    SELECT 
        'Customer' AS EntityType, 
        CAST(CustomerID AS NVARCHAR(15)) AS SourceEntityID, 
        CompanyName AS EntityName, 
        Phone AS PhoneNumber
    FROM Customers
    UNION ALL
    SELECT 
        'Supplier' AS EntityType, 
        CAST(SupplierID AS NVARCHAR(15)) AS SourceEntityID, 
        CompanyName AS EntityName, 
        Phone AS PhoneNumber
    FROM Suppliers
    UNION ALL
    SELECT 
        'Shipper' AS EntityType, 
        CAST(ShipperID AS NVARCHAR(15)) AS SourceEntityID, 
        CompanyName AS EntityName, 
        Phone AS PhoneNumber
    FROM Shippers;

OPEN PhoneDirCursor;
FETCH NEXT FROM PhoneDirCursor INTO @v_EntityType, @v_SourceEntityID, @v_EntityName, @v_PhoneNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextDirectoryID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the projected record
    INSERT INTO Table_ExternalPhoneDirectory (DirectoryID, EntityType, SourceEntityID, EntityName, PhoneNumber)
    VALUES (@nextDirectoryID, @v_EntityType, @v_SourceEntityID, @v_EntityName, @v_PhoneNumber);

    -- Conditionally log Row-Level Lineage based on the entity type origin of the current row
    IF @v_EntityType = 'Customer'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_SourceEntityID, 'Table_ExternalPhoneDirectory', 'DirectoryID', CAST(@nextDirectoryID AS VARCHAR));
    END
    ELSE IF @v_EntityType = 'Supplier'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_SourceEntityID, 'Table_ExternalPhoneDirectory', 'DirectoryID', CAST(@nextDirectoryID AS VARCHAR));
    END
    ELSE IF @v_EntityType = 'Shipper'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', @v_SourceEntityID, 'Table_ExternalPhoneDirectory', 'DirectoryID', CAST(@nextDirectoryID AS VARCHAR));
    END
    
    FETCH NEXT FROM PhoneDirCursor INTO @v_EntityType, @v_SourceEntityID, @v_EntityName, @v_PhoneNumber;
END;

CLOSE PhoneDirCursor; 
DEALLOCATE PhoneDirCursor;
GO