-- Section: Create a Physical Table combining multiple address sources using UNION with Strict Projection
-- Scenario: Centralizing a Global Address Registry to map all physical locations (Employee homes, Customer HQs, Order Ship-To addresses).
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no data transformations.
IF OBJECT_ID('Table_GlobalAddressRegistry', 'U') IS NOT NULL DROP TABLE Table_GlobalAddressRegistry;
CREATE TABLE Table_GlobalAddressRegistry (
    RegistryID INT,
    EntityRole VARCHAR(20),
    OriginalEntityID NVARCHAR(15),
    StreetAddress NVARCHAR(60),
    CityName NVARCHAR(15),
    PostalCode NVARCHAR(10),
    CountryName NVARCHAR(15)
);
GO

DECLARE @v_EntityRole VARCHAR(20),
        @v_OriginalEntityID NVARCHAR(15),
        @v_StreetAddress NVARCHAR(60),
        @v_CityName NVARCHAR(15),
        @v_PostalCode NVARCHAR(10),
        @v_CountryName NVARCHAR(15),
        @nextRegistryID INT;

-- Cursor using UNION ALL for strict projection across three distinct tables.
-- Selecting only exact, existing columns. CAST is used solely for structural type alignment 
-- required by the UNION operator, ensuring no underlying data is mathematically or logically transformed.
DECLARE AddressRegistryCursor CURSOR FOR 
    SELECT 
        'CustomerHQ' AS EntityRole, 
        CAST(CustomerID AS NVARCHAR(15)) AS OriginalEntityID, 
        Address AS StreetAddress, 
        City AS CityName, 
        PostalCode, 
        Country AS CountryName
    FROM Customers
    UNION ALL
    SELECT 
        'EmployeeHome' AS EntityRole, 
        CAST(EmployeeID AS NVARCHAR(15)) AS OriginalEntityID, 
        Address AS StreetAddress, 
        City AS CityName, 
        PostalCode, 
        Country AS CountryName
    FROM Employees
    UNION ALL
    SELECT 
        'OrderDestination' AS EntityRole, 
        CAST(OrderID AS NVARCHAR(15)) AS OriginalEntityID, 
        ShipAddress AS StreetAddress, 
        ShipCity AS CityName, 
        ShipPostalCode AS PostalCode, 
        ShipCountry AS CountryName
    FROM Orders;

OPEN AddressRegistryCursor;
FETCH NEXT FROM AddressRegistryCursor INTO @v_EntityRole, @v_OriginalEntityID, @v_StreetAddress, @v_CityName, @v_PostalCode, @v_CountryName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextRegistryID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected address record
    INSERT INTO Table_GlobalAddressRegistry (RegistryID, EntityRole, OriginalEntityID, StreetAddress, CityName, PostalCode, CountryName)
    VALUES (@nextRegistryID, @v_EntityRole, @v_OriginalEntityID, @v_StreetAddress, @v_CityName, @v_PostalCode, @v_CountryName);

    -- Conditionally log Row-Level Lineage based on which table the address originated from
    IF @v_EntityRole = 'CustomerHQ'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_OriginalEntityID, 'Table_GlobalAddressRegistry', 'RegistryID', CAST(@nextRegistryID AS VARCHAR));
    END
    ELSE IF @v_EntityRole = 'EmployeeHome'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_OriginalEntityID, 'Table_GlobalAddressRegistry', 'RegistryID', CAST(@nextRegistryID AS VARCHAR));
    END
    ELSE IF @v_EntityRole = 'OrderDestination'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', @v_OriginalEntityID, 'Table_GlobalAddressRegistry', 'RegistryID', CAST(@nextRegistryID AS VARCHAR));
    END
    
    FETCH NEXT FROM AddressRegistryCursor INTO @v_EntityRole, @v_OriginalEntityID, @v_StreetAddress, @v_CityName, @v_PostalCode, @v_CountryName;
END;

CLOSE AddressRegistryCursor; 
DEALLOCATE AddressRegistryCursor;
GO