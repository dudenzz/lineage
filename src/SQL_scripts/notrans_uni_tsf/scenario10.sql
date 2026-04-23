-- Section: Create a Physical Table combining corporate entities using UNION with Strict Projection
-- Scenario: Compiling a Universal Company Registry for Master Data Management (MDM) deduplication analysis.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no string manipulations.
IF OBJECT_ID('Table_UniversalCompanyRegistry', 'U') IS NOT NULL DROP TABLE Table_UniversalCompanyRegistry;
CREATE TABLE Table_UniversalCompanyRegistry (
    RegistryID INT,
    SourceSystem VARCHAR(20),
    OriginalID NVARCHAR(15),
    EntityName NVARCHAR(40)
);
GO

DECLARE @v_SourceSystem VARCHAR(20),
        @v_OriginalID NVARCHAR(15),
        @v_EntityName NVARCHAR(40),
        @nextRegistryID INT;

-- Cursor using UNION ALL for strict projection across three distinct entity tables.
-- Selecting exact, existing columns only. CAST is strictly used for the structural 
-- data-type alignment required by the UNION operator, maintaining raw data integrity.
DECLARE CompanyRegistryCursor CURSOR FOR 
    SELECT 
        'Customer' AS SourceSystem, 
        CAST(CustomerID AS NVARCHAR(15)) AS OriginalID, 
        CompanyName AS EntityName 
    FROM Customers
    UNION ALL
    SELECT 
        'Supplier' AS SourceSystem, 
        CAST(SupplierID AS NVARCHAR(15)) AS OriginalID, 
        CompanyName AS EntityName 
    FROM Suppliers
    UNION ALL
    SELECT 
        'Shipper' AS SourceSystem, 
        CAST(ShipperID AS NVARCHAR(15)) AS OriginalID, 
        CompanyName AS EntityName 
    FROM Shippers;

OPEN CompanyRegistryCursor;
FETCH NEXT FROM CompanyRegistryCursor INTO @v_SourceSystem, @v_OriginalID, @v_EntityName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextRegistryID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected company record
    INSERT INTO Table_UniversalCompanyRegistry (RegistryID, SourceSystem, OriginalID, EntityName)
    VALUES (@nextRegistryID, @v_SourceSystem, @v_OriginalID, @v_EntityName);

    -- Conditionally log Row-Level Lineage based on which table the company name originated from
    IF @v_SourceSystem = 'Customer'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_OriginalID, 'Table_UniversalCompanyRegistry', 'RegistryID', CAST(@nextRegistryID AS VARCHAR));
    END
    ELSE IF @v_SourceSystem = 'Supplier'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_OriginalID, 'Table_UniversalCompanyRegistry', 'RegistryID', CAST(@nextRegistryID AS VARCHAR));
    END
    ELSE IF @v_SourceSystem = 'Shipper'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', @v_OriginalID, 'Table_UniversalCompanyRegistry', 'RegistryID', CAST(@nextRegistryID AS VARCHAR));
    END
    
    FETCH NEXT FROM CompanyRegistryCursor INTO @v_SourceSystem, @v_OriginalID, @v_EntityName;
END;

CLOSE CompanyRegistryCursor; 
DEALLOCATE CompanyRegistryCursor;
GO