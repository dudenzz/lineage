-- Section: Create a Physical Table combining postal data using UNION with Strict Projection
-- Scenario: Compiling a Global Postal Code Density Index for courier routing and rate negotiation analysis.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no data formatting.
IF OBJECT_ID('Table_PostalCodeDensityIndex', 'U') IS NOT NULL DROP TABLE Table_PostalCodeDensityIndex;
CREATE TABLE Table_PostalCodeDensityIndex (
    DensityID INT,
    ReferenceType VARCHAR(20),
    OriginalRecordID NVARCHAR(15),
    PostalCode NVARCHAR(15)
);
GO

DECLARE @v_ReferenceType VARCHAR(20),
        @v_OriginalRecordID NVARCHAR(15),
        @v_PostalCode NVARCHAR(15),
        @nextDensityID INT;

-- Cursor using UNION ALL for strict projection across four distinct tables.
-- Selecting exact, existing columns only. CAST is used solely for the structural 
-- data-type alignment required by the UNION operator, preserving the raw data.
DECLARE PostalDensityCursor CURSOR FOR 
    SELECT 
        'Customer' AS ReferenceType, 
        CAST(CustomerID AS NVARCHAR(15)) AS OriginalRecordID, 
        PostalCode 
    FROM Customers
    UNION ALL
    SELECT 
        'Supplier' AS ReferenceType, 
        CAST(SupplierID AS NVARCHAR(15)) AS OriginalRecordID, 
        PostalCode 
    FROM Suppliers
    UNION ALL
    SELECT 
        'Employee' AS ReferenceType, 
        CAST(EmployeeID AS NVARCHAR(15)) AS OriginalRecordID, 
        PostalCode 
    FROM Employees
    UNION ALL
    SELECT 
        'Order' AS ReferenceType, 
        CAST(OrderID AS NVARCHAR(15)) AS OriginalRecordID, 
        ShipPostalCode AS PostalCode 
    FROM Orders;

OPEN PostalDensityCursor;
FETCH NEXT FROM PostalDensityCursor INTO @v_ReferenceType, @v_OriginalRecordID, @v_PostalCode;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextDensityID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected postal record
    INSERT INTO Table_PostalCodeDensityIndex (DensityID, ReferenceType, OriginalRecordID, PostalCode)
    VALUES (@nextDensityID, @v_ReferenceType, @v_OriginalRecordID, @v_PostalCode);

    -- Conditionally log Row-Level Lineage based on which table the postal code originated from
    IF @v_ReferenceType = 'Customer'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_OriginalRecordID, 'Table_PostalCodeDensityIndex', 'DensityID', CAST(@nextDensityID AS VARCHAR));
    END
    ELSE IF @v_ReferenceType = 'Supplier'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_OriginalRecordID, 'Table_PostalCodeDensityIndex', 'DensityID', CAST(@nextDensityID AS VARCHAR));
    END
    ELSE IF @v_ReferenceType = 'Employee'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_OriginalRecordID, 'Table_PostalCodeDensityIndex', 'DensityID', CAST(@nextDensityID AS VARCHAR));
    END
    ELSE IF @v_ReferenceType = 'Order'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', @v_OriginalRecordID, 'Table_PostalCodeDensityIndex', 'DensityID', CAST(@nextDensityID AS VARCHAR));
    END
    
    FETCH NEXT FROM PostalDensityCursor INTO @v_ReferenceType, @v_OriginalRecordID, @v_PostalCode;
END;

CLOSE PostalDensityCursor; 
DEALLOCATE PostalDensityCursor;
GO