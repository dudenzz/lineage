-- Section: Create a Physical Table combining natively aligned columns using UNION with Strict Projection
-- Scenario: Building a Predictive Text Dictionary for an application's autocomplete engine.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection. No Joins, zero data manipulation (native INT and NVARCHAR(40) alignment).
IF OBJECT_ID('Table_PredictiveTextDictionary', 'U') IS NOT NULL DROP TABLE Table_PredictiveTextDictionary;
CREATE TABLE Table_PredictiveTextDictionary (
    DictionaryID INT,
    SourceDomain VARCHAR(20),
    EntityID INT,            -- Natively matches ProductID, SupplierID, and ShipperID
    EntityName NVARCHAR(40)  -- Natively matches ProductName and CompanyName
);
GO

DECLARE @v_SourceDomain VARCHAR(20),
        @v_EntityID INT,
        @v_EntityName NVARCHAR(40),
        @nextDictionaryID INT;

-- Cursor using UNION ALL for strict projection across three distinct tables.
-- Because ProductID/SupplierID/ShipperID are all natively INT, and 
-- ProductName/CompanyName are all natively NVARCHAR(40), this requires absolutely ZERO data type casting.
DECLARE DictionaryCursor CURSOR FOR 
    SELECT 
        'Product' AS SourceDomain, 
        ProductID AS EntityID, 
        ProductName AS EntityName 
    FROM Products
    UNION ALL
    SELECT 
        'Supplier' AS SourceDomain, 
        SupplierID AS EntityID, 
        CompanyName AS EntityName 
    FROM Suppliers
    UNION ALL
    SELECT 
        'Shipper' AS SourceDomain, 
        ShipperID AS EntityID, 
        CompanyName AS EntityName 
    FROM Shippers;

OPEN DictionaryCursor;
FETCH NEXT FROM DictionaryCursor INTO @v_SourceDomain, @v_EntityID, @v_EntityName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextDictionaryID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the purely projected textual record
    INSERT INTO Table_PredictiveTextDictionary (DictionaryID, SourceDomain, EntityID, EntityName)
    VALUES (@nextDictionaryID, @v_SourceDomain, @v_EntityID, @v_EntityName);

    -- Conditionally log Row-Level Lineage based on which table the dictionary term originated from
    -- (CAST is used here exclusively to meet the DataLineage table's metadata storage requirements, 
    -- not modifying the actual payload data passing through the pipeline)
    IF @v_SourceDomain = 'Product'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_EntityID AS VARCHAR), 'Table_PredictiveTextDictionary', 'DictionaryID', CAST(@nextDictionaryID AS VARCHAR));
    END
    ELSE IF @v_SourceDomain = 'Supplier'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', CAST(@v_EntityID AS VARCHAR), 'Table_PredictiveTextDictionary', 'DictionaryID', CAST(@nextDictionaryID AS VARCHAR));
    END
    ELSE IF @v_SourceDomain = 'Shipper'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', CAST(@v_EntityID AS VARCHAR), 'Table_PredictiveTextDictionary', 'DictionaryID', CAST(@nextDictionaryID AS VARCHAR));
    END
    
    FETCH NEXT FROM DictionaryCursor INTO @v_SourceDomain, @v_EntityID, @v_EntityName;
END;

CLOSE DictionaryCursor; 
DEALLOCATE DictionaryCursor;
GO