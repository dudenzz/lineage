-- Section: Create a Physical Table combining fax numbers using UNION with Strict Projection
-- Scenario: Compiling a Global Fax Broadcast List to send a mandatory legal compliance notice to all corporate partners.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no data formatting or string manipulation.
IF OBJECT_ID('Table_FaxBroadcastList', 'U') IS NOT NULL DROP TABLE Table_FaxBroadcastList;
CREATE TABLE Table_FaxBroadcastList (
    BroadcastID INT,
    PartnerType VARCHAR(20),
    OriginalPartnerID NVARCHAR(15),
    FaxNumber NVARCHAR(24)
);
GO

DECLARE @v_PartnerType VARCHAR(20),
        @v_OriginalPartnerID NVARCHAR(15),
        @v_FaxNumber NVARCHAR(24),
        @nextBroadcastID INT;

-- Cursor using UNION ALL for strict projection across two distinct corporate entity tables.
-- Selecting exact, existing columns only. CAST is strictly used for the structural 
-- data-type alignment required by the UNION operator, preserving the raw underlying data.
DECLARE FaxBroadcastCursor CURSOR FOR 
    SELECT 
        'Customer' AS PartnerType, 
        CAST(CustomerID AS NVARCHAR(15)) AS OriginalPartnerID, 
        Fax AS FaxNumber 
    FROM Customers
    UNION ALL
    SELECT 
        'Supplier' AS PartnerType, 
        CAST(SupplierID AS NVARCHAR(15)) AS OriginalPartnerID, 
        Fax AS FaxNumber 
    FROM Suppliers;

OPEN FaxBroadcastCursor;
FETCH NEXT FROM FaxBroadcastCursor INTO @v_PartnerType, @v_OriginalPartnerID, @v_FaxNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextBroadcastID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected fax record
    -- Note: NULL fax numbers are inserted exactly as they exist in the source, preserving raw data states
    INSERT INTO Table_FaxBroadcastList (BroadcastID, PartnerType, OriginalPartnerID, FaxNumber)
    VALUES (@nextBroadcastID, @v_PartnerType, @v_OriginalPartnerID, @v_FaxNumber);

    -- Conditionally log Row-Level Lineage based on which table the fax number originated from
    IF @v_PartnerType = 'Customer'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_OriginalPartnerID, 'Table_FaxBroadcastList', 'BroadcastID', CAST(@nextBroadcastID AS VARCHAR));
    END
    ELSE IF @v_PartnerType = 'Supplier'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_OriginalPartnerID, 'Table_FaxBroadcastList', 'BroadcastID', CAST(@nextBroadcastID AS VARCHAR));
    END
    
    FETCH NEXT FROM FaxBroadcastCursor INTO @v_PartnerType, @v_OriginalPartnerID, @v_FaxNumber;
END;

CLOSE FaxBroadcastCursor; 
DEALLOCATE FaxBroadcastCursor;
GO