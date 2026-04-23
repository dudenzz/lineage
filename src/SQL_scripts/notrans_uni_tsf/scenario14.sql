-- Section: Create a Physical Table combining relational foreign keys using UNION with Strict Projection
-- Scenario: Compiling a system-wide Foreign Key Audit Ledger to verify referential integrity across operational tables.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection. No Joins, zero data manipulation (native INT alignment).
IF OBJECT_ID('Table_ForeignKeyAuditLedger', 'U') IS NOT NULL DROP TABLE Table_ForeignKeyAuditLedger;
CREATE TABLE Table_ForeignKeyAuditLedger (
    AuditLineID INT,
    OriginatingTable VARCHAR(20),
    TargetEntityDomain VARCHAR(20),
    OriginatingRecordID INT,
    ExtractedForeignKey INT
);
GO

DECLARE @v_OriginatingTable VARCHAR(20),
        @v_TargetEntityDomain VARCHAR(20),
        @v_OriginatingRecordID INT,
        @v_ExtractedForeignKey INT,
        @nextAuditLineID INT;

-- Cursor using UNION ALL for strict projection across multiple operational tables.
-- Selecting exact, existing INT columns only. Because all source IDs and foreign keys 
-- are integers, this requires absolutely zero data type casting or manipulation.
DECLARE FKAuditCursor CURSOR FOR 
    SELECT 
        'Products' AS OriginatingTable,
        'SupplierTarget' AS TargetEntityDomain, 
        ProductID AS OriginatingRecordID, 
        SupplierID AS ExtractedForeignKey 
    FROM Products
    UNION ALL
    SELECT 
        'Orders' AS OriginatingTable,
        'EmployeeTarget' AS TargetEntityDomain, 
        OrderID AS OriginatingRecordID, 
        EmployeeID AS ExtractedForeignKey 
    FROM Orders
    UNION ALL
    SELECT 
        'Orders' AS OriginatingTable,
        'ShipperTarget' AS TargetEntityDomain, 
        OrderID AS OriginatingRecordID, 
        ShipVia AS ExtractedForeignKey 
    FROM Orders;

OPEN FKAuditCursor;
FETCH NEXT FROM FKAuditCursor INTO @v_OriginatingTable, @v_TargetEntityDomain, @v_OriginatingRecordID, @v_ExtractedForeignKey;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextAuditLineID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected relational record
    -- Note: NULL foreign keys are inserted exactly as they exist, preserving the raw data state
    INSERT INTO Table_ForeignKeyAuditLedger (AuditLineID, OriginatingTable, TargetEntityDomain, OriginatingRecordID, ExtractedForeignKey)
    VALUES (@nextAuditLineID, @v_OriginatingTable, @v_TargetEntityDomain, @v_OriginatingRecordID, @v_ExtractedForeignKey);

    -- Conditionally log Row-Level Lineage based on the exact table the foreign key was extracted from
    IF @v_OriginatingTable = 'Products'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        -- Note: CAST is only used here to fit the standard Lineage table VARCHAR constraints, not modifying the payload data
        VALUES ('Products', 'ProductID', CAST(@v_OriginatingRecordID AS VARCHAR), 'Table_ForeignKeyAuditLedger', 'AuditLineID', CAST(@nextAuditLineID AS VARCHAR));
    END
    ELSE IF @v_OriginatingTable = 'Orders'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', CAST(@v_OriginatingRecordID AS VARCHAR), 'Table_ForeignKeyAuditLedger', 'AuditLineID', CAST(@nextAuditLineID AS VARCHAR));
    END
    
    FETCH NEXT FROM FKAuditCursor INTO @v_OriginatingTable, @v_TargetEntityDomain, @v_OriginatingRecordID, @v_ExtractedForeignKey;
END;

CLOSE FKAuditCursor; 
DEALLOCATE FKAuditCursor;
GO