-- Section: Create a Physical Table combining native columns using UNION with Strict Projection
-- Scenario: Compiling a Unified Telecommunications Ledger for automated emergency dialing systems.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection. No Joins, zero data manipulation (native INT and NVARCHAR(24) alignment).
IF OBJECT_ID('Table_UnifiedTelecomLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedTelecomLedger;
CREATE TABLE Table_UnifiedTelecomLedger (
    TelecomLineID INT,
    EntityClassification VARCHAR(20),
    SystemEntityID INT,           -- Natively matches EmployeeID, ShipperID, and SupplierID
    RegisteredNumber NVARCHAR(24) -- Natively matches HomePhone and Phone
);
GO

DECLARE @v_EntityClassification VARCHAR(20),
        @v_SystemEntityID INT,
        @v_RegisteredNumber NVARCHAR(24),
        @nextTelecomLineID INT;

-- Cursor using UNION ALL for strict projection across three distinct operational domains.
-- Because EmployeeID/ShipperID/SupplierID are natively INT, and HomePhone/Phone are 
-- natively NVARCHAR(24), this query extracts the exact data with absolutely zero casting.
DECLARE TelecomLedgerCursor CURSOR FOR 
    SELECT 
        'InternalStaff' AS EntityClassification, 
        EmployeeID AS SystemEntityID, 
        HomePhone AS RegisteredNumber 
    FROM Employees
    UNION ALL
    SELECT 
        'LogisticsPartner' AS EntityClassification, 
        ShipperID AS SystemEntityID, 
        Phone AS RegisteredNumber 
    FROM Shippers
    UNION ALL
    SELECT 
        'ProcurementSource' AS EntityClassification, 
        SupplierID AS SystemEntityID, 
        Phone AS RegisteredNumber 
    FROM Suppliers;

OPEN TelecomLedgerCursor;
FETCH NEXT FROM TelecomLedgerCursor INTO @v_EntityClassification, @v_SystemEntityID, @v_RegisteredNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextTelecomLineID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedTelecomLedger (TelecomLineID, EntityClassification, SystemEntityID, RegisteredNumber)
    VALUES (@nextTelecomLineID, @v_EntityClassification, @v_SystemEntityID, @v_RegisteredNumber);

    IF @v_EntityClassification = 'InternalStaff'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', CAST(@v_SystemEntityID AS VARCHAR), 'Table_UnifiedTelecomLedger', 'TelecomLineID', CAST(@nextTelecomLineID AS VARCHAR));
    END
    ELSE IF @v_EntityClassification = 'LogisticsPartner'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', CAST(@v_SystemEntityID AS VARCHAR), 'Table_UnifiedTelecomLedger', 'TelecomLineID', CAST(@nextTelecomLineID AS VARCHAR));
    END
    ELSE IF @v_EntityClassification = 'ProcurementSource'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', CAST(@v_SystemEntityID AS VARCHAR), 'Table_UnifiedTelecomLedger', 'TelecomLineID', CAST(@nextTelecomLineID AS VARCHAR));
    END
    
    FETCH NEXT FROM TelecomLedgerCursor INTO @v_EntityClassification, @v_SystemEntityID, @v_RegisteredNumber;
END;

CLOSE TelecomLedgerCursor; 
DEALLOCATE TelecomLedgerCursor;
GO