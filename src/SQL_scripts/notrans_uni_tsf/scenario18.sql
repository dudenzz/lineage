-- Section: Create a Physical Table combining native columns using UNION with Strict Projection
-- Scenario: Compiling an Internal Operations City Ledger for regional tax zoning analysis.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection. No Joins, zero data manipulation (native INT and NVARCHAR alignment).
IF OBJECT_ID('Table_OperationsCityLedger', 'U') IS NOT NULL DROP TABLE Table_OperationsCityLedger;
CREATE TABLE Table_OperationsCityLedger (
    LedgerLineID INT,
    OperationType VARCHAR(20),
    EntityIdentifier INT,      -- Natively matches SupplierID and EmployeeID
    OperationCity NVARCHAR(15) -- Natively matches City in both tables
);
GO

DECLARE @v_OperationType VARCHAR(20),
        @v_EntityIdentifier INT,
        @v_OperationCity NVARCHAR(15),
        @nextLedgerLineID INT;

-- Cursor using UNION ALL for strict projection across two distinct operational domains.
-- Because SupplierID/EmployeeID are natively INT, and City is natively NVARCHAR(15) 
-- in both tables, this query extracts the exact data with absolutely zero casting or manipulation.
DECLARE CityLedgerCursor CURSOR FOR 
    SELECT 
        'SupplierNode' AS OperationType, 
        SupplierID AS EntityIdentifier, 
        City AS OperationCity 
    FROM Suppliers
    UNION ALL
    SELECT 
        'EmployeeNode' AS OperationType, 
        EmployeeID AS EntityIdentifier, 
        City AS OperationCity 
    FROM Employees;

OPEN CityLedgerCursor;
FETCH NEXT FROM CityLedgerCursor INTO @v_OperationType, @v_EntityIdentifier, @v_OperationCity;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLedgerLineID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_OperationsCityLedger (LedgerLineID, OperationType, EntityIdentifier, OperationCity)
    VALUES (@nextLedgerLineID, @v_OperationType, @v_EntityIdentifier, @v_OperationCity);

    IF @v_OperationType = 'SupplierNode'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', CAST(@v_EntityIdentifier AS VARCHAR), 'Table_OperationsCityLedger', 'LedgerLineID', CAST(@nextLedgerLineID AS VARCHAR));
    END
    ELSE IF @v_OperationType = 'EmployeeNode'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', CAST(@v_EntityIdentifier AS VARCHAR), 'Table_OperationsCityLedger', 'LedgerLineID', CAST(@nextLedgerLineID AS VARCHAR));
    END
    
    FETCH NEXT FROM CityLedgerCursor INTO @v_OperationType, @v_EntityIdentifier, @v_OperationCity;
END;

CLOSE CityLedgerCursor; 
DEALLOCATE CityLedgerCursor;
GO