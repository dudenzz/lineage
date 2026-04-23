-- Section: Create a Physical Table combining native monetary columns using UNION with Strict Projection
-- Scenario: Compiling a System-Wide Monetary Audit Ledger for financial precision and currency conversion testing.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection. No Joins, zero data manipulation (native INT and MONEY alignment).
IF OBJECT_ID('Table_MonetaryAuditLedger', 'U') IS NOT NULL DROP TABLE Table_MonetaryAuditLedger;
CREATE TABLE Table_MonetaryAuditLedger (
    LedgerID INT,
    SourceContext VARCHAR(20),
    OriginalEntityID INT,        -- Natively matches both ProductID and OrderID
    RawMonetaryValue MONEY       -- Natively matches both UnitPrice and Freight
);
GO

DECLARE @v_SourceContext VARCHAR(20),
        @v_OriginalEntityID INT,
        @v_RawMonetaryValue MONEY,
        @nextLedgerID INT;

-- Cursor using UNION ALL for strict projection across two distinct operational tables.
-- Because ProductID/OrderID are both INT, and UnitPrice/Freight are both MONEY, 
-- this query extracts the raw values with absolute zero data type casting or math.
DECLARE MonetaryAuditCursor CURSOR FOR 
    SELECT 
        'ProductPricing' AS SourceContext, 
        ProductID AS OriginalEntityID, 
        UnitPrice AS RawMonetaryValue 
    FROM Products
    UNION ALL
    SELECT 
        'OrderFreight' AS SourceContext, 
        OrderID AS OriginalEntityID, 
        Freight AS RawMonetaryValue 
    FROM Orders;

OPEN MonetaryAuditCursor;
FETCH NEXT FROM MonetaryAuditCursor INTO @v_SourceContext, @v_OriginalEntityID, @v_RawMonetaryValue;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextLedgerID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected financial record
    -- Note: NULL monetary values are inserted exactly as they exist, preserving the raw database state
    INSERT INTO Table_MonetaryAuditLedger (LedgerID, SourceContext, OriginalEntityID, RawMonetaryValue)
    VALUES (@nextLedgerID, @v_SourceContext, @v_OriginalEntityID, @v_RawMonetaryValue);

    -- Conditionally log Row-Level Lineage based on which table the monetary value originated from
    -- (CAST is used here exclusively to meet the standardized DataLineage tracking table requirements, 
    -- but the actual financial payload passing into the new table remains completely unmanipulated)
    IF @v_SourceContext = 'ProductPricing'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_OriginalEntityID AS VARCHAR), 'Table_MonetaryAuditLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    ELSE IF @v_SourceContext = 'OrderFreight'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', CAST(@v_OriginalEntityID AS VARCHAR), 'Table_MonetaryAuditLedger', 'LedgerID', CAST(@nextLedgerID AS VARCHAR));
    END
    
    FETCH NEXT FROM MonetaryAuditCursor INTO @v_SourceContext, @v_OriginalEntityID, @v_RawMonetaryValue;
END;

CLOSE MonetaryAuditCursor; 
DEALLOCATE MonetaryAuditCursor;
GO