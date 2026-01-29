-- Section 1: Producer Session - Create and Populate Global Temp Table
-- Tests: Lineage tracking into a global ephemeral object from multiple physical sources.
IF OBJECT_ID('tempdb..##GlobalOrderMetrics') IS NOT NULL DROP TABLE ##GlobalOrderMetrics;

SELECT 
    o.OrderID,
    o.OrderDate,
    c.CustomerID,
    SUM(od.UnitPrice * od.Quantity) AS SubTotal
INTO ##GlobalOrderMetrics
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Customers c ON o.CustomerID = c.CustomerID
GROUP BY o.OrderID, o.OrderDate, c.CustomerID;

-- Log Lineage: Mapping physical sources to the Global Temp Table
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Orders', 'OrderID', CAST(OrderID AS VARCHAR), '##GlobalOrderMetrics', 'OrderID', CAST(OrderID AS VARCHAR)
FROM ##GlobalOrderMetrics;
GO

-- Section 2: Consumer Session - Enrichment of Global Temp Table
-- Tests: Tracking 'Used for Creation' when an ephemeral object is updated/transformed.
-- We create a secondary temp table to hold the results of an enrichment calculation.
IF OBJECT_ID('tempdb..##EnrichedSalesData') IS NOT NULL DROP TABLE ##EnrichedSalesData;

SELECT 
    OrderID,
    SubTotal,
    (SubTotal * 0.15) AS TaxAmount,
    (SubTotal * 0.05) AS DiscountAmount
INTO ##EnrichedSalesData
FROM ##GlobalOrderMetrics
WHERE SubTotal > 1000;

-- Log Lineage: Link between the two Global Temporary Objects
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT '##GlobalOrderMetrics', 'OrderID', CAST(OrderID AS VARCHAR), '##EnrichedSalesData', 'OrderID', CAST(OrderID AS VARCHAR)
FROM ##EnrichedSalesData;
GO

-- Section 3: Final Persistence to Physical Audit Table
-- Tests: Bridging the gap from a global temp object to a permanent table.
IF OBJECT_ID('Final_Sales_Tax_Audit', 'U') IS NOT NULL DROP TABLE Final_Sales_Tax_Audit;
CREATE TABLE Final_Sales_Tax_Audit (
    AuditID INT PRIMARY KEY,
    SourceOrderID INT,
    TotalWithTax DECIMAL(18,2)
);

DECLARE @cur_oid INT, @cur_sub DECIMAL(18,2), @cur_tax DECIMAL(18,2), @nextAuditID INT;
DECLARE FinalAuditCursor CURSOR FOR SELECT OrderID, SubTotal, TaxAmount FROM ##EnrichedSalesData;

OPEN FinalAuditCursor;
FETCH NEXT FROM FinalAuditCursor INTO @cur_oid, @cur_sub, @cur_tax;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Final_Sales_Tax_Audit (AuditID, SourceOrderID, TotalWithTax)
    VALUES (@nextAuditID, @cur_oid, @cur_sub + @cur_tax);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('##EnrichedSalesData', 'OrderID', CAST(@cur_oid AS VARCHAR), 'Final_Sales_Tax_Audit', 'AuditID', CAST(@nextAuditID AS VARCHAR));

    FETCH NEXT FROM FinalAuditCursor INTO @cur_oid, @cur_sub, @cur_tax;
END;
CLOSE FinalAuditCursor; DEALLOCATE FinalAuditCursor;
GO

-- Section 4: Simulate Expiration of Global Objects
-- Tests: Verification that the lineage tool has captured the logic before the objects disappeared.
DROP TABLE ##GlobalOrderMetrics;
DROP TABLE ##EnrichedSalesData;
GO