-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating International Customs Brokerage Fees and VAT Estimates on Freight.
CREATE OR ALTER VIEW vw_IntlCustomsMetrics AS
SELECT 
    OrderID,
    -- Copied Columns
    CustomerID,
    ShipCountry,
    Freight,
    -- Linear Transformation 1: Brokerage Fee ($) f(x) = 0.15x + 25.00
    CAST((Freight * 0.15) + 25.00 AS DECIMAL(10,2)) AS BrokerageFee,
    -- Linear Transformation 2: Estimated VAT on Freight ($) f(x) = 0.20x + 5.00
    CAST((Freight * 0.20) + 5.00 AS DECIMAL(10,2)) AS EstimatedVAT
FROM Orders
WHERE ShipCountry NOT IN ('USA', 'Canada'); -- Filter: Only calculate for overseas international shipments
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_IntlCustomsMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_IntlCustomsMetrics', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_EuropeanCustoms', 'U') IS NOT NULL DROP TABLE Table_EuropeanCustoms;
CREATE TABLE Table_EuropeanCustoms (
    CustomsID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    Freight DECIMAL(10,2),
    BrokerageFee DECIMAL(10,2),
    EstimatedVAT DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_freight DECIMAL(10,2), @v_brokerage DECIMAL(10,2), @v_vat DECIMAL(10,2), @nextCustomsID INT;

-- Filter: Only process customs for specific European countries
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, Freight, BrokerageFee, EstimatedVAT 
    FROM vw_IntlCustomsMetrics 
    WHERE ShipCountry IN ('UK', 'France', 'Germany');

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight, @v_brokerage, @v_vat;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextCustomsID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_EuropeanCustoms (CustomsID, OriginalOrderID, CustomerID, Freight, BrokerageFee, EstimatedVAT)
    VALUES (@nextCustomsID, @v_oid, @v_cid, @v_freight, @v_brokerage, @v_vat);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_IntlCustomsMetrics', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_EuropeanCustoms', 'CustomsID', CAST(@nextCustomsID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight, @v_brokerage, @v_vat;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeCustomsReport AS
BEGIN
    IF OBJECT_ID('Final_BrokerageAuditReport', 'U') IS NOT NULL DROP TABLE Final_BrokerageAuditReport;
    CREATE TABLE Final_BrokerageAuditReport (
        ReportID INT, 
        CustomerID NCHAR(5), 
        TotalBrokerageFee DECIMAL(10,2), 
        TotalAssessedVAT DECIMAL(10,2),
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_brokerage DECIMAL(10,2), @t_vat DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, CustomerID, BrokerageFee, EstimatedVAT 
        FROM ##TempCustomsBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_brokerage, @t_vat;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_BrokerageAuditReport (ReportID, CustomerID, TotalBrokerageFee, TotalAssessedVAT, AuditStatus)
        VALUES (@finalID, @t_cid, @t_brokerage, @t_vat, 'Audited');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempCustomsBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_BrokerageAuditReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_brokerage, @t_vat;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageCustomsMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempCustomsBuffer') IS NOT NULL DROP TABLE ##TempCustomsBuffer;
    CREATE TABLE ##TempCustomsBuffer (
        TempID INT, 
        CustomerID NCHAR(5), 
        BrokerageFee DECIMAL(10,2),
        EstimatedVAT DECIMAL(10,2)
    );

    DECLARE @tid INT, @cid NCHAR(5), @brokerage DECIMAL(10,2), @vat DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT CustomsID, CustomerID, BrokerageFee, EstimatedVAT 
        FROM Table_EuropeanCustoms;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @brokerage, @vat;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempCustomsBuffer VALUES (@newTempID, @cid, @brokerage, @vat);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_EuropeanCustoms', 'CustomsID', CAST(@tid AS VARCHAR), '##TempCustomsBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @brokerage, @vat;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeCustomsReport;
END;
GO

EXEC proc_StageCustomsMetrics;