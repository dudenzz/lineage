-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Carbon Footprint and Eco-Tax Offsets for Orders.
CREATE OR ALTER VIEW vw_CarbonEmissions AS
SELECT 
    OrderID,
    -- Copied Columns
    CustomerID,
    ShipCountry,
    Freight,
    -- Linear Transformation 1: Carbon Footprint (kg CO2) f(x) = 2.5x + 15.0
    CAST((Freight * 2.5) + 15.0 AS DECIMAL(10,2)) AS CarbonFootprintKg,
    -- Linear Transformation 2: Carbon Offset Tax ($) f(x) = 0.1x + 5.00
    CAST((Freight * 0.1) + 5.00 AS DECIMAL(10,2)) AS OffsetTax
FROM Orders
WHERE Freight > 0; -- Filter: Only orders with actual freight weight/cost
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_CarbonEmissions;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_CarbonEmissions', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_EcoShipping', 'U') IS NOT NULL DROP TABLE Table_EcoShipping;
CREATE TABLE Table_EcoShipping (
    EcoShippingID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    Freight DECIMAL(10,2),
    CarbonFootprintKg DECIMAL(10,2),
    OffsetTax DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_freight DECIMAL(10,2), @v_carbon DECIMAL(10,2), @v_tax DECIMAL(10,2), @nextEcoID INT;

-- Filter: Only process emissions tracking for shipments to Germany
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, Freight, CarbonFootprintKg, OffsetTax 
    FROM vw_CarbonEmissions 
    WHERE ShipCountry = 'Germany';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight, @v_carbon, @v_tax;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextEcoID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_EcoShipping (EcoShippingID, OriginalOrderID, CustomerID, Freight, CarbonFootprintKg, OffsetTax)
    VALUES (@nextEcoID, @v_oid, @v_cid, @v_freight, @v_carbon, @v_tax);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_CarbonEmissions', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_EcoShipping', 'EcoShippingID', CAST(@nextEcoID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight, @v_carbon, @v_tax;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeEcoReport AS
BEGIN
    IF OBJECT_ID('Final_SustainabilityReport', 'U') IS NOT NULL DROP TABLE Final_SustainabilityReport;
    CREATE TABLE Final_SustainabilityReport (
        ReportID INT, 
        CustomerID NCHAR(5), 
        TotalEmissionsKg DECIMAL(10,2), 
        AppliedTax DECIMAL(10,2),
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_carbon DECIMAL(10,2), @t_tax DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, CustomerID, CarbonFootprintKg, OffsetTax 
        FROM ##TempEcoBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_carbon, @t_tax;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_SustainabilityReport (ReportID, CustomerID, TotalEmissionsKg, AppliedTax, AuditStatus)
        VALUES (@finalID, @t_cid, @t_carbon, @t_tax, 'Compliant');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempEcoBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_SustainabilityReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_carbon, @t_tax;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageEcoMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempEcoBuffer') IS NOT NULL DROP TABLE ##TempEcoBuffer;
    CREATE TABLE ##TempEcoBuffer (
        TempID INT, 
        CustomerID NCHAR(5), 
        CarbonFootprintKg DECIMAL(10,2),
        OffsetTax DECIMAL(10,2)
    );

    DECLARE @tid INT, @cid NCHAR(5), @carbon DECIMAL(10,2), @tax DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT EcoShippingID, CustomerID, CarbonFootprintKg, OffsetTax 
        FROM Table_EcoShipping;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @carbon, @tax;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempEcoBuffer VALUES (@newTempID, @cid, @carbon, @tax);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_EcoShipping', 'EcoShippingID', CAST(@tid AS VARCHAR), '##TempEcoBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @carbon, @tax;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeEcoReport;
END;
GO

EXEC proc_StageEcoMetrics;