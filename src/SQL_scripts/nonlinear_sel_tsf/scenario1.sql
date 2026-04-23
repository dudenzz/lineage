-- Section 1: Create a View with Selection and Non-Linear Transformations
-- Scenario: Calculating Volumetric Surcharges and Logarithmic Complexity Indices for heavy freight.
-- Rule: Use selection (WHERE clause) AND non-linear transformations (e.g., POWER, LOG).
CREATE OR ALTER VIEW vw_NonLinearFreightMetrics AS
SELECT 
    OrderID,
    -- Copied Columns
    CustomerID,
    ShipCountry,
    Freight,
    -- Non-linear Transformation 1: Volumetric Surcharge f(x) = x^1.5
    CAST(POWER(Freight, 1.5) AS DECIMAL(10,2)) AS VolumetricSurcharge,
    -- Non-linear Transformation 2: Complexity Index f(x) = 15 * ln(x + 1)
    CAST(LOG(Freight + 1) * 15.00 AS DECIMAL(10,2)) AS ComplexityIndex
FROM Orders
WHERE Freight > 50.00; -- Selection applied here (Heavy freight only)
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_NonLinearFreightMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_NonLinearFreightMetrics', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (USA shipments only).
IF OBJECT_ID('Table_USA_HeavyFreight', 'U') IS NOT NULL DROP TABLE Table_USA_HeavyFreight;
CREATE TABLE Table_USA_HeavyFreight (
    LogisticsID INT, 
    OriginalOrderID INT, 
    CustomerID NCHAR(5),
    Freight DECIMAL(10,2),
    VolumetricSurcharge DECIMAL(10,2),
    ComplexityIndex DECIMAL(10,2)
);
GO

DECLARE @v_oid INT, @v_cid NCHAR(5), @v_freight DECIMAL(10,2), @v_surcharge DECIMAL(10,2), @v_index DECIMAL(10,2), @nextLogisticsID INT;

-- Filter: Only process these advanced metrics for heavy orders shipped to the USA
DECLARE TableCursor CURSOR FOR 
    SELECT OrderID, CustomerID, Freight, VolumetricSurcharge, ComplexityIndex 
    FROM vw_NonLinearFreightMetrics 
    WHERE ShipCountry = 'USA';

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight, @v_surcharge, @v_index;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLogisticsID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_USA_HeavyFreight (LogisticsID, OriginalOrderID, CustomerID, Freight, VolumetricSurcharge, ComplexityIndex)
    VALUES (@nextLogisticsID, @v_oid, @v_cid, @v_freight, @v_surcharge, @v_index);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_NonLinearFreightMetrics', 'OrderID', CAST(@v_oid AS VARCHAR), 'Table_USA_HeavyFreight', 'LogisticsID', CAST(@nextLogisticsID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_oid, @v_cid, @v_freight, @v_surcharge, @v_index;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeAdvancedLogistics AS
BEGIN
    IF OBJECT_ID('Final_FreightComplexityReport', 'U') IS NOT NULL DROP TABLE Final_FreightComplexityReport;
    CREATE TABLE Final_FreightComplexityReport (
        ReportID INT, 
        CustomerID NCHAR(5), 
        TotalVolumetricCost DECIMAL(10,2), 
        CalculatedComplexity DECIMAL(10,2),
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_cid NCHAR(5), @t_surcharge DECIMAL(10,2), @t_index DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, CustomerID, VolumetricSurcharge, ComplexityIndex 
        FROM ##TempAdvancedFreightBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_surcharge, @t_index;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_FreightComplexityReport (ReportID, CustomerID, TotalVolumetricCost, CalculatedComplexity, AuditStatus)
        VALUES (@finalID, @t_cid, @t_surcharge, @t_index, 'Analyzed');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempAdvancedFreightBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_FreightComplexityReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_cid, @t_surcharge, @t_index;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageAdvancedLogistics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempAdvancedFreightBuffer') IS NOT NULL DROP TABLE ##TempAdvancedFreightBuffer;
    CREATE TABLE ##TempAdvancedFreightBuffer (
        TempID INT, 
        CustomerID NCHAR(5), 
        VolumetricSurcharge DECIMAL(10,2),
        ComplexityIndex DECIMAL(10,2)
    );

    DECLARE @tid INT, @cid NCHAR(5), @surcharge DECIMAL(10,2), @index DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT LogisticsID, CustomerID, VolumetricSurcharge, ComplexityIndex 
        FROM Table_USA_HeavyFreight;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @cid, @surcharge, @index;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempAdvancedFreightBuffer VALUES (@newTempID, @cid, @surcharge, @index);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_USA_HeavyFreight', 'LogisticsID', CAST(@tid AS VARCHAR), '##TempAdvancedFreightBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @cid, @surcharge, @index;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeAdvancedLogistics;
END;
GO

EXEC proc_StageAdvancedLogistics;