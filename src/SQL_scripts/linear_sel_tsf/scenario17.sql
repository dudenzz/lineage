-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Supplier Quality Assessment Fees and Restocking Surcharges.
CREATE OR ALTER VIEW vw_SupplierQualityMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitsOnOrder,
    -- Linear Transformation 1: Quality Assessment Fee ($) f(x) = 0.05x + 50.00
    CAST((UnitsOnOrder * 0.05) + 50.00 AS DECIMAL(10,2)) AS QualityFee,
    -- Linear Transformation 2: Restocking Surcharge ($) f(x) = 0.15x + 10.00
    CAST((UnitsOnOrder * 0.15) + 10.00 AS DECIMAL(10,2)) AS RestockingSurcharge
FROM Products
WHERE UnitsOnOrder > 10; -- Filter: Only calculate for bulk replenishment orders
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_SupplierQualityMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_SupplierQualityMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_SupplierPenalties', 'U') IS NOT NULL DROP TABLE Table_SupplierPenalties;
CREATE TABLE Table_SupplierPenalties (
    PenaltyID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitsOnOrder SMALLINT,
    QualityFee DECIMAL(10,2),
    RestockingSurcharge DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_uoo SMALLINT, @v_qfee DECIMAL(10,2), @v_rsurcharge DECIMAL(10,2), @nextPenaltyID INT;

-- Filter: Only process penalties for a specific supplier region/ID (e.g., Supplier 2 - New Orleans Cajun Delights)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitsOnOrder, QualityFee, RestockingSurcharge 
    FROM vw_SupplierQualityMetrics 
    WHERE SupplierID = 2;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uoo, @v_qfee, @v_rsurcharge;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextPenaltyID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_SupplierPenalties (PenaltyID, OriginalProductID, ProductName, UnitsOnOrder, QualityFee, RestockingSurcharge)
    VALUES (@nextPenaltyID, @v_pid, @v_pname, @v_uoo, @v_qfee, @v_rsurcharge);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_SupplierQualityMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_SupplierPenalties', 'PenaltyID', CAST(@nextPenaltyID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uoo, @v_qfee, @v_rsurcharge;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeSupplierAudit AS
BEGIN
    IF OBJECT_ID('Final_SupplierComplianceReport', 'U') IS NOT NULL DROP TABLE Final_SupplierComplianceReport;
    CREATE TABLE Final_SupplierComplianceReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalQualityFees DECIMAL(10,2), 
        TotalRestockSurcharges DECIMAL(10,2),
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_qfee DECIMAL(10,2), @t_rsurcharge DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, QualityFee, RestockingSurcharge 
        FROM ##TempSupplierBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_qfee, @t_rsurcharge;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_SupplierComplianceReport (ReportID, ProductName, TotalQualityFees, TotalRestockSurcharges, AuditStatus)
        VALUES (@finalID, @t_pname, @t_qfee, @t_rsurcharge, 'In Review');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempSupplierBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_SupplierComplianceReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_qfee, @t_rsurcharge;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageSupplierMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempSupplierBuffer') IS NOT NULL DROP TABLE ##TempSupplierBuffer;
    CREATE TABLE ##TempSupplierBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        QualityFee DECIMAL(10,2),
        RestockingSurcharge DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @qfee DECIMAL(10,2), @rsurcharge DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT PenaltyID, ProductName, QualityFee, RestockingSurcharge 
        FROM Table_SupplierPenalties;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @qfee, @rsurcharge;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempSupplierBuffer VALUES (@newTempID, @pname, @qfee, @rsurcharge);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_SupplierPenalties', 'PenaltyID', CAST(@tid AS VARCHAR), '##TempSupplierBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @qfee, @rsurcharge;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeSupplierAudit;
END;
GO

EXEC proc_StageSupplierMetrics;