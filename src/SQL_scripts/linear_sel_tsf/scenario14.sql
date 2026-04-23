-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Import Duties and Wholesale Tariffs for Foreign Suppliers.
CREATE OR ALTER VIEW vw_ImportTariffMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitPrice,
    -- Linear Transformation 1: Import Duty ($) f(x) = 0.15x + 10.00
    CAST((UnitPrice * 0.15) + 10.00 AS DECIMAL(10,2)) AS ImportDuty,
    -- Linear Transformation 2: Wholesale Tariff ($) f(x) = 0.05x + 5.00
    CAST((UnitPrice * 0.05) + 5.00 AS DECIMAL(10,2)) AS WholesaleTariff
FROM Products
WHERE SupplierID IN (5, 6, 7); -- Filter: Only calculate for specific foreign suppliers
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_ImportTariffMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ImportTariffMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_ForeignImports', 'U') IS NOT NULL DROP TABLE Table_ForeignImports;
CREATE TABLE Table_ForeignImports (
    ImportID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitPrice DECIMAL(10,2),
    ImportDuty DECIMAL(10,2),
    WholesaleTariff DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_price DECIMAL(10,2), @v_duty DECIMAL(10,2), @v_tariff DECIMAL(10,2), @nextImportID INT;

-- Filter: Only process tariffs for a specific supplier (e.g., Supplier 5 - Cooperativa de Quesos 'Las Cabras')
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitPrice, ImportDuty, WholesaleTariff 
    FROM vw_ImportTariffMetrics 
    WHERE SupplierID = 5;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price, @v_duty, @v_tariff;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextImportID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_ForeignImports (ImportID, OriginalProductID, ProductName, UnitPrice, ImportDuty, WholesaleTariff)
    VALUES (@nextImportID, @v_pid, @v_pname, @v_price, @v_duty, @v_tariff);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ImportTariffMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_ForeignImports', 'ImportID', CAST(@nextImportID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price, @v_duty, @v_tariff;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeTariffReport AS
BEGIN
    IF OBJECT_ID('Final_CustomsComplianceReport', 'U') IS NOT NULL DROP TABLE Final_CustomsComplianceReport;
    CREATE TABLE Final_CustomsComplianceReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalImportDuty DECIMAL(10,2), 
        TotalWholesaleTariff DECIMAL(10,2),
        CustomsStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_duty DECIMAL(10,2), @t_tariff DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, ImportDuty, WholesaleTariff 
        FROM ##TempImportBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_duty, @t_tariff;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_CustomsComplianceReport (ReportID, ProductName, TotalImportDuty, TotalWholesaleTariff, CustomsStatus)
        VALUES (@finalID, @t_pname, @t_duty, @t_tariff, 'Cleared');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempImportBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_CustomsComplianceReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_duty, @t_tariff;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageImportMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempImportBuffer') IS NOT NULL DROP TABLE ##TempImportBuffer;
    CREATE TABLE ##TempImportBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        ImportDuty DECIMAL(10,2),
        WholesaleTariff DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @duty DECIMAL(10,2), @tariff DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT ImportID, ProductName, ImportDuty, WholesaleTariff 
        FROM Table_ForeignImports;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @duty, @tariff;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempImportBuffer VALUES (@newTempID, @pname, @duty, @tariff);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_ForeignImports', 'ImportID', CAST(@tid AS VARCHAR), '##TempImportBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @duty, @tariff;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeTariffReport;
END;
GO

EXEC proc_StageImportMetrics;