-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Refrigeration Maintenance Costs and Spoilage Risk Premiums for Perishable Goods.
CREATE OR ALTER VIEW vw_PerishableColdStorage AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitsInStock,
    UnitPrice,
    -- Linear Transformation 1: Refrigeration Cost ($) f(x) = 0.20x + 15.00
    CAST((UnitsInStock * 0.20) + 15.00 AS DECIMAL(10,2)) AS RefrigerationCost,
    -- Linear Transformation 2: Spoilage Risk Premium ($) f(x) = 0.05x + 2.50
    CAST((UnitPrice * 0.05) + 2.50 AS DECIMAL(10,2)) AS SpoilagePremium
FROM Products
WHERE CategoryID IN (1, 4, 6, 8); -- Filter: Beverages, Dairy, Meat/Poultry, and Seafood
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_PerishableColdStorage;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_PerishableColdStorage', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_ColdStorageLogistics', 'U') IS NOT NULL DROP TABLE Table_ColdStorageLogistics;
CREATE TABLE Table_ColdStorageLogistics (
    StorageID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitsInStock SMALLINT,
    RefrigerationCost DECIMAL(10,2),
    SpoilagePremium DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_uis SMALLINT, @v_fridge DECIMAL(10,2), @v_spoil DECIMAL(10,2), @nextStorageID INT;

-- Filter: Only process logistics for items that have significant inventory taking up space
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitsInStock, RefrigerationCost, SpoilagePremium 
    FROM vw_PerishableColdStorage 
    WHERE UnitsInStock > 15;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_fridge, @v_spoil;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextStorageID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_ColdStorageLogistics (StorageID, OriginalProductID, ProductName, UnitsInStock, RefrigerationCost, SpoilagePremium)
    VALUES (@nextStorageID, @v_pid, @v_pname, @v_uis, @v_fridge, @v_spoil);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_PerishableColdStorage', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_ColdStorageLogistics', 'StorageID', CAST(@nextStorageID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_fridge, @v_spoil;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeColdStorageReport AS
BEGIN
    IF OBJECT_ID('Final_PerishableAuditReport', 'U') IS NOT NULL DROP TABLE Final_PerishableAuditReport;
    CREATE TABLE Final_PerishableAuditReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalFacilityCost DECIMAL(10,2), 
        TotalRiskPremium DECIMAL(10,2),
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_fridge DECIMAL(10,2), @t_spoil DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, RefrigerationCost, SpoilagePremium 
        FROM ##TempColdStorageBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_fridge, @t_spoil;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_PerishableAuditReport (ReportID, ProductName, TotalFacilityCost, TotalRiskPremium, AuditStatus)
        VALUES (@finalID, @t_pname, @t_fridge, @t_spoil, 'Inspected');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempColdStorageBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_PerishableAuditReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_fridge, @t_spoil;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageColdStorageMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempColdStorageBuffer') IS NOT NULL DROP TABLE ##TempColdStorageBuffer;
    CREATE TABLE ##TempColdStorageBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        RefrigerationCost DECIMAL(10,2),
        SpoilagePremium DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @fridge DECIMAL(10,2), @spoil DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT StorageID, ProductName, RefrigerationCost, SpoilagePremium 
        FROM Table_ColdStorageLogistics;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @fridge, @spoil;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempColdStorageBuffer VALUES (@newTempID, @pname, @fridge, @spoil);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_ColdStorageLogistics', 'StorageID', CAST(@tid AS VARCHAR), '##TempColdStorageBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @fridge, @spoil;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeColdStorageReport;
END;
GO

EXEC proc_StageColdStorageMetrics;