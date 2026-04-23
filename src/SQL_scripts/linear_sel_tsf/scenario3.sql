-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Inventory Holding Costs and Insurance Premiums for Products.
CREATE OR ALTER VIEW vw_InventoryMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitsInStock,
    -- Linear Transformation 1: Monthly Storage Cost f(x) = 0.15x + 12.50
    CAST((UnitsInStock * 0.15) + 12.50 AS DECIMAL(10,2)) AS StorageCost,
    -- Linear Transformation 2: Base Insurance Premium f(x) = 0.02x + 5.00
    CAST((UnitPrice * 0.02) + 5.00 AS DECIMAL(10,2)) AS InsurancePremium
FROM Products
WHERE UnitsInStock > 0; -- Filter: Only items currently in stock
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_InventoryMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_InventoryMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_SupplierInventory', 'U') IS NOT NULL DROP TABLE Table_SupplierInventory;
CREATE TABLE Table_SupplierInventory (
    InventoryMetricID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    SupplierID INT,
    StorageCost DECIMAL(10,2),
    InsurancePremium DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_sid INT, @v_storage DECIMAL(10,2), @v_insurance DECIMAL(10,2), @nextInvID INT;

-- Filter: Only process metrics for a specific set of Suppliers (e.g., Supplier 1 and 2)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, SupplierID, StorageCost, InsurancePremium 
    FROM vw_InventoryMetrics 
    WHERE SupplierID IN (1, 2);

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_sid, @v_storage, @v_insurance;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextInvID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_SupplierInventory (InventoryMetricID, OriginalProductID, ProductName, SupplierID, StorageCost, InsurancePremium)
    VALUES (@nextInvID, @v_pid, @v_pname, @v_sid, @v_storage, @v_insurance);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_InventoryMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_SupplierInventory', 'InventoryMetricID', CAST(@nextInvID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_sid, @v_storage, @v_insurance;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeInventoryReport AS
BEGIN
    IF OBJECT_ID('Final_InventoryCostsReport', 'U') IS NOT NULL DROP TABLE Final_InventoryCostsReport;
    CREATE TABLE Final_InventoryCostsReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalHoldingCost DECIMAL(10,2), 
        RiskPremium DECIMAL(10,2),
        ReportStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_storage DECIMAL(10,2), @t_insurance DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, StorageCost, InsurancePremium 
        FROM ##TempInventoryBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_storage, @t_insurance;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_InventoryCostsReport (ReportID, ProductName, TotalHoldingCost, RiskPremium, ReportStatus)
        VALUES (@finalID, @t_pname, @t_storage, @t_insurance, 'Audited');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempInventoryBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_InventoryCostsReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_storage, @t_insurance;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageInventoryMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempInventoryBuffer') IS NOT NULL DROP TABLE ##TempInventoryBuffer;
    CREATE TABLE ##TempInventoryBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        StorageCost DECIMAL(10,2),
        InsurancePremium DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @storage DECIMAL(10,2), @insurance DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT InventoryMetricID, ProductName, StorageCost, InsurancePremium 
        FROM Table_SupplierInventory;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @storage, @insurance;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempInventoryBuffer VALUES (@newTempID, @pname, @storage, @insurance);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_SupplierInventory', 'InventoryMetricID', CAST(@tid AS VARCHAR), '##TempInventoryBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @storage, @insurance;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeInventoryReport;
END;
GO

EXEC proc_StageInventoryMetrics;