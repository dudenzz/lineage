-- Section 1: Create View with clear identifier
CREATE OR ALTER VIEW vw_SupplierStockValue AS
SELECT 
    p.ProductID, -- This serves as the PK for the view
    p.SupplierID,
    (p.UnitPrice * p.UnitsInStock) AS StockValue
FROM Products p;
GO

-- Log lineage from physical Product to the View record
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_SupplierStockValue;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_SupplierStockValue', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: View -> Table_WarehouseInventory
IF OBJECT_ID('Table_WarehouseInventory', 'U') IS NOT NULL DROP TABLE Table_WarehouseInventory;
CREATE TABLE Table_WarehouseInventory (
    InventoryID INT, 
    ProductID INT, 
    Value DECIMAL(18,2), 
    ValueCategory VARCHAR(20)
);

DECLARE @v_pid INT, @v_val DECIMAL(18,2), @nextInvID INT;
DECLARE TableCursor CURSOR FOR SELECT ProductID, StockValue FROM vw_SupplierStockValue;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_val;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextInvID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_WarehouseInventory (InventoryID, ProductID, Value, ValueCategory)
    VALUES (@nextInvID, @v_pid, @v_val, CASE WHEN @v_val > 500 THEN 'High Value' ELSE 'Low Value' END);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_SupplierStockValue', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_WarehouseInventory', 'InventoryID', CAST(@nextInvID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_val;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3: Table_WarehouseInventory -> Final_LogisticsReport (via Procedure)
CREATE OR ALTER PROCEDURE proc_GenerateLogisticsReport AS
BEGIN
    IF OBJECT_ID('Final_LogisticsReport', 'U') IS NOT NULL DROP TABLE Final_LogisticsReport;
    CREATE TABLE Final_LogisticsReport (ReportID INT, SourceInventoryID INT, FinalValue DECIMAL(18,2));

    DECLARE @invID INT, @val DECIMAL(18,2), @repID INT;
    DECLARE ProcCursor CURSOR FOR 
    SELECT InventoryID, Value FROM Table_WarehouseInventory WHERE ValueCategory = 'High Value';

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @invID, @val;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @repID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_LogisticsReport (ReportID, SourceInventoryID, FinalValue)
        VALUES (@repID, @invID, @val);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_WarehouseInventory', 'InventoryID', CAST(@invID AS VARCHAR), 'Final_LogisticsReport', 'ReportID', CAST(@repID AS VARCHAR));
        
        FETCH NEXT FROM ProcCursor INTO @invID, @val;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;
END;
GO

EXEC proc_GenerateLogisticsReport;