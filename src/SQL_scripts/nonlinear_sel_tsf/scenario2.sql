-- Section 1: Create a View with Selection and Non-Linear Transformations
-- Scenario: Calculating Bilinear Stock Value and Exponential Holding Risks for Inventory.
-- Rule: Use selection, non-linear mathematical functions (SQRT), and bilinear transformation (A * B).
CREATE OR ALTER VIEW vw_InventoryRiskMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitPrice,
    UnitsInStock,
    -- Non-linear Transformation 1 (Bilinear): Total Stock Value f(A,B) = A * B
    CAST((UnitPrice * UnitsInStock) AS DECIMAL(10,2)) AS TotalStockValue,
    -- Non-linear Transformation 2: Holding Risk Factor f(x) = sqrt(x) * 2.5
    CAST(SQRT(UnitsInStock) * 2.50 AS DECIMAL(10,2)) AS HoldingRiskFactor
FROM Products
WHERE UnitsInStock > 20 AND UnitPrice > 15.00; -- Selection applied here (Significant stock of valuable items)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_InventoryRiskMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_InventoryRiskMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (Beverages only).
IF OBJECT_ID('Table_Beverage_Risk_Audit', 'U') IS NOT NULL DROP TABLE Table_Beverage_Risk_Audit;
CREATE TABLE Table_Beverage_Risk_Audit (
    AuditID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    TotalStockValue DECIMAL(10,2),
    HoldingRiskFactor DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_value DECIMAL(10,2), @v_risk DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process advanced risk metrics for items in Category 1 (Beverages)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, TotalStockValue, HoldingRiskFactor 
    FROM vw_InventoryRiskMetrics 
    WHERE CategoryID = 1;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_value, @v_risk;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Beverage_Risk_Audit (AuditID, OriginalProductID, ProductName, TotalStockValue, HoldingRiskFactor)
    VALUES (@nextAuditID, @v_pid, @v_pname, @v_value, @v_risk);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_InventoryRiskMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Beverage_Risk_Audit', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_value, @v_risk;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeRiskAudit AS
BEGIN
    IF OBJECT_ID('Final_FinancialRiskReport', 'U') IS NOT NULL DROP TABLE Final_FinancialRiskReport;
    CREATE TABLE Final_FinancialRiskReport (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        CapitalTiedUp DECIMAL(10,2), 
        CalculatedRisk DECIMAL(10,2),
        AuditStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_value DECIMAL(10,2), @t_risk DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, TotalStockValue, HoldingRiskFactor 
        FROM ##TempRiskBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_value, @t_risk;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_FinancialRiskReport (ReportID, ItemName, CapitalTiedUp, CalculatedRisk, AuditStatus)
        VALUES (@finalID, @t_pname, @t_value, @t_risk, 'Risk Logged');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempRiskBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_FinancialRiskReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_value, @t_risk;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageRiskMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempRiskBuffer') IS NOT NULL DROP TABLE ##TempRiskBuffer;
    CREATE TABLE ##TempRiskBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        TotalStockValue DECIMAL(10,2),
        HoldingRiskFactor DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @value DECIMAL(10,2), @risk DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AuditID, ProductName, TotalStockValue, HoldingRiskFactor 
        FROM Table_Beverage_Risk_Audit;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @value, @risk;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempRiskBuffer VALUES (@newTempID, @pname, @value, @risk);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Beverage_Risk_Audit', 'AuditID', CAST(@tid AS VARCHAR), '##TempRiskBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @value, @risk;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeRiskAudit;
END;
GO

EXEC proc_StageRiskMetrics;