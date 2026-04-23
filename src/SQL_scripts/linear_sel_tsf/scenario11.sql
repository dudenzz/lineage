-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Depreciation Expenses and Salvage Values for Discontinued Assets.
CREATE OR ALTER VIEW vw_ProductDepreciation AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitPrice,
    -- Linear Transformation 1: Depreciation Expense ($) f(x) = 0.10x + 2.50
    CAST((UnitPrice * 0.10) + 2.50 AS DECIMAL(10,2)) AS DepreciationExpense,
    -- Linear Transformation 2: Salvage Value ($) f(x) = 0.25x + 5.00
    CAST((UnitPrice * 0.25) + 5.00 AS DECIMAL(10,2)) AS SalvageValue
FROM Products
WHERE Discontinued = 1; -- Filter: Only calculate for discontinued products
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_ProductDepreciation;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ProductDepreciation', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_DiscontinuedAssets', 'U') IS NOT NULL DROP TABLE Table_DiscontinuedAssets;
CREATE TABLE Table_DiscontinuedAssets (
    AssetID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitPrice DECIMAL(10,2),
    DepreciationExpense DECIMAL(10,2),
    SalvageValue DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_price DECIMAL(10,2), @v_dep DECIMAL(10,2), @v_salvage DECIMAL(10,2), @nextAssetID INT;

-- Filter: Only process assets for a specific category (e.g., Category 8 - Seafood)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitPrice, DepreciationExpense, SalvageValue 
    FROM vw_ProductDepreciation 
    WHERE CategoryID = 8;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price, @v_dep, @v_salvage;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAssetID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_DiscontinuedAssets (AssetID, OriginalProductID, ProductName, UnitPrice, DepreciationExpense, SalvageValue)
    VALUES (@nextAssetID, @v_pid, @v_pname, @v_price, @v_dep, @v_salvage);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ProductDepreciation', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_DiscontinuedAssets', 'AssetID', CAST(@nextAssetID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price, @v_dep, @v_salvage;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeAssetReport AS
BEGIN
    IF OBJECT_ID('Final_AssetWriteOffReport', 'U') IS NOT NULL DROP TABLE Final_AssetWriteOffReport;
    CREATE TABLE Final_AssetWriteOffReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalWriteOff DECIMAL(10,2), 
        RecoveredValue DECIMAL(10,2),
        LedgerStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_dep DECIMAL(10,2), @t_salvage DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, DepreciationExpense, SalvageValue 
        FROM ##TempAssetBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_dep, @t_salvage;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_AssetWriteOffReport (ReportID, ProductName, TotalWriteOff, RecoveredValue, LedgerStatus)
        VALUES (@finalID, @t_pname, @t_dep, @t_salvage, 'Posted');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempAssetBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_AssetWriteOffReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_dep, @t_salvage;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageAssetMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempAssetBuffer') IS NOT NULL DROP TABLE ##TempAssetBuffer;
    CREATE TABLE ##TempAssetBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        DepreciationExpense DECIMAL(10,2),
        SalvageValue DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @dep DECIMAL(10,2), @salvage DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AssetID, ProductName, DepreciationExpense, SalvageValue 
        FROM Table_DiscontinuedAssets;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @dep, @salvage;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempAssetBuffer VALUES (@newTempID, @pname, @dep, @salvage);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_DiscontinuedAssets', 'AssetID', CAST(@tid AS VARCHAR), '##TempAssetBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @dep, @salvage;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeAssetReport;
END;
GO

EXEC proc_StageAssetMetrics;