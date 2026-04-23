-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Shelf-Space Allocations and Marketing Subsidies for Wholesale Vendors.
CREATE OR ALTER VIEW vw_VendorMarketingMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitsInStock,
    -- Linear Transformation 1: Marketing Subsidy ($) f(x) = 0.25x + 50.00
    CAST((UnitsInStock * 0.25) + 50.00 AS DECIMAL(10,2)) AS MarketingSubsidy,
    -- Linear Transformation 2: Shelf-Space Allocation (sq inches) f(x) = 1.5x + 20.0
    CAST((UnitsInStock * 1.5) + 20.0 AS DECIMAL(10,2)) AS ShelfSpaceSqIn
FROM Products
WHERE UnitsInStock > 10; -- Filter: Only calculate for items with substantial stock
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_VendorMarketingMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_VendorMarketingMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_VendorShelfSpace', 'U') IS NOT NULL DROP TABLE Table_VendorShelfSpace;
CREATE TABLE Table_VendorShelfSpace (
    AllocationID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitsInStock SMALLINT,
    MarketingSubsidy DECIMAL(10,2),
    ShelfSpaceSqIn DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_uis SMALLINT, @v_subsidy DECIMAL(10,2), @v_space DECIMAL(10,2), @nextAllocID INT;

-- Filter: Only process allocations for a specific supplier (e.g., Supplier 4 - Tokyo Traders)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitsInStock, MarketingSubsidy, ShelfSpaceSqIn 
    FROM vw_VendorMarketingMetrics 
    WHERE SupplierID = 4;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_subsidy, @v_space;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAllocID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_VendorShelfSpace (AllocationID, OriginalProductID, ProductName, UnitsInStock, MarketingSubsidy, ShelfSpaceSqIn)
    VALUES (@nextAllocID, @v_pid, @v_pname, @v_uis, @v_subsidy, @v_space);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_VendorMarketingMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_VendorShelfSpace', 'AllocationID', CAST(@nextAllocID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_subsidy, @v_space;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeMarketingReport AS
BEGIN
    IF OBJECT_ID('Final_VendorMarketingReport', 'U') IS NOT NULL DROP TABLE Final_VendorMarketingReport;
    CREATE TABLE Final_VendorMarketingReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalSubsidy DECIMAL(10,2), 
        TotalShelfSpace DECIMAL(10,2),
        DeploymentStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_subsidy DECIMAL(10,2), @t_space DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, MarketingSubsidy, ShelfSpaceSqIn 
        FROM ##TempMarketingBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_subsidy, @t_space;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_VendorMarketingReport (ReportID, ProductName, TotalSubsidy, TotalShelfSpace, DeploymentStatus)
        VALUES (@finalID, @t_pname, @t_subsidy, @t_space, 'Approved');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempMarketingBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_VendorMarketingReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_subsidy, @t_space;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageMarketingMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempMarketingBuffer') IS NOT NULL DROP TABLE ##TempMarketingBuffer;
    CREATE TABLE ##TempMarketingBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        MarketingSubsidy DECIMAL(10,2),
        ShelfSpaceSqIn DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @subsidy DECIMAL(10,2), @space DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AllocationID, ProductName, MarketingSubsidy, ShelfSpaceSqIn 
        FROM Table_VendorShelfSpace;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @subsidy, @space;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempMarketingBuffer VALUES (@newTempID, @pname, @subsidy, @space);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_VendorShelfSpace', 'AllocationID', CAST(@tid AS VARCHAR), '##TempMarketingBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @subsidy, @space;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeMarketingReport;
END;
GO

EXEC proc_StageMarketingMetrics;