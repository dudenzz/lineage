-- Section 1: Create a View with Linear Transformation and Copying
-- Tests: Basic view-to-table lineage, direct copy, and linear math f(x) = 1.2x + 2.00
CREATE OR ALTER VIEW vw_IntlProductPricing AS
SELECT 
    ProductID,
    ProductName, -- Direct Copy
    CategoryID,
    -- Linear Transformation: m=1.2, b=2.00
    CAST((UnitPrice * 1.2) + 2.00 AS DECIMAL(10,2)) AS AdjustedPrice
FROM Products
WHERE Discontinued = 0; -- Filter out discontinued
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_IntlProductPricing;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_IntlProductPricing', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
-- Tests: Tool's ability to track lineage when records are filtered out based on Category.
IF OBJECT_ID('Table_IntlProducts', 'U') IS NOT NULL DROP TABLE Table_IntlProducts;
CREATE TABLE Table_IntlProducts (
    IntlProductID INT, 
    OriginalID INT, 
    ProductName NVARCHAR(40),
    AdjustedPrice DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_price DECIMAL(10,2), @nextIntlID INT;
-- Filter: Only process Category 1 (Beverages)
DECLARE TableCursor CURSOR FOR SELECT ProductID, ProductName, AdjustedPrice FROM vw_IntlProductPricing WHERE CategoryID = 1;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextIntlID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_IntlProducts (IntlProductID, OriginalID, ProductName, AdjustedPrice)
    VALUES (@nextIntlID, @v_pid, @v_pname, @v_price);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_IntlProductPricing', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_IntlProducts', 'IntlProductID', CAST(@nextIntlID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_price;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeProductReport AS
BEGIN
    IF OBJECT_ID('Final_ProductReport', 'U') IS NOT NULL DROP TABLE Final_ProductReport;
    CREATE TABLE Final_ProductReport (ReportID INT, ProductName NVARCHAR(40), FinalPrice DECIMAL(10,2), Status VARCHAR(10));

    DECLARE @t_id INT, @t_name NVARCHAR(40), @t_price DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR SELECT TempID, ProductName, AdjustedPrice FROM ##TempProductBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_name, @t_price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_ProductReport (ReportID, ProductName, FinalPrice, Status)
        VALUES (@finalID, @t_name, @t_price, 'Processed');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempProductBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_ProductReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_name, @t_price;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessProductStaging AS
BEGIN
    IF OBJECT_ID('tempdb..##TempProductBuffer') IS NOT NULL DROP TABLE ##TempProductBuffer;
    CREATE TABLE ##TempProductBuffer (TempID INT, ProductName NVARCHAR(40), AdjustedPrice DECIMAL(10,2));

    DECLARE @tid INT, @name NVARCHAR(40), @price DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR SELECT IntlProductID, ProductName, AdjustedPrice FROM Table_IntlProducts;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @name, @price;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempProductBuffer VALUES (@newTempID, @name, @price);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_IntlProducts', 'IntlProductID', CAST(@tid AS VARCHAR), '##TempProductBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @name, @price;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeProductReport;
END;
GO

EXEC proc_ProcessProductStaging;