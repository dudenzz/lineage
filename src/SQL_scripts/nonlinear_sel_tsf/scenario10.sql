-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Luxury Tax Penalties and Market Saturation Indices for premium goods.
-- Rule: Use selection and strictly univariate non-linear transformations (EXP, POWER). No bilinear (A*B).
CREATE OR ALTER VIEW vw_LuxuryTaxMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitPrice,
    -- Non-linear Transformation 1 (Exponential): Luxury Tax Penalty f(x) = 5 * e^(x/50)
    -- Models a sharply increasing tax penalty for exceptionally high-priced items.
    CAST(EXP(UnitPrice / 50.0) * 5.00 AS DECIMAL(10,2)) AS LuxuryTaxPenalty,
    -- Non-linear Transformation 2 (Power): Market Saturation Index f(x) = x^1.15
    -- Models non-linear difficulty in selling goods as their price increases.
    CAST(POWER(UnitPrice, 1.15) AS DECIMAL(10,2)) AS SaturationIndex
FROM Products
WHERE UnitPrice > 50.00; -- Selection applied here (Only premium, high-priced goods)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_LuxuryTaxMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_LuxuryTaxMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (Confections only).
IF OBJECT_ID('Table_Premium_Confections', 'U') IS NOT NULL DROP TABLE Table_Premium_Confections;
CREATE TABLE Table_Premium_Confections (
    LuxuryAuditID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    LuxuryTaxPenalty DECIMAL(10,2),
    SaturationIndex DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_tax DECIMAL(10,2), @v_sat DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process these financial metrics for premium goods in Category 3 (Confections)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, LuxuryTaxPenalty, SaturationIndex 
    FROM vw_LuxuryTaxMetrics 
    WHERE CategoryID = 3;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_tax, @v_sat;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Premium_Confections (LuxuryAuditID, OriginalProductID, ProductName, LuxuryTaxPenalty, SaturationIndex)
    VALUES (@nextAuditID, @v_pid, @v_pname, @v_tax, @v_sat);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_LuxuryTaxMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Premium_Confections', 'LuxuryAuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_tax, @v_sat;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeLuxuryReport AS
BEGIN
    IF OBJECT_ID('Final_LuxuryPricingRegistry', 'U') IS NOT NULL DROP TABLE Final_LuxuryPricingRegistry;
    CREATE TABLE Final_LuxuryPricingRegistry (
        ReportID INT, 
        ItemName NVARCHAR(40), 
        AssessedPenalty DECIMAL(10,2), 
        MarketResistance DECIMAL(10,2),
        AuditStatus VARCHAR(25)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_tax DECIMAL(10,2), @t_sat DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, LuxuryTaxPenalty, SaturationIndex 
        FROM ##TempLuxuryBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_tax, @t_sat;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_LuxuryPricingRegistry (ReportID, ItemName, AssessedPenalty, MarketResistance, AuditStatus)
        VALUES (@finalID, @t_pname, @t_tax, @t_sat, 'Surcharge Applied');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempLuxuryBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_LuxuryPricingRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_tax, @t_sat;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageLuxuryMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempLuxuryBuffer') IS NOT NULL DROP TABLE ##TempLuxuryBuffer;
    CREATE TABLE ##TempLuxuryBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        LuxuryTaxPenalty DECIMAL(10,2),
        SaturationIndex DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @tax DECIMAL(10,2), @sat DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT LuxuryAuditID, ProductName, LuxuryTaxPenalty, SaturationIndex 
        FROM Table_Premium_Confections;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @tax, @sat;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempLuxuryBuffer VALUES (@newTempID, @pname, @tax, @sat);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Premium_Confections', 'LuxuryAuditID', CAST(@tid AS VARCHAR), '##TempLuxuryBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @tax, @sat;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeLuxuryReport;
END;
GO

EXEC proc_StageLuxuryMetrics;