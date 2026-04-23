-- Section 1: Create a View with Linear Transformations and Copying
-- Scenario: Calculating Energy Consumption Surcharges and Recycling Fees for Products.
CREATE OR ALTER VIEW vw_EnergyMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    CategoryID,
    UnitsInStock,
    -- Linear Transformation 1: Energy Surcharge ($) f(x) = 0.08x + 15.00
    CAST((UnitsInStock * 0.08) + 15.00 AS DECIMAL(10,2)) AS EnergySurcharge,
    -- Linear Transformation 2: Recycling Fee ($) f(x) = 0.12x + 8.50
    CAST((UnitsInStock * 0.12) + 8.50 AS DECIMAL(10,2)) AS RecyclingFee
FROM Products
WHERE UnitsInStock > 0; -- Filter: Only calculate for items currently in stock
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_EnergyMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_EnergyMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with a WHERE clause
IF OBJECT_ID('Table_DairySurcharges', 'U') IS NOT NULL DROP TABLE Table_DairySurcharges;
CREATE TABLE Table_DairySurcharges (
    SurchargeID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    UnitsInStock SMALLINT,
    EnergySurcharge DECIMAL(10,2),
    RecyclingFee DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_uis SMALLINT, @v_energy DECIMAL(10,2), @v_recycle DECIMAL(10,2), @nextSurchargeID INT;

-- Filter: Only process surcharges for a specific category (e.g., Category 4 - Dairy Products)
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, UnitsInStock, EnergySurcharge, RecyclingFee 
    FROM vw_EnergyMetrics 
    WHERE CategoryID = 4;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_energy, @v_recycle;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextSurchargeID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_DairySurcharges (SurchargeID, OriginalProductID, ProductName, UnitsInStock, EnergySurcharge, RecyclingFee)
    VALUES (@nextSurchargeID, @v_pid, @v_pname, @v_uis, @v_energy, @v_recycle);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_EnergyMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_DairySurcharges', 'SurchargeID', CAST(@nextSurchargeID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_uis, @v_energy, @v_recycle;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeEcoComplianceReport AS
BEGIN
    IF OBJECT_ID('Final_EcoComplianceReport', 'U') IS NOT NULL DROP TABLE Final_EcoComplianceReport;
    CREATE TABLE Final_EcoComplianceReport (
        ReportID INT, 
        ProductName NVARCHAR(40), 
        TotalEnergySurcharge DECIMAL(10,2), 
        TotalRecyclingFee DECIMAL(10,2),
        ComplianceStatus VARCHAR(20)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_energy DECIMAL(10,2), @t_recycle DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, EnergySurcharge, RecyclingFee 
        FROM ##TempEcoComplianceBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_energy, @t_recycle;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_EcoComplianceReport (ReportID, ProductName, TotalEnergySurcharge, TotalRecyclingFee, ComplianceStatus)
        VALUES (@finalID, @t_pname, @t_energy, @t_recycle, 'Regulated');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempEcoComplianceBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_EcoComplianceReport', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_energy, @t_recycle;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageEcoComplianceMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempEcoComplianceBuffer') IS NOT NULL DROP TABLE ##TempEcoComplianceBuffer;
    CREATE TABLE ##TempEcoComplianceBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        EnergySurcharge DECIMAL(10,2),
        RecyclingFee DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @energy DECIMAL(10,2), @recycle DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT SurchargeID, ProductName, EnergySurcharge, RecyclingFee 
        FROM Table_DairySurcharges;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @energy, @recycle;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempEcoComplianceBuffer VALUES (@newTempID, @pname, @energy, @recycle);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_DairySurcharges', 'SurchargeID', CAST(@tid AS VARCHAR), '##TempEcoComplianceBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @energy, @recycle;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeEcoComplianceReport;
END;
GO

EXEC proc_StageEcoComplianceMetrics;