-- Section 1: Aggregate raw sales into a view
-- Logic: Join 3 tables and aggregate.
CREATE OR ALTER VIEW vw_EmployeeSalesSummary AS
SELECT 
    e.EmployeeID, e.LastName,
    SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalSales,
    COUNT(o.OrderID) AS OrderCount
FROM Employees e
JOIN Orders o ON e.EmployeeID = o.EmployeeID
JOIN [Order Details] od ON o.OrderID = od.OrderID
GROUP BY e.EmployeeID, e.LastName;
GO

-- TRACK LINEAGE FOR VIEW: Since aggregation collapses many rows into one, 
-- we log the relationship from the base Employee record to the view grain.
DECLARE @eid_v INT;
DECLARE ViewLineageCursor CURSOR FOR SELECT EmployeeID FROM vw_EmployeeSalesSummary;
OPEN ViewLineageCursor;
FETCH NEXT FROM ViewLineageCursor INTO @eid_v;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@eid_v AS VARCHAR), 'vw_EmployeeSalesSummary', 'EmployeeID', CAST(@eid_v AS VARCHAR));
    FETCH NEXT FROM ViewLineageCursor INTO @eid_v;
END;
CLOSE ViewLineageCursor; DEALLOCATE ViewLineageCursor;
GO

-- Section 2: Transform view data into a physical table
IF OBJECT_ID('Table_SalesPerformance', 'U') IS NOT NULL DROP TABLE Table_SalesPerformance;
CREATE TABLE Table_SalesPerformance (EmpID INT, FullName NVARCHAR(50), Revenue DECIMAL(18,2), PerformanceTier VARCHAR(20), LineageID INT);

DECLARE @eid INT, @name NVARCHAR(50), @rev DECIMAL(18,2), @lid INT;
DECLARE SalesCursor CURSOR FOR SELECT EmployeeID, LastName, TotalSales FROM vw_EmployeeSalesSummary;
OPEN SalesCursor;
FETCH NEXT FROM SalesCursor INTO @eid, @name, @rev;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @lid = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_SalesPerformance (EmpID, FullName, Revenue, PerformanceTier, LineageID)
    VALUES (@eid, @name, @rev, CASE WHEN @rev > 15000 THEN 'Elite' ELSE 'Standard' END, @lid);

    -- TRACK LINEAGE: View -> Physical Table
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_EmployeeSalesSummary', 'EmployeeID', CAST(@eid AS VARCHAR), 'Table_SalesPerformance', 'LineageID', CAST(@lid AS VARCHAR));
    
    FETCH NEXT FROM SalesCursor INTO @eid, @name, @rev;
END;
CLOSE SalesCursor; DEALLOCATE SalesCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temp Tables
CREATE OR ALTER PROCEDURE proc_StageHighPerformers AS
BEGIN
    IF OBJECT_ID('tempdb..##TempEliteSales') IS NOT NULL DROP TABLE ##TempEliteSales;
    CREATE TABLE ##TempEliteSales (EmpID INT, Revenue DECIMAL(18,2), StageID INT);
    
    DECLARE @eid INT, @rev DECIMAL(18,2), @parentLid INT, @newID INT;
    DECLARE EliteCursor CURSOR FOR SELECT EmpID, Revenue, LineageID FROM Table_SalesPerformance WHERE PerformanceTier = 'Elite';
    
    OPEN EliteCursor;
    FETCH NEXT FROM EliteCursor INTO @eid, @rev, @parentLid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempEliteSales VALUES (@eid, @rev, @newID);

        -- TRACK LINEAGE: Physical Table -> Temp Table
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_SalesPerformance', 'LineageID', CAST(@parentLid AS VARCHAR), '##TempEliteSales', 'StageID', CAST(@newID AS VARCHAR));
        
        FETCH NEXT FROM EliteCursor INTO @eid, @rev, @parentLid;
    END;
    CLOSE EliteCursor; DEALLOCATE EliteCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_GenerateCommissionReport AS
BEGIN
    IF OBJECT_ID('Final_CommissionReport', 'U') IS NOT NULL DROP TABLE Final_CommissionReport;
    CREATE TABLE Final_CommissionReport (ReportID INT, EmpID INT, CommissionAmount DECIMAL(18,2), ParentLineageID INT);
    
    EXEC proc_StageHighPerformers;
    
    DECLARE @eid INT, @rev DECIMAL(18,2), @sid INT, @reportID INT;
    DECLARE FinalCursor CURSOR FOR SELECT EmpID, Revenue, StageID FROM ##TempEliteSales;
    
    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @eid, @rev, @sid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @reportID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_CommissionReport (ReportID, EmpID, CommissionAmount, ParentLineageID)
        VALUES (@reportID, @eid, @rev * 0.05, @sid);

        -- TRACK LINEAGE: Temp Table -> Final Report Table
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempEliteSales', 'StageID', CAST(@sid AS VARCHAR), 'Final_CommissionReport', 'ReportID', CAST(@reportID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @eid, @rev, @sid;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

-- Execute the process
EXEC proc_GenerateCommissionReport;