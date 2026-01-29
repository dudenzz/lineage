-- Section 1: Create a View with Pivot-like Logic
-- Tests: Attribute-level lineage where one source column (Month) determines the destination of another (Revenue).
CREATE OR ALTER VIEW vw_ProductSeasonalRevenue AS
SELECT 
    p.ProductID,
    p.ProductName,
    SUM(CASE WHEN MONTH(o.OrderDate) BETWEEN 3 AND 5 THEN (od.UnitPrice * od.Quantity) ELSE 0 END) AS SpringRevenue,
    SUM(CASE WHEN MONTH(o.OrderDate) BETWEEN 6 AND 8 THEN (od.UnitPrice * od.Quantity) ELSE 0 END) AS SummerRevenue,
    SUM(CASE WHEN MONTH(o.OrderDate) IN (12, 1, 2) THEN (od.UnitPrice * od.Quantity) ELSE 0 END) AS WinterRevenue
FROM Products p
JOIN [Order Details] od ON p.ProductID = od.ProductID
JOIN Orders o ON od.OrderID = o.OrderID
GROUP BY p.ProductID, p.ProductName;
GO

-- Log Lineage: Captures the join between Product and Sales data
DECLARE @pid INT;
DECLARE PivotCursor CURSOR FOR SELECT ProductID FROM vw_ProductSeasonalRevenue;
OPEN PivotCursor;
FETCH NEXT FROM PivotCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_ProductSeasonalRevenue', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM PivotCursor INTO @pid;
END;
CLOSE PivotCursor; DEALLOCATE PivotCursor;
GO

-- Section 2: Persistence into a Validation Table
-- Tests: Lineage tracking through math operations (summing different seasonal columns).
IF OBJECT_ID('Table_Annual_Performance', 'U') IS NOT NULL DROP TABLE Table_Annual_Performance;
CREATE TABLE Table_Annual_Performance (
    PerfID INT PRIMARY KEY,
    ProductID INT,
    TotalCalculatedYearlyRevenue DECIMAL(18,2)
);

DECLARE @v_pid INT, @v_spring DECIMAL(18,2), @v_summer DECIMAL(18,2), @v_winter DECIMAL(18,2), @nextPerfID INT;
DECLARE PerformanceCursor CURSOR FOR SELECT ProductID, SpringRevenue, SummerRevenue, WinterRevenue FROM vw_ProductSeasonalRevenue;

OPEN PerformanceCursor;
FETCH NEXT FROM PerformanceCursor INTO @v_pid, @v_spring, @v_summer, @v_winter;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextPerfID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Creation involves adding the three derived view columns
    INSERT INTO Table_Annual_Performance (PerfID, ProductID, TotalCalculatedYearlyRevenue)
    VALUES (@nextPerfID, @v_pid, @v_spring + @v_summer + @v_winter);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ProductSeasonalRevenue', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Annual_Performance', 'PerfID', CAST(@nextPerfID AS VARCHAR));
    
    FETCH NEXT FROM PerformanceCursor INTO @v_pid, @v_spring, @v_summer, @v_winter;
END;
CLOSE PerformanceCursor; DEALLOCATE PerformanceCursor;
GO

-- Section 3: Stored Procedure for Historical Archiving
-- Tests: Complex lineage where the procedure finalizes the record for a different "Reporting" context.
CREATE OR ALTER PROCEDURE proc_ArchiveProductPerformance AS
BEGIN
    IF OBJECT_ID('Final_Product_History_Archive', 'U') IS NOT NULL DROP TABLE Final_Product_History_Archive;
    CREATE TABLE Final_Product_History_Archive (
        ArchiveID INT, 
        ProductID INT, 
        FinalRevenue DECIMAL(18,2), 
        ArchiveTimestamp DATETIME
    );

    DECLARE @a_pid INT, @a_perfID INT, @a_rev DECIMAL(18,2), @a_finalID INT;
    DECLARE ArchiveCursor CURSOR FOR SELECT PerfID, ProductID, TotalCalculatedYearlyRevenue FROM Table_Annual_Performance;

    OPEN ArchiveCursor;
    FETCH NEXT FROM ArchiveCursor INTO @a_perfID, @a_pid, @a_rev;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @a_finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_Product_History_Archive (ArchiveID, ProductID, FinalRevenue, ArchiveTimestamp)
        VALUES (@a_finalID, @a_pid, @a_rev, GETDATE());

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Annual_Performance', 'PerfID', CAST(@a_perfID AS VARCHAR), 'Final_Product_History_Archive', 'ArchiveID', CAST(@a_finalID AS VARCHAR));

        FETCH NEXT FROM ArchiveCursor INTO @a_perfID, @a_pid, @a_rev;
    END;
    CLOSE ArchiveCursor; DEALLOCATE ArchiveCursor;
END;
GO

EXEC proc_ArchiveProductPerformance;