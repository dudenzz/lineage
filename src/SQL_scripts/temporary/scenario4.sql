-- Section 1: Main Batch Initialization
-- Tests: Lineage from physical source to a temp table that will be passed to a procedure.
IF OBJECT_ID('tempdb..#ShipperPerformance') IS NOT NULL DROP TABLE #ShipperPerformance;

CREATE TABLE #ShipperPerformance (
    ShipperID INT,
    CompanyName NVARCHAR(40),
    TotalOrdersProcessed INT DEFAULT 0,
    EfficiencyScore DECIMAL(5,2)
);

INSERT INTO #ShipperPerformance (ShipperID, CompanyName)
SELECT ShipperID, CompanyName
FROM Shippers;

-- Log Lineage: Shippers -> #ShipperPerformance
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Shippers', 'ShipperID', CAST(ShipperID AS VARCHAR), '#ShipperPerformance', 'ShipperID', CAST(ShipperID AS VARCHAR)
FROM #ShipperPerformance;
GO

-- Section 2: The "Child" Enrichment Procedure
-- Tests: Tool's ability to resolve references to a temp table created OUTSIDE the procedure.
CREATE OR ALTER PROCEDURE proc_EnrichShipperStats AS
BEGIN
    -- This procedure assumes #ShipperPerformance exists in the calling scope.
    -- Tests: Out-of-scope object resolution.
    UPDATE sp
    SET sp.TotalOrdersProcessed = (SELECT COUNT(*) FROM Orders o WHERE o.ShipVia = sp.ShipperID),
        sp.EfficiencyScore = 1.0 -- Simplified logic
    FROM #ShipperPerformance sp;

    -- Log Lineage: Orders -> #ShipperPerformance (via Update)
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT DISTINCT 'Orders', 'ShipVia', CAST(sp.ShipperID AS VARCHAR), '#ShipperPerformance', 'ShipperID', CAST(sp.ShipperID AS VARCHAR)
    FROM #ShipperPerformance sp;
END;
GO

-- Section 3: Execution and Final Move
-- Tests: Execution of the call stack and final persistence.
EXEC proc_EnrichShipperStats;

IF OBJECT_ID('Final_Shipper_Audit', 'U') IS NOT NULL DROP TABLE Final_Shipper_Audit;
CREATE TABLE Final_Shipper_Audit (
    AuditID INT PRIMARY KEY,
    ShipperName NVARCHAR(40),
    OrderCount INT
);

DECLARE @s_name NVARCHAR(40), @s_count INT, @nextAuditID INT, @s_id INT;
DECLARE FinalCursor CURSOR FOR SELECT ShipperID, CompanyName, TotalOrdersProcessed FROM #ShipperPerformance;

OPEN FinalCursor;
FETCH NEXT FROM FinalCursor INTO @s_id, @s_name, @s_count;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Final_Shipper_Audit (AuditID, ShipperName, OrderCount)
    VALUES (@nextAuditID, @s_name, @s_count);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('#ShipperPerformance', 'ShipperID', CAST(@s_id AS VARCHAR), 'Final_Shipper_Audit', 'AuditID', CAST(@nextAuditID AS VARCHAR));

    FETCH NEXT FROM FinalCursor INTO @s_id, @s_name, @s_count;
END;
CLOSE FinalCursor; DEALLOCATE FinalCursor;

DROP TABLE #ShipperPerformance;
GO