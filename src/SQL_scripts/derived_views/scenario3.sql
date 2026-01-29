-- Section 1: Flattened De-normalized View
-- Tests: Lineage through multiple JOINs and calculated metrics across tables.
CREATE OR ALTER VIEW vw_LogisticsConsolidated AS
SELECT 
    od.OrderID,
    od.ProductID,
    p.ProductName,
    (od.UnitPrice * od.Quantity) AS LineItemTotal,
    o.ShippedDate,
    o.ShipCountry
FROM [Order Details] od
JOIN Orders o ON od.OrderID = o.OrderID
JOIN Products p ON od.ProductID = p.ProductID;
GO

-- Log Lineage: Maps detail lines back to their three parent sources
DECLARE @oid INT, @pid INT;
DECLARE LogisticsCursor CURSOR FOR SELECT OrderID, ProductID FROM vw_LogisticsConsolidated;
OPEN LogisticsCursor;
FETCH NEXT FROM LogisticsCursor INTO @oid, @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Order Details', 'OrderID-ProductID', CAST(@oid AS VARCHAR) + '-' + CAST(@pid AS VARCHAR), 'vw_LogisticsConsolidated', 'OrderID-ProductID', CAST(@oid AS VARCHAR) + '-' + CAST(@pid AS VARCHAR));
    FETCH NEXT FROM LogisticsCursor INTO @oid, @pid;
END;
CLOSE LogisticsCursor; DEALLOCATE LogisticsCursor;
GO

-- Section 2: Regional Staging Table (Filtering by Region)
-- Tests: Tool's ability to track data when a view is subsetted into a specific physical table.
IF OBJECT_ID('Table_European_Logistics', 'U') IS NOT NULL DROP TABLE Table_European_Logistics;
CREATE TABLE Table_European_Logistics (
    LogisticsID INT PRIMARY KEY,
    OriginalOrderID INT,
    OriginalProductID INT,
    VolumeMetric DECIMAL(10,2)
);

DECLARE @e_oid INT, @e_pid INT, @e_total DECIMAL(10,2), @nextLogID INT;
DECLARE EuroCursor CURSOR FOR 
    SELECT OrderID, ProductID, LineItemTotal FROM vw_LogisticsConsolidated 
    WHERE ShipCountry IN ('Germany', 'France', 'UK', 'Belgium');

OPEN EuroCursor;
FETCH NEXT FROM EuroCursor INTO @e_oid, @e_pid, @e_total;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLogID = NEXT VALUE FOR GlobalIDSequence;
    INSERT INTO Table_European_Logistics (LogisticsID, OriginalOrderID, OriginalProductID, VolumeMetric)
    VALUES (@nextLogID, @e_oid, @e_pid, @e_total);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_LogisticsConsolidated', 'OrderID-ProductID', CAST(@e_oid AS VARCHAR) + '-' + CAST(@e_pid AS VARCHAR), 'Table_European_Logistics', 'LogisticsID', CAST(@nextLogID AS VARCHAR));
    
    FETCH NEXT FROM EuroCursor INTO @e_oid, @e_pid, @e_total;
END;
CLOSE EuroCursor; DEALLOCATE EuroCursor;
GO

-- Section 3: Procedural Analysis Table
-- Tests: Tracking lineage through a procedure that applies a secondary transformation.
CREATE OR ALTER PROCEDURE proc_FinalizeLogisticsAudit AS
BEGIN
    IF OBJECT_ID('Final_Logistics_Audit_Report', 'U') IS NOT NULL DROP TABLE Final_Logistics_Audit_Report;
    CREATE TABLE Final_Logistics_Audit_Report (AuditID INT, OrderRef INT, ProductRef INT, AuditStatus NVARCHAR(20));

    DECLARE @a_oid INT, @a_pid INT, @a_logID INT, @a_finalID INT;
    DECLARE AuditCursor CURSOR FOR SELECT LogisticsID, OriginalOrderID, OriginalProductID FROM Table_European_Logistics;

    OPEN AuditCursor;
    FETCH NEXT FROM AuditCursor INTO @a_logID, @a_oid, @a_pid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @a_finalID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO Final_Logistics_Audit_Report (AuditID, OrderRef, ProductRef, AuditStatus)
        VALUES (@a_finalID, @a_oid, @a_pid, 'Verified');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_European_Logistics', 'LogisticsID', CAST(@a_logID AS VARCHAR), 'Final_Logistics_Audit_Report', 'AuditID', CAST(@a_finalID AS VARCHAR));

        FETCH NEXT FROM AuditCursor INTO @a_logID, @a_oid, @a_pid;
    END;
    CLOSE AuditCursor; DEALLOCATE AuditCursor;
END;
GO

EXEC proc_FinalizeLogisticsAudit;