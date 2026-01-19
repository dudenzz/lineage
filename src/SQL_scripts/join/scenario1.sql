
-- ==========================================================
-- Level 0: Create a view
-- ==========================================================
CREATE VIEW vw_OrderFulfillment AS
SELECT 
    o.OrderID, 
    c.CompanyName AS CustomerName, 
    s.CompanyName AS ShipperName,
    o.OrderDate,
    o.RequiredDate
FROM Orders o
INNER JOIN Customers c ON o.CustomerID = c.CustomerID
INNER JOIN Shippers s ON o.ShipVia = s.ShipperID;
GO
-- ==========================================================
-- Level 1: View Simulation & Multi-Source Tracking
-- ==========================================================
GO
DECLARE @orderID INT, @custName NVARCHAR(40), @shipName NVARCHAR(40), @oDate DATETIME, @rDate DATETIME;

DECLARE JoinViewCursor CURSOR FOR
SELECT o.OrderID, c.CompanyName, s.CompanyName, o.OrderDate, o.RequiredDate
FROM Orders o
INNER JOIN Customers c ON o.CustomerID = c.CustomerID
INNER JOIN Shippers s ON o.ShipVia = s.ShipperID;

OPEN JoinViewCursor;
FETCH NEXT FROM JoinViewCursor INTO @orderID, @custName, @shipName, @oDate, @rDate;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- 1. Track contribution from 'Orders'
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@orderID AS VARCHAR), 'vw_OrderFulfillment', 'OrderID', CAST(@orderID AS VARCHAR));
    
    -- 2. Track contribution from 'Customers' (via CompanyName/ID)
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CompanyName', @custName, 'vw_OrderFulfillment', 'OrderID', CAST(@orderID AS VARCHAR));
    
    -- 3. Track contribution from 'Shippers'
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'CompanyName', @shipName, 'vw_OrderFulfillment', 'OrderID', CAST(@orderID AS VARCHAR));

    FETCH NEXT FROM JoinViewCursor INTO @orderID, @custName, @shipName, @oDate, @rDate;
END;
CLOSE JoinViewCursor; DEALLOCATE JoinViewCursor;

-- ==========================================================
-- Level 2: Table Creation (Table_FulfillmentLeads)
-- ==========================================================
GO
DECLARE @nextGlobID INT;
IF OBJECT_ID('dbo.Table_FulfillmentLeads', 'U') IS NOT NULL DROP TABLE dbo.Table_FulfillmentLeads;

-- Create table structure
SELECT TOP 0 OrderID, CustomerName, ShipperName, OrderDate, RequiredDate 
INTO Table_FulfillmentLeads FROM (
    SELECT o.OrderID, c.CompanyName AS CustomerName, s.CompanyName AS ShipperName, o.OrderDate, o.RequiredDate
    FROM Orders o INNER JOIN Customers c ON o.CustomerID = c.CustomerID INNER JOIN Shippers s ON o.ShipVia = s.ShipperID
) x;
ALTER TABLE Table_FulfillmentLeads ADD GlobalID INT;

DECLARE @fOrderID INT, @fCust NVARCHAR(40), @fShip NVARCHAR(40), @foDate DATETIME, @frDate DATETIME;

DECLARE LeadsCursor CURSOR FOR 
SELECT OrderID, c.CompanyName, s.CompanyName as ShipperName, OrderDate, RequiredDate 
FROM Orders o 
INNER JOIN Customers c ON o.CustomerID = c.CustomerID 
INNER JOIN Shippers s ON o.ShipVia = s.ShipperID
WHERE o.RequiredDate IS NOT NULL;

OPEN LeadsCursor;
FETCH NEXT FROM LeadsCursor INTO @fOrderID, @fCust, @fShip, @foDate, @frDate;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextGlobID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_FulfillmentLeads (OrderID, CustomerName, ShipperName, OrderDate, RequiredDate, GlobalID)
    VALUES (@fOrderID, @fCust, @fShip, @foDate, @frDate, @nextGlobID);

    -- Record lineage from View logic to the physical Table
    INSERT INTO DataLineage VALUES ('vw_OrderFulfillment', 'OrderID', CAST(@fOrderID AS VARCHAR), 'Table_FulfillmentLeads', 'GlobalID', CAST(@nextGlobID AS VARCHAR));

    FETCH NEXT FROM LeadsCursor INTO @fOrderID, @fCust, @fShip, @foDate, @frDate;
END;
CLOSE LeadsCursor; DEALLOCATE LeadsCursor;

-- ==========================================================
-- Level 3 & 4: Procedures (Temp Table and Final Log)
-- ==========================================================
GO
CREATE OR ALTER PROCEDURE proc_FinalizeAtRiskReport AS
BEGIN
    DECLARE @oid INT, @cname VARCHAR(255), @sname VARCHAR(255), @gid INT;
    
    IF OBJECT_ID('dbo.Final_AtRiskLog', 'U') IS NOT NULL DROP TABLE dbo.Final_AtRiskLog;
    CREATE TABLE Final_AtRiskLog (OrderID INT, CustomerName VARCHAR(255), ShipperName VARCHAR(255), LogDate DATETIME, ParentGID INT);

    DECLARE FinalCursor CURSOR FOR SELECT OrderID, CustomerName, ShipperName, GlobalID FROM ##TempAtRisk;
    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @oid, @cname, @sname, @gid;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        INSERT INTO Final_AtRiskLog (OrderID, CustomerName, ShipperName, LogDate, ParentGID)
        VALUES (@oid, @cname, @sname, GETDATE(), @gid);

        INSERT INTO DataLineage VALUES ('##TempAtRisk', 'GlobalID', CAST(@gid AS VARCHAR), 'Final_AtRiskLog', 'OrderID', CAST(@oid AS VARCHAR));
        FETCH NEXT FROM FinalCursor INTO @oid, @cname, @sname, @gid;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_BufferAtRiskOrders AS
BEGIN
    IF OBJECT_ID('tempdb..##TempAtRisk', 'U') IS NOT NULL DROP TABLE ##TempAtRisk;
    CREATE TABLE ##TempAtRisk (OrderID INT, CustomerName VARCHAR(255), ShipperName VARCHAR(255), GlobalID INT);

    DECLARE @oid INT, @cname VARCHAR(255), @sname VARCHAR(255), @gid INT, @newGID INT;
    
    DECLARE BufferCursor CURSOR FOR 
    SELECT OrderID, CustomerName, ShipperName, GlobalID FROM Table_FulfillmentLeads
    WHERE OrderDate > RequiredDate;

    OPEN BufferCursor;
    FETCH NEXT FROM BufferCursor INTO @oid, @cname, @sname, @gid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newGID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempAtRisk VALUES (@oid, @cname, @sname, @newGID);

        INSERT INTO DataLineage VALUES ('Table_FulfillmentLeads', 'GlobalID', CAST(@gid AS VARCHAR), '##TempAtRisk', 'GlobalID', CAST(@newGID AS VARCHAR));
        FETCH NEXT FROM BufferCursor INTO @oid, @cname, @sname, @gid;
    END;
    CLOSE BufferCursor; DEALLOCATE BufferCursor;

    EXEC proc_FinalizeAtRiskReport;
END;
GO

-- Execute the lineage chain
EXEC proc_BufferAtRiskOrders;