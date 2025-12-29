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

SELECT * INTO Table_FulfillmentLeads 
FROM vw_OrderFulfillment 
WHERE RequiredDate IS NOT NULL;
GO

CREATE OR ALTER PROCEDURE proc_BufferAtRiskOrders AS
BEGIN
    -- Identifying late orders
    SELECT OrderID, CustomerName, ShipperName 
    INTO #TempAtRisk
    FROM Table_FulfillmentLeads
    WHERE OrderDate > RequiredDate - 21;

    EXEC proc_FinalizeAtRiskReport;
END;
GO

CREATE OR ALTER PROCEDURE proc_FinalizeAtRiskReport AS
BEGIN
    IF OBJECT_ID('dbo.Final_AtRiskLog', 'U') IS NOT NULL DROP TABLE dbo.Final_AtRiskLog;
    
    SELECT 
        OrderID, 
        CustomerName, 
        ShipperName, 
        GETDATE() as LogDate
    INTO Final_AtRiskLog
    FROM #TempAtRisk;
END;
GO

-- Execute the chain
EXEC proc_BufferAtRiskOrders;