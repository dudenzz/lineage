-- Section 1: Create a View based on a Join and Aggregation
-- Tests: Lineage through joins and many-to-one transformations (Aggregations).
CREATE OR ALTER VIEW vw_OrderSalesSummary AS
SELECT 
    o.OrderID, 
    o.CustomerID, 
    SUM(od.UnitPrice * od.Quantity) AS GrossSales
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
GROUP BY o.OrderID, o.CustomerID;
GO

-- Log Lineage: Captures relationship from Orders to the View
-- Note: In a real benchmark, we log the primary keys that contributed to the view record.
DECLARE @oid INT;
DECLARE ViewCursor CURSOR FOR SELECT OrderID FROM vw_OrderSalesSummary;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_OrderSalesSummary', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @oid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a Derived View from the previous view
-- Tests: Multi-level view dependency (View-on-View lineage).
CREATE OR ALTER VIEW vw_HighValueOrders AS
SELECT 
    OrderID, 
    CustomerID, 
    GrossSales,
    'High-Priority' AS PriorityLevel
FROM vw_OrderSalesSummary
WHERE GrossSales > 500;
GO

-- Log Lineage: Captures relationship between the two views
DECLARE @h_oid INT;
DECLARE HighValCursor CURSOR FOR SELECT OrderID FROM vw_HighValueOrders;
OPEN HighValCursor;
FETCH NEXT FROM HighValCursor INTO @h_oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_OrderSalesSummary', 'OrderID', CAST(@h_oid AS VARCHAR), 'vw_HighValueOrders', 'OrderID', CAST(@h_oid AS VARCHAR));
    FETCH NEXT FROM HighValCursor INTO @h_oid;
END;
CLOSE HighValCursor; DEALLOCATE HighValCursor;
GO

-- Section 3: Final Persistance into a Physical Table
-- Tests: The tool's ability to resolve the full chain: Table -> View -> View -> Table.
IF OBJECT_ID('Final_Sales_Report', 'U') IS NOT NULL DROP TABLE Final_Sales_Report;
CREATE TABLE Final_Sales_Report (
    ReportID INT PRIMARY KEY,
    OriginalOrderID INT,
    Revenue DECIMAL(10,2),
    ProcessingDate DATETIME DEFAULT GETDATE()
);

DECLARE @f_oid INT, @f_sales DECIMAL(10,2), @nextReportID INT;
DECLARE FinalReportCursor CURSOR FOR SELECT OrderID, GrossSales FROM vw_HighValueOrders;

OPEN FinalReportCursor;
FETCH NEXT FROM FinalReportCursor INTO @f_oid, @f_sales;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextReportID = NEXT VALUE FOR GlobalIDSequence; -- Assuming sequence exists from your sample context
    
    INSERT INTO Final_Sales_Report (ReportID, OriginalOrderID, Revenue)
    VALUES (@nextReportID, @f_oid, @f_sales);

    -- Log Lineage: Target is the final physical object
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_HighValueOrders', 'OrderID', CAST(@f_oid AS VARCHAR), 'Final_Sales_Report', 'ReportID', CAST(@nextReportID AS VARCHAR));
    
    FETCH NEXT FROM FinalReportCursor INTO @f_oid, @f_sales;
END;
CLOSE FinalReportCursor; DEALLOCATE FinalReportCursor;
GO