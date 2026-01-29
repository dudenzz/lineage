-- Section 0: Infrastructure Setup
IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'GlobalIDSequence')
    CREATE SEQUENCE GlobalIDSequence START WITH 1 INCREMENT BY 1;
GO

IF OBJECT_ID('Table_Processed_Orders_Archive', 'U') IS NOT NULL DROP TABLE Table_Processed_Orders_Archive;
CREATE TABLE Table_Processed_Orders_Archive (OrderID INT PRIMARY KEY);

-- Pre-populate archive with some existing IDs
INSERT INTO Table_Processed_Orders_Archive (OrderID)
SELECT TOP 10 OrderID FROM Orders;
GO

-- Section 1: The Incremental View
CREATE OR ALTER VIEW vw_UniqueNewOrders AS
SELECT OrderID FROM Orders
EXCEPT
SELECT OrderID FROM Table_Processed_Orders_Archive;
GO

-- Log Lineage for the View
-- Note: 'Orders' is the data source, 'Table_Processed_Orders_Archive' is the filter dependency
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
VALUES ('Orders', 'OrderID', 'SourceData', 'vw_UniqueNewOrders', 'OrderID', 'SetResult');

INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
VALUES ('Table_Processed_Orders_Archive', 'OrderID', 'FilterConstraint', 'vw_UniqueNewOrders', 'OrderID', 'SetResult');
GO

-- Section 2: Materializing to Staging
IF OBJECT_ID('Table_Order_Staging_Buffer', 'U') IS NOT NULL DROP TABLE Table_Order_Staging_Buffer;
CREATE TABLE Table_Order_Staging_Buffer (
    BufferID INT PRIMARY KEY,
    OriginalOrderID INT,
    SourceSystem NVARCHAR(50)
);

INSERT INTO Table_Order_Staging_Buffer (BufferID, OriginalOrderID, SourceSystem)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    OrderID,
    'Northwind_Incremental'
FROM vw_UniqueNewOrders;

-- Log Lineage for the Buffer
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'vw_UniqueNewOrders', 'OrderID', CAST(OrderID AS VARCHAR), 'Table_Order_Staging_Buffer', 'BufferID', 'InternalID'
FROM vw_UniqueNewOrders;
GO

-- Section 3: Final Sync Procedure
CREATE OR ALTER PROCEDURE proc_SyncOrderArchive AS
BEGIN
    IF OBJECT_ID('Final_Synced_Order_Report', 'U') IS NOT NULL DROP TABLE Final_Synced_Order_Report;
    CREATE TABLE Final_Synced_Order_Report (
        SyncID INT PRIMARY KEY, 
        OrderRef INT, 
        SyncStatus NVARCHAR(20)
    );

    INSERT INTO Final_Synced_Order_Report (SyncID, OrderRef, SyncStatus)
    SELECT 
        BufferID,
        OriginalOrderID,
        'Newly_Synced'
    FROM Table_Order_Staging_Buffer;

    -- Log Lineage for the Final Report
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Order_Staging_Buffer', 'BufferID', CAST(BufferID AS VARCHAR), 'Final_Synced_Order_Report', 'SyncID', 'Final'
    FROM Table_Order_Staging_Buffer;
END;
GO

-- Execute and verify
EXEC proc_SyncOrderArchive;