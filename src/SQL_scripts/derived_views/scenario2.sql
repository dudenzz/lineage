-- Section 1: Create a View with conditional logic (CASE statements)
-- Tests: Lineage tracking when new columns are derived from existing data points.
CREATE OR ALTER VIEW vw_CustomerShippingTiers AS
SELECT 
    o.OrderID,
    o.CustomerID,
    o.Freight,
    CASE 
        WHEN o.Freight > 100 THEN 'Premium'
        WHEN o.Freight > 50 THEN 'Standard'
        ELSE 'Economy'
    END AS ShippingTier,
    c.ContactName,
    c.Country
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID;
GO

-- Log Row-Level Lineage for View
DECLARE @oid INT;
DECLARE TierCursor CURSOR FOR SELECT OrderID FROM vw_CustomerShippingTiers;
OPEN TierCursor;
FETCH NEXT FROM TierCursor INTO @oid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@oid AS VARCHAR), 'vw_CustomerShippingTiers', 'OrderID', CAST(@oid AS VARCHAR));
    FETCH NEXT FROM TierCursor INTO @oid;
END;
CLOSE TierCursor; DEALLOCATE TierCursor;
GO

-- Section 2: Create a physical table for high-freight shipments
-- Tests: Lineage extraction from a view with a complex WHERE filter.
IF OBJECT_ID('Table_Premium_Shipments', 'U') IS NOT NULL DROP TABLE Table_Premium_Shipments;
CREATE TABLE Table_Premium_Shipments (
    ShipmentID INT PRIMARY KEY,
    OrderID INT,
    CustomerName NVARCHAR(100),
    FreightAmount DECIMAL(10,2)
);

DECLARE @p_oid INT, @p_name NVARCHAR(100), @p_freight DECIMAL(10,2), @nextShipID INT;
DECLARE PremiumCursor CURSOR FOR 
    SELECT OrderID, ContactName, Freight FROM vw_CustomerShippingTiers 
    WHERE ShippingTier = 'Premium';

OPEN PremiumCursor;
FETCH NEXT FROM PremiumCursor INTO @p_oid, @p_name, @p_freight;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextShipID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Premium_Shipments (ShipmentID, OrderID, CustomerName, FreightAmount)
    VALUES (@nextShipID, @p_oid, @p_name, @p_freight);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_CustomerShippingTiers', 'OrderID', CAST(@p_oid AS VARCHAR), 'Table_Premium_Shipments', 'ShipmentID', CAST(@nextShipID AS VARCHAR));
    
    FETCH NEXT FROM PremiumCursor INTO @p_oid, @p_name, @p_freight;
END;
CLOSE PremiumCursor; DEALLOCATE PremiumCursor;
GO

-- Section 3: Final Procedure for Regional Aggregation
-- Tests: Ability to resolve lineage across three levels of transformation within a procedure.
CREATE OR ALTER PROCEDURE proc_ArchiveRegionalSales AS
BEGIN
    IF OBJECT_ID('Final_Regional_Archive', 'U') IS NOT NULL DROP TABLE Final_Regional_Archive;
    CREATE TABLE Final_Regional_Archive (ArchiveID INT, OrderID INT, RegionStatus VARCHAR(20));

    DECLARE @arch_oid INT, @arch_id INT;
    DECLARE ArchiveCursor CURSOR FOR SELECT OrderID FROM Table_Premium_Shipments;

    OPEN ArchiveCursor;
    FETCH NEXT FROM ArchiveCursor INTO @arch_oid;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @arch_id = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_Regional_Archive (ArchiveID, OrderID, RegionStatus)
        VALUES (@arch_id, @arch_oid, 'Archived');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Premium_Shipments', 'ShipmentID', CAST(@arch_oid AS VARCHAR), 'Final_Regional_Archive', 'ArchiveID', CAST(@arch_id AS VARCHAR));

        FETCH NEXT FROM ArchiveCursor INTO @arch_oid;
    END;
    CLOSE ArchiveCursor; DEALLOCATE ArchiveCursor;
END;
GO

EXEC proc_ArchiveRegionalSales;