-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Logistics & Shipper Performance Audit Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_LogisticsPerformanceLedger', 'U') IS NOT NULL DROP TABLE Table_LogisticsPerformanceLedger;
CREATE TABLE Table_LogisticsPerformanceLedger (
    AuditID INT,
    OrderID INT,             -- Native Projection from Orders
    ShipperID INT,           -- Native Projection from Shippers
    ShipName NVARCHAR(40),   -- Native Projection from Orders
    ShipperCompany NVARCHAR(40), -- Native Projection from Shippers
    Freight MONEY,           -- Native Projection from Orders
    ShipRegion NVARCHAR(15)  -- Native Projection from Orders
);
GO

DECLARE @v_OrderID INT,
        @v_ShipperID INT,
        @v_ShipName NVARCHAR(40),
        @v_ShipperCompany NVARCHAR(40),
        @v_Freight MONEY,
        @v_ShipRegion NVARCHAR(15),
        @nextAuditID INT;

-- Cursor using JOIN for strict projection across Order fulfillment and Logistics providers.
-- Selection: Only orders shipped to the 'RJ' or 'SP' regions in Brazil via ShipperID 2 (United Package).
-- All attributes are native; no linear transformations, rounding, or regional aliasing applied.
DECLARE LogisticsAuditCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        S.ShipperID, 
        O.ShipName, 
        S.CompanyName, 
        O.Freight,
        O.ShipRegion
    FROM Orders O
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    WHERE O.ShipCountry = 'Brazil' 
      AND O.ShipRegion IN ('RJ', 'SP')
      AND S.ShipperID = 2; -- Selection

OPEN LogisticsAuditCursor;
FETCH NEXT FROM LogisticsAuditCursor INTO 
    @v_OrderID, @v_ShipperID, @v_ShipName, @v_ShipperCompany, @v_Freight, @v_ShipRegion;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data (No Transformations)
    INSERT INTO Table_LogisticsPerformanceLedger (
        AuditID, OrderID, ShipperID, ShipName, ShipperCompany, Freight, ShipRegion
    )
    VALUES (
        @nextAuditID, @v_OrderID, @v_ShipperID, @v_ShipName, @v_ShipperCompany, @v_Freight, @v_ShipRegion
    );

    -- Log Dual-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_LogisticsPerformanceLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_LogisticsPerformanceLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM LogisticsAuditCursor INTO 
        @v_OrderID, @v_ShipperID, @v_ShipName, @v_ShipperCompany, @v_Freight, @v_ShipRegion;
END;

CLOSE LogisticsAuditCursor; 
DEALLOCATE LogisticsAuditCursor;
GO