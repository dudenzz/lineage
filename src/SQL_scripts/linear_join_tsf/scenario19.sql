-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Personnel Regional Logistics Oversight & Cost Efficiency Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_PersonnelLogisticsLedger', 'U') IS NOT NULL DROP TABLE Table_PersonnelLogisticsLedger;
CREATE TABLE Table_PersonnelLogisticsLedger (
    OversightAuditID INT,
    EmployeeID INT,           -- Native Projection from Employees
    OrderID INT,              -- Native Projection from Orders
    ShipperID INT,           -- Native Projection from Shippers
    EmployeeLastName NVARCHAR(20), -- Native Projection from Employees
    WeightedFreightImpact MONEY,   -- Linearly transformed column (Orders.Freight)
    OperationalEfficiencyScore DECIMAL(18,2), -- Linearly transformed column (Employees.EmployeeID)
    CarrierServicePremium MONEY     -- Linearly transformed column (Orders.Freight)
);
GO

DECLARE @v_EmployeeID INT,
        @v_OrderID INT,
        @v_ShipperID INT,
        @v_EmployeeLastName NVARCHAR(20),
        @v_WeightedFreightImpact MONEY,
        @v_OperationalEfficiencyScore DECIMAL(18,2),
        @v_CarrierServicePremium MONEY,
        @nextOversightAuditID INT;

-- Linear Transformation Constants:
-- 1. Weighted Freight Impact: Adjusted freight to account for regional logistical overhead (y = 1.10 * Freight + 32.00)
-- 2. Operational Efficiency Score: Seniority-based index for managing transit complexity (y = 4.75 * EmployeeID + 85.00)
-- 3. Carrier Service Premium: Estimated expedited handling fee based on freight volume (y = 0.18 * Freight + 20.00)
DECLARE @FreightScalar DECIMAL(10,2) = 1.10;
DECLARE @FreightBase MONEY = 32.00;
DECLARE @EfficiencyScalar DECIMAL(10,2) = 4.75;
DECLARE @EfficiencyBase DECIMAL(10,2) = 85.00;
DECLARE @PremiumScalar DECIMAL(10,2) = 0.18;
DECLARE @PremiumBase MONEY = 20.00;

-- Cursor using JOIN to integrate Staff assignments, Order transit data, and Carrier metadata.
-- Selection: Only orders shipped to 'UK' or 'Ireland' handled by 'Sales Managers' or 'Inside Sales Coordinators'.
DECLARE OversightCursor CURSOR FOR 
    SELECT 
        E.EmployeeID, 
        O.OrderID, 
        S.ShipperID,
        E.LastName, 
        (O.Freight * @FreightScalar) + @FreightBase AS WeightedFreightImpact,
        (CAST(E.EmployeeID AS DECIMAL(18,2)) * @EfficiencyScalar) + @EfficiencyBase AS OperationalEfficiencyScore,
        (O.Freight * @PremiumScalar) + @PremiumBase AS CarrierServicePremium
    FROM Employees E
    INNER JOIN Orders O ON E.EmployeeID = O.EmployeeID
    INNER JOIN Shippers S ON O.ShipVia = S.ShipperID
    WHERE O.ShipCountry IN ('UK', 'Ireland') 
      AND E.Title IN ('Sales Manager', 'Inside Sales Coordinator'); -- Selection

OPEN OversightCursor;
FETCH NEXT FROM OversightCursor INTO 
    @v_EmployeeID, @v_OrderID, @v_ShipperID, @v_EmployeeLastName, @v_WeightedFreightImpact, @v_OperationalEfficiencyScore, @v_CarrierServicePremium;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextOversightAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_PersonnelLogisticsLedger (
        OversightAuditID, EmployeeID, OrderID, ShipperID, EmployeeLastName, WeightedFreightImpact, OperationalEfficiencyScore, CarrierServicePremium
    )
    VALUES (
        @nextOversightAuditID, @v_EmployeeID, @v_OrderID, @v_ShipperID, @v_EmployeeLastName, @v_WeightedFreightImpact, @v_OperationalEfficiencyScore, @v_CarrierServicePremium
    );

    -- Log Triple-Source Lineage
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_PersonnelLogisticsLedger', 'OversightAuditID', CAST(@nextOversightAuditID AS VARCHAR));
    
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_PersonnelLogisticsLedger', 'OversightAuditID', CAST(@nextOversightAuditID AS VARCHAR));

    -- Record source for Shippers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Shippers', 'ShipperID', CAST(@v_ShipperID AS VARCHAR), 'Table_PersonnelLogisticsLedger', 'OversightAuditID', CAST(@nextOversightAuditID AS VARCHAR));
    
    FETCH NEXT FROM OversightCursor INTO 
        @v_EmployeeID, @v_OrderID, @v_ShipperID, @v_EmployeeLastName, @v_WeightedFreightImpact, @v_OperationalEfficiencyScore, @v_CarrierServicePremium;
END;

CLOSE OversightCursor; 
DEALLOCATE OversightCursor;
GO  