-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Regional Sustainability & Logistics Scalability Ledger.
-- Rule: UNION ALL between different tables (Shippers and Customers), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Logistics Scalability Credits" for shippers and "Sustainability Participation Fees" for regional customers.

IF OBJECT_ID('Table_SustainabilityScalabilityLedger', 'U') IS NOT NULL DROP TABLE Table_SustainabilityScalabilityLedger;
CREATE TABLE Table_SustainabilityScalabilityLedger (
    EntryID INT,
    AuditSector VARCHAR(25),
    SourcePKID NVARCHAR(15),
    EntityLabel NVARCHAR(40),
    AdjustmentValue DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_AuditSector VARCHAR(25),
        @v_SourcePKID NVARCHAR(15),
        @v_EntityLabel NVARCHAR(40),
        @v_AdjustmentValue DECIMAL(18, 2),
        @nextEntryID INT;

-- Linear Transformation Constants:
-- 1. Scalability (Shippers): Credit based on ShipperID (proxy for network nodes) plus a flat infrastructure bonus (y = 5.50 * ShipperID + 100.00)
-- 2. Sustainability (Customers): Fee based on a flat regional environmental levy (y = 0 * x + 45.00)
DECLARE @ScalabilityScalar DECIMAL(10,2) = 5.50;
DECLARE @ScalabilityBaseBonus DECIMAL(10,2) = 100.00;
DECLARE @SustainabilityRegionalLevy DECIMAL(10,2) = 45.00;

-- Cursor combining DIFFERENT tables (Shippers and Customers) via UNION ALL
DECLARE SustainabilityScalabilityCursor CURSOR FOR 
    -- Branch 1: Shippers (Logistics Network Scalability)
    -- Selection: All shippers (Infrastructure assessment)
    -- Transformation: Linear scalability credit (y = 5.50 * x + 100.00)
    SELECT 
        'LogisticsScalability' AS AuditSector, 
        CAST(ShipperID AS NVARCHAR(15)) AS SourcePKID, 
        CompanyName AS EntityLabel, 
        (CAST(ShipperID AS DECIMAL(18,2)) * @ScalabilityScalar) + @ScalabilityBaseBonus AS AdjustmentValue 
    FROM Shippers
    
    UNION ALL

    -- Branch 2: Customers (Regional Sustainability Program)
    -- Selection: Only customers in Scandinavia (Denmark, Norway, Sweden, Finland)
    -- Transformation: Constant linear levy for regional compliance (y = 0 * x + 45.00)
    SELECT 
        'SustainabilityLevy' AS AuditSector, 
        CAST(CustomerID AS NVARCHAR(15)) AS SourcePKID, 
        CompanyName AS EntityLabel, 
        @SustainabilityRegionalLevy AS AdjustmentValue 
    FROM Customers
    WHERE Country IN ('Denmark', 'Norway', 'Sweden', 'Finland'); -- Selection

OPEN SustainabilityScalabilityCursor;
FETCH NEXT FROM SustainabilityScalabilityCursor INTO @v_AuditSector, @v_SourcePKID, @v_EntityLabel, @v_AdjustmentValue;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextEntryID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_SustainabilityScalabilityLedger (EntryID, AuditSector, SourcePKID, EntityLabel, AdjustmentValue)
    VALUES (@nextEntryID, @v_AuditSector, @v_SourcePKID, @v_EntityLabel, @v_AdjustmentValue);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AuditSector = 'LogisticsScalability'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', @v_SourcePKID, 'Table_SustainabilityScalabilityLedger', 'EntryID', CAST(@nextEntryID AS VARCHAR));
    END
    ELSE IF @v_AuditSector = 'SustainabilityLevy'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_SourcePKID, 'Table_SustainabilityScalabilityLedger', 'EntryID', CAST(@nextEntryID AS VARCHAR));
    END
    
    FETCH NEXT FROM SustainabilityScalabilityCursor INTO @v_AuditSector, @v_SourcePKID, @v_EntityLabel, @v_AdjustmentValue;
END;

CLOSE SustainabilityScalabilityCursor; 
DEALLOCATE SustainabilityScalabilityCursor;
GO