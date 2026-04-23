-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Logistics-Financial Friction" & Network Pressure Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Order-Shipping data and Product-Supplier data).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to normalize "Friction" metrics across domains.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedFrictionPressureLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedFrictionPressureLedger;
CREATE TABLE Table_UnifiedFrictionPressureLedger (
    PressureAuditID INT,
    SourceSystemKey NVARCHAR(20), -- OrderID or SupplierID
    StreamType NVARCHAR(20),      -- Discriminator: 'TRANSIT_FRICTION' or 'SUPPLY_PRESSURE'
    AttributeLabel NVARCHAR(60),  -- ShipCountry or CompanyName
    FinancialFrictionMass MONEY,  -- Non-Linear Transformation: (Freight * 1.25) OR (UnitPrice * 0.75)
    StructuralEntropy FLOAT,      -- Non-Linear Transformation: SQRT(Value * 2.0)
    SystemicResistance FLOAT      -- Non-Linear Transformation: LOG(Value + 5.0) / 1.8
);
GO

DECLARE @v_SourceSystemKey NVARCHAR(20),
        @v_StreamType NVARCHAR(20),
        @v_AttributeLabel NVARCHAR(60),
        @v_FinancialFrictionMass MONEY,
        @v_StructuralEntropy FLOAT,
        @v_SystemicResistance FLOAT,
        @nextPressureAuditID INT;

-- Cursor using UNION ALL to harmonize transit-based logistical friction and supply-side inventory pressure.
-- Selection: Orders shipped to 'Germany' or 'France' and Suppliers from 'Japan' or 'Australia'.
-- Transformations (Non-Linear):
-- 1. Financial Friction Mass: For orders (Freight * 1.25); For suppliers (UnitPrice proxy * 0.75).
-- 2. Structural Entropy: Square root of the mass scaled by 2.0 to model organizational disorder.
-- 3. Systemic Resistance: Logarithmic dampening of the value to identify bottleneck intensity.
DECLARE FrictionUnionCursor CURSOR FOR 
    -- Segment A: Order Transit Friction (Logistics Stream)
    SELECT 
        CAST(OrderID AS NVARCHAR(20)) AS SourceSystemKey,
        'TRANSIT_FRICTION' AS StreamType,
        ShipCountry AS AttributeLabel,
        CAST((Freight * 1.25) AS MONEY) AS FinancialFrictionMass,
        SQRT(CAST(Freight AS FLOAT) * 2.0) AS StructuralEntropy,
        LOG(CAST(Freight AS FLOAT) + 5.0) / 1.8 AS SystemicResistance
    FROM Orders
    WHERE ShipCountry IN ('Germany', 'France')
    
    UNION ALL

    -- Segment B: Supplier Input Pressure (Sourcing Stream)
    -- Joining Products to get a price proxy for supplier pressure
    SELECT 
        CAST(S.SupplierID AS NVARCHAR(20)) AS SourceSystemKey,
        'SUPPLY_PRESSURE' AS StreamType,
        S.CompanyName AS AttributeLabel,
        CAST((P.UnitPrice * 0.75) AS MONEY) AS FinancialFrictionMass,
        SQRT(CAST(P.UnitPrice AS FLOAT) * 2.0) AS StructuralEntropy,
        LOG(CAST(P.UnitPrice AS FLOAT) + 5.0) / 1.8 AS SystemicResistance
    FROM Suppliers S
    INNER JOIN Products P ON S.SupplierID = P.SupplierID
    WHERE S.Country IN ('Japan', 'Australia') AND P.Discontinued = 0;

OPEN FrictionUnionCursor;
FETCH NEXT FROM FrictionUnionCursor INTO 
    @v_SourceSystemKey, @v_StreamType, @v_AttributeLabel, @v_FinancialFrictionMass, @v_StructuralEntropy, @v_SystemicResistance;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextPressureAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedFrictionPressureLedger (
        PressureAuditID, SourceSystemKey, StreamType, AttributeLabel, 
        FinancialFrictionMass, StructuralEntropy, SystemicResistance
    )
    VALUES (
        @nextPressureAuditID, @v_SourceSystemKey, @v_StreamType, @v_AttributeLabel, 
        @v_FinancialFrictionMass, @v_StructuralEntropy, @v_SystemicResistance
    );

    -- Log Single-Source Lineage based on the StreamType discriminator
    IF @v_StreamType = 'TRANSIT_FRICTION'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', @v_SourceSystemKey, 'Table_UnifiedFrictionPressureLedger', 'PressureAuditID', CAST(@nextPressureAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_SourceSystemKey, 'Table_UnifiedFrictionPressureLedger', 'PressureAuditID', CAST(@nextPressureAuditID AS VARCHAR));
    
    FETCH NEXT FROM FrictionUnionCursor INTO 
        @v_SourceSystemKey, @v_StreamType, @v_AttributeLabel, @v_FinancialFrictionMass, @v_StructuralEntropy, @v_SystemicResistance;
END;

CLOSE FrictionUnionCursor; 
DEALLOCATE FrictionUnionCursor;
GO