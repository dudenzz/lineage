-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Channel-Impact" & Growth Acceleration Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Customer-Market and Employee-Sales domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to harmonize engagement and momentum scores.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedFulfillmentVelocityLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedFulfillmentVelocityLedger;
CREATE TABLE Table_UnifiedFulfillmentVelocityLedger (
    VelocityAuditID INT,
    SourceInstanceKey NVARCHAR(20), -- ShipperID or EmployeeID
    OperationalDomain NVARCHAR(20), -- Discriminator: 'CARRIER_LOGISTICS' or 'STAFF_FULFILLMENT'
    EntityLabel NVARCHAR(60),      -- CompanyName or LastName
    KineticThroughput FLOAT,       -- Non-Linear Transformation: (ID * 2.5) / LOG(ID + 2.0)
    SystemicFrictionScore FLOAT,    -- Non-Linear Transformation: SQRT(CAST(ID AS FLOAT) * 60.0)
    AccelerationPotential FLOAT    -- Non-Linear Transformation: POWER(CAST(ID AS FLOAT), 0.78)
);
GO

DECLARE @v_SourceInstanceKey NVARCHAR(20),
        @v_OperationalDomain NVARCHAR(20),
        @v_EntityLabel NVARCHAR(60),
        @v_KineticThroughput FLOAT,
        @v_SystemicFrictionScore FLOAT,
        @v_AccelerationPotential FLOAT,
        @nextVelocityAuditID INT;

-- Cursor using UNION ALL to harmonize external logistics carriers and internal fulfillment staff.
-- Selection: Shippers 1-3 (Primary Fleet) and Employees with Title 'Sales Manager' or 'Inside Sales Coordinator'.
-- Transformations (Non-Linear):
-- 1. Kinetic Throughput: Models the energy of the fulfillment node relative to its internal ID (A' = A * B / log(A)).
-- 2. Systemic Friction: Square root scaling to proxy the administrative "drag" associated with the node.
-- 3. Acceleration Potential: A power function (x^0.78) to estimate the growth ceiling of the resource.
DECLARE VelocityUnionCursor CURSOR FOR 
    -- Segment A: Carrier Logistics Domain
    SELECT 
        CAST(ShipperID AS NVARCHAR(20)) AS SourceInstanceKey,
        'CARRIER_LOGISTICS' AS OperationalDomain,
        CompanyName AS EntityLabel,
        (CAST(ShipperID AS FLOAT) * 2.5) / LOG(CAST(ShipperID AS FLOAT) + 2.0) AS KineticThroughput,
        SQRT(CAST(ShipperID AS FLOAT) * 60.0) AS SystemicFrictionScore,
        POWER(CAST(ShipperID AS FLOAT), 0.78) AS AccelerationPotential
    FROM Shippers
    
    UNION ALL

    -- Segment B: Staff Fulfillment Domain
    SELECT 
        CAST(EmployeeID AS NVARCHAR(20)) AS SourceInstanceKey,
        'STAFF_FULFILLMENT' AS OperationalDomain,
        LastName AS EntityLabel,
        (CAST(EmployeeID AS FLOAT) * 2.5) / LOG(CAST(EmployeeID AS FLOAT) + 2.0) AS KineticThroughput,
        SQRT(CAST(EmployeeID AS FLOAT) * 60.0) AS SystemicFrictionScore,
        POWER(CAST(EmployeeID AS FLOAT), 0.78) AS AccelerationPotential
    FROM Employees
    WHERE Title IN ('Sales Manager', 'Inside Sales Coordinator');

OPEN VelocityUnionCursor;
FETCH NEXT FROM VelocityUnionCursor INTO 
    @v_SourceInstanceKey, @v_OperationalDomain, @v_EntityLabel, @v_KineticThroughput, @v_SystemicFrictionScore, @v_AccelerationPotential;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextVelocityAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedFulfillmentVelocityLedger (
        VelocityAuditID, SourceInstanceKey, OperationalDomain, EntityLabel, 
        KineticThroughput, SystemicFrictionScore, AccelerationPotential
    )
    VALUES (
        @nextVelocityAuditID, @v_SourceInstanceKey, @v_OperationalDomain, @v_EntityLabel, 
        @v_KineticThroughput, @v_SystemicFrictionScore, @v_AccelerationPotential
    );

    -- Log Single-Source Lineage based on the OperationalDomain discriminator
    IF @v_OperationalDomain = 'CARRIER_LOGISTICS'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Shippers', 'ShipperID', @v_SourceInstanceKey, 'Table_UnifiedFulfillmentVelocityLedger', 'VelocityAuditID', CAST(@nextVelocityAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_SourceInstanceKey, 'Table_UnifiedFulfillmentVelocityLedger', 'VelocityAuditID', CAST(@nextVelocityAuditID AS VARCHAR));
    
    FETCH NEXT FROM VelocityUnionCursor INTO 
        @v_SourceInstanceKey, @v_OperationalDomain, @v_EntityLabel, @v_KineticThroughput, @v_SystemicFrictionScore, @v_AccelerationPotential;
END;

CLOSE VelocityUnionCursor; 
DEALLOCATE VelocityUnionCursor;
GO