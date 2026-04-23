-- Section: Create a Physical Table using Selection, Projection, UNION Operators, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Unified "Entity-Health" & Resource Utilization Ledger.
-- Rule: Combine inputs via UNION ALL (Merging Employee-Personnel and Supplier-Vendor domains).
-- Rule: Apply Non-Linear Transformations (y = f(x)) to normalize "Health" and "Influence" metrics.
-- Lineage: Tracks specific source table and primary key for the contributing entity in the Union set.

IF OBJECT_ID('Table_UnifiedEntityHealthLedger', 'U') IS NOT NULL DROP TABLE Table_UnifiedEntityHealthLedger;
CREATE TABLE Table_UnifiedEntityHealthLedger (
    HealthAuditID INT,
    ReferenceID NVARCHAR(20),  -- EmployeeID or SupplierID
    DomainType NVARCHAR(20),   -- Discriminator: 'INTERNAL_STAFF' or 'EXTERNAL_VENDOR'
    DisplayName NVARCHAR(60),  -- LastName or CompanyName
    UtilizationMass FLOAT,     -- Non-Linear Transformation: (ID * 1.5) / LOG(ID + 2.0)
    ComplexityCurvature FLOAT, -- Non-Linear Transformation: SQRT(CAST(ID AS FLOAT) * 100.0)
    SystemicInfluenceScore FLOAT -- Non-Linear Transformation: POWER(CAST(ID AS FLOAT), 0.85)
);
GO

DECLARE @v_ReferenceID NVARCHAR(20),
        @v_DomainType NVARCHAR(20),
        @v_DisplayName NVARCHAR(60),
        @v_UtilizationMass FLOAT,
        @v_ComplexityCurvature FLOAT,
        @v_SystemicInfluenceScore FLOAT,
        @nextHealthAuditID INT;

-- Cursor using UNION ALL to harmonize internal personnel and external partners into a single strategic view.
-- Selection: Employees with ID < 10 (Senior/Legacy staff) and Suppliers from 'Europe' or 'Americas'.
-- Transformations (Non-Linear):
-- 1. Utilization Mass: Models resource "weight" using a non-linear ID-based scaling to proxy tenure/legacy impact.
-- 2. Complexity Curvature: Square root scaling of the identifier to map organizational complexity tiers.
-- 3. Systemic Influence: Power function to calculate the "reach" of the entity within the ecosystem.
DECLARE HealthUnionCursor CURSOR FOR 
    -- Segment A: Internal Personnel Domain
    SELECT 
        CAST(EmployeeID AS NVARCHAR(20)) AS ReferenceID,
        'INTERNAL_STAFF' AS DomainType,
        LastName AS DisplayName,
        (CAST(EmployeeID AS FLOAT) * 1.5) / LOG(CAST(EmployeeID AS FLOAT) + 2.0) AS UtilizationMass,
        SQRT(CAST(EmployeeID AS FLOAT) * 100.0) AS ComplexityCurvature,
        POWER(CAST(EmployeeID AS FLOAT), 0.85) AS SystemicInfluenceScore
    FROM Employees
    WHERE EmployeeID < 10
    
    UNION ALL

    -- Segment B: External Vendor Domain
    SELECT 
        CAST(SupplierID AS NVARCHAR(20)) AS ReferenceID,
        'EXTERNAL_VENDOR' AS DomainType,
        CompanyName AS DisplayName,
        (CAST(SupplierID AS FLOAT) * 1.5) / LOG(CAST(SupplierID AS FLOAT) + 2.0) AS UtilizationMass,
        SQRT(CAST(SupplierID AS FLOAT) * 100.0) AS ComplexityCurvature,
        POWER(CAST(SupplierID AS FLOAT), 0.85) AS SystemicInfluenceScore
    FROM Suppliers
    WHERE Country IN ('UK', 'USA', 'Germany', 'Brazil', 'Italy');

OPEN HealthUnionCursor;
FETCH NEXT FROM HealthUnionCursor INTO 
    @v_ReferenceID, @v_DomainType, @v_DisplayName, @v_UtilizationMass, @v_ComplexityCurvature, @v_SystemicInfluenceScore;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextHealthAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_UnifiedEntityHealthLedger (
        HealthAuditID, ReferenceID, DomainType, DisplayName, 
        UtilizationMass, ComplexityCurvature, SystemicInfluenceScore
    )
    VALUES (
        @nextHealthAuditID, @v_ReferenceID, @v_DomainType, @v_DisplayName, 
        @v_UtilizationMass, @v_ComplexityCurvature, @v_SystemicInfluenceScore
    );

    -- Log Single-Source Lineage based on the DomainType discriminator
    IF @v_DomainType = 'INTERNAL_STAFF'
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_ReferenceID, 'Table_UnifiedEntityHealthLedger', 'HealthAuditID', CAST(@nextHealthAuditID AS VARCHAR));
    ELSE
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_ReferenceID, 'Table_UnifiedEntityHealthLedger', 'HealthAuditID', CAST(@nextHealthAuditID AS VARCHAR));
    
    FETCH NEXT FROM HealthUnionCursor INTO 
        @v_ReferenceID, @v_DomainType, @v_DisplayName, @v_UtilizationMass, @v_ComplexityCurvature, @v_SystemicInfluenceScore;
END;

CLOSE HealthUnionCursor; 
DEALLOCATE HealthUnionCursor;
GO