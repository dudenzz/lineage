-- Section: Create a Physical Table using Selection, Projection, Union, and Linear Transformations
-- Scenario: Compiling an Enterprise Asset Security & Digital Integrity Ledger.
-- Rule: UNION ALL between different tables (Employees and Customers), Selection (WHERE), and Linear Transformations (y = cx + d).
-- Purpose: Calculating "Security Clearance Weight" for internal staff and "Digital Trust Index" for enterprise-tier customers.

IF OBJECT_ID('Table_SecurityIntegrityLedger', 'U') IS NOT NULL DROP TABLE Table_SecurityIntegrityLedger;
CREATE TABLE Table_SecurityIntegrityLedger (
    SecurityID INT,
    AuditDomain VARCHAR(25),
    SourcePKID NVARCHAR(15),
    EntityLabel NVARCHAR(40),
    IntegrityMetric DECIMAL(18, 2) -- Linearly transformed column
);
GO

DECLARE @v_AuditDomain VARCHAR(25),
        @v_SourcePKID NVARCHAR(15),
        @v_EntityLabel NVARCHAR(40),
        @v_IntegrityMetric DECIMAL(18, 2),
        @nextSecurityID INT;

-- Linear Transformation Constants:
-- 1. Internal Security (Employees): Clearance weight based on ID (seniority proxy) plus a baseline security factor (y = 0.55 * EmployeeID + 50.00)
-- 2. External Trust (Customers): Trust index based on a flat strategic partnership constant (y = 0 * x + 90.00)
DECLARE @ClearanceScalar DECIMAL(10,2) = 0.55;
DECLARE @ClearanceBaseline DECIMAL(10,2) = 50.00;
DECLARE @TrustBaseline DECIMAL(10,2) = 90.00;

-- Cursor combining DIFFERENT tables (Employees and Customers) via UNION ALL
DECLARE SecurityIntegrityCursor CURSOR FOR 
    -- Branch 1: Employees (Internal Access Control)
    -- Selection: Only employees with a title containing 'Manager'
    -- Transformation: Linear clearance scaling (y = 0.55 * x + 50.00)
    SELECT 
        'InternalClearance' AS AuditDomain, 
        CAST(EmployeeID AS NVARCHAR(15)) AS SourcePKID, 
        LastName AS EntityLabel, 
        (CAST(EmployeeID AS DECIMAL(18,2)) * @ClearanceScalar) + @ClearanceBaseline AS IntegrityMetric 
    FROM Employees 
    WHERE Title LIKE '%Manager%' -- Selection
    
    UNION ALL

    -- Branch 2: Customers (External Partnership Trust)
    -- Selection: Only customers with an assigned Fax number (Proxy for established business infrastructure)
    -- Transformation: Constant linear projection for trust rating (y = 0 * x + 90.00)
    SELECT 
        'ExternalTrust' AS AuditDomain, 
        CAST(CustomerID AS NVARCHAR(15)) AS SourcePKID, 
        CompanyName AS EntityLabel, 
        @TrustBaseline AS IntegrityMetric 
    FROM Customers
    WHERE Fax IS NOT NULL; -- Selection

OPEN SecurityIntegrityCursor;
FETCH NEXT FROM SecurityIntegrityCursor INTO @v_AuditDomain, @v_SourcePKID, @v_EntityLabel, @v_IntegrityMetric;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextSecurityID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_SecurityIntegrityLedger (SecurityID, AuditDomain, SourcePKID, EntityLabel, IntegrityMetric)
    VALUES (@nextSecurityID, @v_AuditDomain, @v_SourcePKID, @v_EntityLabel, @v_IntegrityMetric);

    -- Log Row-Level Lineage based on the Union origin
    IF @v_AuditDomain = 'InternalClearance'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_SourcePKID, 'Table_SecurityIntegrityLedger', 'SecurityID', CAST(@nextSecurityID AS VARCHAR));
    END
    ELSE IF @v_AuditDomain = 'ExternalTrust'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_SourcePKID, 'Table_SecurityIntegrityLedger', 'SecurityID', CAST(@nextSecurityID AS VARCHAR));
    END
    
    FETCH NEXT FROM SecurityIntegrityCursor INTO @v_AuditDomain, @v_SourcePKID, @v_EntityLabel, @v_IntegrityMetric;
END;

CLOSE SecurityIntegrityCursor; 
DEALLOCATE SecurityIntegrityCursor;
GO