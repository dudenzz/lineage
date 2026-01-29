-- Section 1: Setup Partitioned Sinks
-- Tests: Tracking lineage from a single source into functional partitions.
IF OBJECT_ID('Table_Staff_Core_Public', 'U') IS NOT NULL DROP TABLE Table_Staff_Core_Public;
IF OBJECT_ID('Table_Staff_Restricted_PII', 'U') IS NOT NULL DROP TABLE Table_Staff_Restricted_PII;

CREATE TABLE Table_Staff_Core_Public (
    EmployeeID INT PRIMARY KEY,
    FullName NVARCHAR(100),
    OfficePhone NVARCHAR(24)
);

CREATE TABLE Table_Staff_Restricted_PII (
    EmployeeID INT PRIMARY KEY,
    BirthDate DATETIME,
    HomePhone NVARCHAR(24),
    AddressLine NVARCHAR(100)
);
GO

-- Section 2: Functional Partitioning Script
-- Tests: Attribute-level lineage where specific columns go to specific tables.
INSERT INTO Table_Staff_Core_Public (EmployeeID, FullName, OfficePhone)
SELECT 
    EmployeeID, 
    FirstName + ' ' + LastName, 
    Extension 
FROM Employees;

INSERT INTO Table_Staff_Restricted_PII (EmployeeID, BirthDate, HomePhone, AddressLine)
SELECT 
    EmployeeID, 
    BirthDate, 
    HomePhone, 
    Address 
FROM Employees;

-- Log Lineage: Capturing the split
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Employees', 'EmployeeID', CAST(EmployeeID AS VARCHAR), 'Table_Staff_Core_Public', 'EmployeeID', CAST(EmployeeID AS VARCHAR)
FROM Table_Staff_Core_Public;

INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Employees', 'EmployeeID', CAST(EmployeeID AS VARCHAR), 'Table_Staff_Restricted_PII', 'EmployeeID', CAST(EmployeeID AS VARCHAR)
FROM Table_Staff_Restricted_PII;
GO

-- Section 3: Compliance Reporting (Converging Partitions)
-- Tests: Lineage from multiple partitions back into a single audited object.
CREATE OR ALTER PROCEDURE proc_GenerateComplianceSnapshot AS
BEGIN
    IF OBJECT_ID('Final_GDPR_Audit_Report', 'U') IS NOT NULL DROP TABLE Final_GDPR_Audit_Report;
    
    CREATE TABLE Final_GDPR_Audit_Report (
        AuditID INT PRIMARY KEY,
        EmpID INT,
        ExposureLevel NVARCHAR(20),
        RestrictedFieldsCount INT
    );

    -- This insert logic bridges the two functional partitions
    INSERT INTO Final_GDPR_Audit_Report (AuditID, EmpID, ExposureLevel, RestrictedFieldsCount)
    SELECT 
        NEXT VALUE FOR GlobalIDSequence,
        p.EmployeeID,
        CASE WHEN r.EmployeeID IS NOT NULL THEN 'High' ELSE 'Low' END,
        (CASE WHEN r.BirthDate IS NOT NULL THEN 1 ELSE 0 END + 
         CASE WHEN r.HomePhone IS NOT NULL THEN 1 ELSE 0 END)
    FROM Table_Staff_Core_Public p
    LEFT JOIN Table_Staff_Restricted_PII r ON p.EmployeeID = r.EmployeeID;

    -- Log Lineage: Multi-parent dependency
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Staff_Core_Public', 'EmployeeID', CAST(EmpID AS VARCHAR), 'Final_GDPR_Audit_Report', 'AuditID', 'GDPR'
    FROM Final_GDPR_Audit_Report;
END;
GO

EXEC proc_GenerateComplianceSnapshot;