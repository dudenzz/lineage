-- Section 0: Setup Missing Dependencies
-- Ensuring the GlobalIDSequence exists for the script to run.
IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'GlobalIDSequence')
    CREATE SEQUENCE GlobalIDSequence START WITH 1 INCREMENT BY 1;
GO

-- Section 1: Create View with a Left Outer Join
-- Tests: Lineage through optional relationships.
CREATE OR ALTER VIEW vw_ProductSupplierMatch AS
SELECT 
    p.ProductID, 
    p.ProductName, 
    s.SupplierID,
    s.CompanyName AS SupplierName
FROM Products p
LEFT OUTER JOIN Suppliers s ON p.SupplierID = s.SupplierID;
GO

-- Section 2: Materialize into SupplyChainStaging
-- Tests: Lineage through CASE logic based on JOIN results.
IF OBJECT_ID('Table_SupplyChainStaging', 'U') IS NOT NULL DROP TABLE Table_SupplyChainStaging;
CREATE TABLE Table_SupplyChainStaging (
    SCID INT PRIMARY KEY, 
    ProdID INT, 
    SourcingStatus NVARCHAR(100)
);

INSERT INTO Table_SupplyChainStaging (SCID, ProdID, SourcingStatus)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    ProductID,
    CASE 
        WHEN SupplierID IS NULL THEN 'ORPHAN: ' + ProductName 
        ELSE 'Verified: ' + SupplierName 
    END
FROM vw_ProductSupplierMatch;
GO

-- Section 3: Procedures for Integrity Reporting
-- Tests: Multi-step procedural lineage involving global temp tables.

CREATE OR ALTER PROCEDURE proc_FinalizeIntegrityReport AS
BEGIN
    IF OBJECT_ID('Final_SupplyChainAudit', 'U') IS NOT NULL DROP TABLE Final_SupplyChainAudit;
    CREATE TABLE Final_SupplyChainAudit (AuditID INT PRIMARY KEY, ProductID INT, AlertStatus NVARCHAR(100));

    -- Move data from the global buffer to the final audit table
    INSERT INTO Final_SupplyChainAudit (AuditID, ProductID, AlertStatus)
    SELECT NEXT VALUE FOR GlobalIDSequence, ProdID, SourcingStatus
    FROM ##TempIntegrityBuffer;
END;
GO

CREATE OR ALTER PROCEDURE proc_ProcessOrphanedProducts AS
BEGIN
    -- Section 4: Create Global Temp Buffer
    -- Filter specifically for products that failed the join
    IF OBJECT_ID('tempdb..##TempIntegrityBuffer') IS NOT NULL DROP TABLE ##TempIntegrityBuffer;
    CREATE TABLE ##TempIntegrityBuffer (TempID INT, ProdID INT, SourcingStatus NVARCHAR(100));

    INSERT INTO ##TempIntegrityBuffer (TempID, ProdID, SourcingStatus)
    SELECT SCID, ProdID, SourcingStatus 
    FROM Table_SupplyChainStaging 
    WHERE SourcingStatus LIKE 'ORPHAN%';

    -- Call the finalization step
    EXEC proc_FinalizeIntegrityReport;
END;
GO

-- Execute the chain
EXEC proc_ProcessOrphanedProducts;