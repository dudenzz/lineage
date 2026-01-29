-- Section 0: Ensure Sequence and DataLineage table exist
IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'GlobalIDSequence')
    CREATE SEQUENCE GlobalIDSequence START WITH 1 INCREMENT BY 1;
GO

-- Section 1: Setup the Normalized Audit Table
IF OBJECT_ID('Table_Audit_Unpivoted', 'U') IS NOT NULL DROP TABLE Table_Audit_Unpivoted;

CREATE TABLE Table_Audit_Unpivoted (
    AuditID INT PRIMARY KEY,
    CategoryID INT,
    QuarterLabel NVARCHAR(20),
    RevenueValue DECIMAL(18,2)
);
GO

-- Section 2: Collapsing Columns into Rows (Self-Contained)
-- We use a CTE to generate source data, making this script independent of Scenario 1.
WITH SourceData AS (
    SELECT 1 as CategoryID, 100.00 as Q1_Sales, 150.00 as Q2_Sales, 120.00 as Q3_Sales, 180.00 as Q4_Sales
    UNION ALL
    SELECT 2, 200.00, 250.00, 220.00, 280.00
)
INSERT INTO Table_Audit_Unpivoted (AuditID, CategoryID, QuarterLabel, RevenueValue)
SELECT 
    NEXT VALUE FOR GlobalIDSequence,
    CategoryID, 
    QuarterLabel, 
    RevenueValue
FROM (
    SELECT CategoryID, Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales
    FROM SourceData
) p
UNPIVOT (
    RevenueValue FOR QuarterLabel IN (Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales)
) AS unpvt;

-- Section 3: Log Lineage
-- We log the lineage from the logical source 'SourceData' (simulating the wide table)
-- to the physical target 'Table_Audit_Unpivoted'.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 
    'Wide_Sales_Source', 
    'Q1-Q4_Columns', 
    CAST(CategoryID AS VARCHAR), 
    'Table_Audit_Unpivoted', 
    'AuditID', 
    CAST(AuditID AS VARCHAR)
FROM Table_Audit_Unpivoted;
GO

-- Verification
SELECT * FROM Table_Audit_Unpivoted;