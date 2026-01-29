-- Section 1: Materializing Matching Entities
IF OBJECT_ID('Table_Common_Customer_Base', 'U') IS NOT NULL DROP TABLE Table_Common_Customer_Base;

CREATE TABLE Table_Common_Customer_Base (
    CommonName NVARCHAR(40) PRIMARY KEY,
    MatchedDate DATETIME DEFAULT GETDATE()
);

-- INTERSECT finds records that exist in both sets
INSERT INTO Table_Common_Customer_Base (CommonName)
SELECT CompanyName FROM Customers
INTERSECT
SELECT CompanyName FROM Suppliers;

-- Log Lineage:
-- Tool must show that BOTH Customers and Suppliers are "Used for Creation" of this list.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
VALUES ('Customers', 'CompanyName', 'Set-Intersect', 'Table_Common_Customer_Base', 'CommonName', 'Overlap');
GO