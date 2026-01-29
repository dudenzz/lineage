-- Section 1: Create a View that flattens the 3-table hierarchy
-- Tests: Lineage across a linear chain of foreign key relationships.
CREATE OR ALTER VIEW vw_RegionalTerritoryMapping AS
SELECT 
    r.RegionID,
    r.RegionDescription,
    t.TerritoryID,
    t.TerritoryDescription,
    e.EmployeeID,
    e.FirstName + ' ' + e.LastName AS StaffName
FROM Region r
JOIN Territories t ON r.RegionID = t.RegionID
JOIN EmployeeTerritories et ON t.TerritoryID = et.TerritoryID
JOIN Employees e ON et.EmployeeID = e.EmployeeID;
GO

-- Log Lineage:
-- Tool must recognize all four tables as contributing to the view.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Region', 'RegionID', CAST(RegionID AS VARCHAR), 'vw_RegionalTerritoryMapping', 'Composite', CAST(RegionID AS VARCHAR) + '-' + CAST(TerritoryID AS VARCHAR)
FROM vw_RegionalTerritoryMapping;
GO

-- Section 2: Materialize into a Territory Assignments table
-- Tests: Column-level lineage from multiple source tables into specific target columns.
IF OBJECT_ID('Table_Staff_Territory_Audit', 'U') IS NOT NULL DROP TABLE Table_Staff_Territory_Audit;
CREATE TABLE Table_Staff_Territory_Audit (
    AuditID INT PRIMARY KEY,
    StaffName NVARCHAR(100),
    TerritoryPath NVARCHAR(MAX),
    RegionCode NVARCHAR(50)
);

DECLARE @s_name NVARCHAR(100), @t_desc NVARCHAR(100), @r_desc NVARCHAR(100), @nextAuditID INT;
DECLARE AuditCursor CURSOR FOR 
    SELECT StaffName, TerritoryDescription, RegionDescription 
    FROM vw_RegionalTerritoryMapping;

OPEN AuditCursor;
FETCH NEXT FROM AuditCursor INTO @s_name, @t_desc, @r_desc;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Staff_Territory_Audit (AuditID, StaffName, TerritoryPath, RegionCode)
    VALUES (@nextAuditID, @s_name, @r_desc + ' > ' + @t_desc, LEFT(@r_desc, 3));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_RegionalTerritoryMapping', 'Composite', 'Internal', 'Table_Staff_Territory_Audit', 'AuditID', CAST(@nextAuditID AS VARCHAR));

    FETCH NEXT FROM AuditCursor INTO @s_name, @t_desc, @r_desc;
END;
CLOSE AuditCursor; DEALLOCATE AuditCursor;
GO

-- Section 3: Procedural Summary Table
-- Tests: Final transformation based on hierarchical attributes.
CREATE OR ALTER PROCEDURE proc_SummarizeRegionalCoverage AS
BEGIN
    IF OBJECT_ID('Final_Regional_Coverage_Summary', 'U') IS NOT NULL DROP TABLE Final_Regional_Coverage_Summary;
    CREATE TABLE Final_Regional_Coverage_Summary (SummaryID INT, RegionCode NVARCHAR(50), TotalStaffCount INT);

    INSERT INTO Final_Regional_Coverage_Summary (SummaryID, RegionCode, TotalStaffCount)
    SELECT 
        NEXT VALUE FOR GlobalIDSequence,
        RegionCode,
        COUNT(DISTINCT StaffName)
    FROM Table_Staff_Territory_Audit
    GROUP BY RegionCode;

    -- Log Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Staff_Territory_Audit', 'AuditID', 'Grouped', 'Final_Regional_Coverage_Summary', 'SummaryID', 'Grouped'
    FROM Final_Regional_Coverage_Summary;
END;
GO

EXEC proc_SummarizeRegionalCoverage;