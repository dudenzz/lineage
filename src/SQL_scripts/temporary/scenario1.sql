-- Step 1: Initialize the first temporary table (#EmployeeTerritoryBase)
-- Tests: Ability to capture lineage to an object that does not exist in the System Catalog.
SELECT 
    e.EmployeeID,
    e.LastName + ', ' + e.FirstName AS EmployeeName,
    t.TerritoryDescription,
    r.RegionDescription
INTO #EmployeeTerritoryBase
FROM Employees e
JOIN EmployeeTerritories et ON e.EmployeeID = et.EmployeeID
JOIN Territories t ON et.TerritoryID = t.TerritoryID
JOIN Region r ON t.RegionID = r.RegionID;

-- Log Lineage (Simulated)
-- Tool must link 'Employees' and 'Territories' to '#EmployeeTerritoryBase'
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Employees', 'EmployeeID', CAST(EmployeeID AS VARCHAR), '#EmployeeTerritoryBase', 'EmployeeID', CAST(EmployeeID AS VARCHAR)
FROM #EmployeeTerritoryBase;
GO

-- Step 2: Refine data into a second temporary table (#RegionSummary)
-- Tests: Lineage between two ephemeral (temporary) objects.
SELECT 
    RegionDescription,
    COUNT(DISTINCT EmployeeID) AS StaffCount
INTO #RegionSummary
FROM #EmployeeTerritoryBase
GROUP BY RegionDescription;

-- Log Lineage (Simulated)
-- Tool must link '#EmployeeTerritoryBase' to '#RegionSummary'
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT '#EmployeeTerritoryBase', 'RegionDescription', RegionDescription, '#RegionSummary', 'RegionDescription', RegionDescription
FROM #RegionSummary;
GO

-- Step 3: Final Persistence to Physical Table
-- Tests: Bridging the gap from a "missing" temp object back to a permanent database object.
IF OBJECT_ID('Final_Regional_Staffing_Report', 'U') IS NOT NULL DROP TABLE Final_Regional_Staffing_Report;
CREATE TABLE Final_Regional_Staffing_Report (
    ReportID INT PRIMARY KEY,
    RegionName NVARCHAR(50),
    TotalStaff INT
);

DECLARE @region NVARCHAR(50), @count INT, @reportID INT;
DECLARE FinalCursor CURSOR FOR SELECT RegionDescription, StaffCount FROM #RegionSummary;

OPEN FinalCursor;
FETCH NEXT FROM FinalCursor INTO @region, @count;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @reportID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Final_Regional_Staffing_Report (ReportID, RegionName, TotalStaff)
    VALUES (@reportID, @region, @count);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('#RegionSummary', 'RegionDescription', @region, 'Final_Regional_Staffing_Report', 'ReportID', CAST(@reportID AS VARCHAR));

    FETCH NEXT FROM FinalCursor INTO @region, @count;
END;
CLOSE FinalCursor; DEALLOCATE FinalCursor;
GO

-- Step 4: Simulate Expiration
-- Tests: Does the tool fail once these objects are deleted from the session?
DROP TABLE #EmployeeTerritoryBase;
DROP TABLE #RegionSummary;
GO