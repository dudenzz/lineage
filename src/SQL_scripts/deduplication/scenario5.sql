-- Section 1: Create a Source View for Assignments
-- Tests: Standard view-to-table lineage before the MERGE complexity.
CREATE OR ALTER VIEW vw_IncomingTerritoryData AS
SELECT 
    TerritoryID, 
    TerritoryDescription, 
    RegionID 
FROM Territories;
GO

-- Section 2: Staging Table for Dedup Logic
-- Tests: Intermediate persistence of data intended for a MERGE operation.
IF OBJECT_ID('Table_Territory_Staging', 'U') IS NOT NULL DROP TABLE Table_Territory_Staging;
CREATE TABLE Table_Territory_Staging (
    StagingID INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID NVARCHAR(20),
    T_Desc NVARCHAR(50),
    RID INT
);

INSERT INTO Table_Territory_Staging (TerritoryID, T_Desc, RID)
SELECT TerritoryID, TerritoryDescription, RegionID FROM vw_IncomingTerritoryData;

-- Log Lineage:
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'vw_IncomingTerritoryData', 'TerritoryID', TerritoryID, 'Table_Territory_Staging', 'TerritoryID', TerritoryID
FROM Table_Territory_Staging;
GO

-- Section 3: The MERGE Deduplication Procedure
-- Tests: "Used for Creation" tracking through MATCHED/NOT MATCHED branches.
-- This is a high-level test for tool logic: Can it see the source and target in a MERGE statement?
CREATE OR ALTER PROCEDURE proc_UpsertTerritoryMaster AS
BEGIN
    IF OBJECT_ID('Final_Territory_Master', 'U') IS NOT NULL DROP TABLE Final_Territory_Master;
    CREATE TABLE Final_Territory_Master (
        TID NVARCHAR(20) PRIMARY KEY, 
        Description NVARCHAR(50), 
        LastUpdated DATETIME
    );

    -- Perform the MERGE (Deduplication via Upsert)
    MERGE Final_Territory_Master AS Target
    USING Table_Territory_Staging AS Source
    ON (Target.TID = Source.TerritoryID)
    WHEN MATCHED THEN
        UPDATE SET Target.LastUpdated = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (TID, Description, LastUpdated)
        VALUES (Source.TerritoryID, Source.T_Desc, GETDATE());

    -- Log Lineage: Mapping the source staging table to the master table
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Territory_Staging', 'TerritoryID', TerritoryID, 'Final_Territory_Master', 'TID', TerritoryID
    FROM Table_Territory_Staging;
END;
GO

EXEC proc_UpsertTerritoryMaster;