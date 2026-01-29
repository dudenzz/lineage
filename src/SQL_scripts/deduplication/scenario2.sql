-- Section 1: Create a Consolidation View
-- Tests: Lineage through GROUP BY where multiple source rows contribute to a single target row.
-- The tool must track that the final 'City' comes from MAX(City) across all duplicates.
CREATE OR ALTER VIEW vw_ConsolidatedSuppliers AS
SELECT 
    ContactName,
    MAX(CompanyName) AS PrimaryCompanyName,
    MAX(Address) AS LatestAddress,
    MAX(City) AS PrimaryCity,
    COUNT(SupplierID) AS DuplicateSourceCount
FROM Suppliers
GROUP BY ContactName;
GO

-- Log Lineage:
-- Tool must recognize 'Suppliers' as the source for the grouped view.
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Suppliers', 'ContactName', ContactName, 'vw_ConsolidatedSuppliers', 'ContactName', ContactName
FROM vw_ConsolidatedSuppliers;
GO

-- Section 2: Materializing the Master Supplier Table
-- Tests: Tracking lineage through a "Used for Creation" predicate involving an aggregate count.
IF OBJECT_ID('Table_Master_Suppliers', 'U') IS NOT NULL DROP TABLE Table_Master_Suppliers;
CREATE TABLE Table_Master_Suppliers (
    MasterID INT PRIMARY KEY,
    ContactName NVARCHAR(30),
    MasterCompany NVARCHAR(40),
    IsConsolidated BIT
);

DECLARE @m_contact NVARCHAR(30), @m_company NVARCHAR(40), @m_dupCount INT, @nextMasterID INT;
DECLARE MasterCursor CURSOR FOR 
    SELECT ContactName, PrimaryCompanyName, DuplicateSourceCount 
    FROM vw_ConsolidatedSuppliers;

OPEN MasterCursor;
FETCH NEXT FROM MasterCursor INTO @m_contact, @m_company, @m_dupCount;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextMasterID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Master_Suppliers (MasterID, ContactName, MasterCompany, IsConsolidated)
    VALUES (@nextMasterID, @m_contact, @m_company, CASE WHEN @m_dupCount > 1 THEN 1 ELSE 0 END);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_ConsolidatedSuppliers', 'ContactName', @m_contact, 'Table_Master_Suppliers', 'MasterID', CAST(@nextMasterID AS VARCHAR));

    FETCH NEXT FROM MasterCursor INTO @m_contact, @m_company, @m_dupCount;
END;
CLOSE MasterCursor; DEALLOCATE MasterCursor;
GO

-- Section 3: Final Stored Procedure for Supplier Audit
-- Tests: Lineage of a flag (IsConsolidated) derived from the deduplication logic.
CREATE OR ALTER PROCEDURE proc_ArchiveMasterSuppliers AS
BEGIN
    IF OBJECT_ID('Final_Supplier_Master_Archive', 'U') IS NOT NULL DROP TABLE Final_Supplier_Master_Archive;
    CREATE TABLE Final_Supplier_Master_Archive (ArchiveID INT, SupplierName NVARCHAR(40), QualityStatus NVARCHAR(20));

    INSERT INTO Final_Supplier_Master_Archive (ArchiveID, SupplierName, QualityStatus)
    SELECT 
        NEXT VALUE FOR GlobalIDSequence,
        MasterCompany,
        CASE WHEN IsConsolidated = 1 THEN 'Deduplicated' ELSE 'Original' END
    FROM Table_Master_Suppliers;

    -- Log Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    SELECT 'Table_Master_Suppliers', 'MasterID', CAST(MasterID AS VARCHAR), 'Final_Supplier_Master_Archive', 'ArchiveID', 'Archived'
    FROM Table_Master_Suppliers;
END;
GO

EXEC proc_ArchiveMasterSuppliers;