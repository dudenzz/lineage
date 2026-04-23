-- Section: Create a Physical Table combining job titles using UNION with Strict Projection
-- Scenario: Compiling a Global Role and Persona Matrix to analyze the job titles of all business contacts (internal and external).
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no string manipulations.
IF OBJECT_ID('Table_GlobalRoleMatrix', 'U') IS NOT NULL DROP TABLE Table_GlobalRoleMatrix;
CREATE TABLE Table_GlobalRoleMatrix (
    MatrixID INT,
    Domain VARCHAR(20),
    OriginalEntityID NVARCHAR(15),
    JobTitle NVARCHAR(30)
);
GO

DECLARE @v_Domain VARCHAR(20),
        @v_OriginalEntityID NVARCHAR(15),
        @v_JobTitle NVARCHAR(30),
        @nextMatrixID INT;

-- Cursor using UNION ALL for strict projection across three distinct tables.
-- Selecting exact, existing columns only. CAST is strictly used for the structural 
-- data-type alignment required by the UNION operator, not for transforming the data.
DECLARE RoleMatrixCursor CURSOR FOR 
    SELECT 
        'CustomerContact' AS Domain, 
        CAST(CustomerID AS NVARCHAR(15)) AS OriginalEntityID, 
        ContactTitle AS JobTitle 
    FROM Customers
    UNION ALL
    SELECT 
        'SupplierContact' AS Domain, 
        CAST(SupplierID AS NVARCHAR(15)) AS OriginalEntityID, 
        ContactTitle AS JobTitle 
    FROM Suppliers
    UNION ALL
    SELECT 
        'InternalEmployee' AS Domain, 
        CAST(EmployeeID AS NVARCHAR(15)) AS OriginalEntityID, 
        Title AS JobTitle 
    FROM Employees;

OPEN RoleMatrixCursor;
FETCH NEXT FROM RoleMatrixCursor INTO @v_Domain, @v_OriginalEntityID, @v_JobTitle;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextMatrixID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected job title record
    INSERT INTO Table_GlobalRoleMatrix (MatrixID, Domain, OriginalEntityID, JobTitle)
    VALUES (@nextMatrixID, @v_Domain, @v_OriginalEntityID, @v_JobTitle);

    -- Conditionally log Row-Level Lineage based on which table the job title originated from
    IF @v_Domain = 'CustomerContact'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_OriginalEntityID, 'Table_GlobalRoleMatrix', 'MatrixID', CAST(@nextMatrixID AS VARCHAR));
    END
    ELSE IF @v_Domain = 'SupplierContact'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_OriginalEntityID, 'Table_GlobalRoleMatrix', 'MatrixID', CAST(@nextMatrixID AS VARCHAR));
    END
    ELSE IF @v_Domain = 'InternalEmployee'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', @v_OriginalEntityID, 'Table_GlobalRoleMatrix', 'MatrixID', CAST(@nextMatrixID AS VARCHAR));
    END
    
    FETCH NEXT FROM RoleMatrixCursor INTO @v_Domain, @v_OriginalEntityID, @v_JobTitle;
END;

CLOSE RoleMatrixCursor; 
DEALLOCATE RoleMatrixCursor;
GO