-- Section: Create a Physical Table combining multiple stakeholder sources using UNION with Strict Projection
-- Scenario: Compiling a unified External Stakeholder Communication List for corporate newsletter distribution.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection (selecting columns). No Joins, no string concatenations or data formatting.
IF OBJECT_ID('Table_StakeholderNewsletterList', 'U') IS NOT NULL DROP TABLE Table_StakeholderNewsletterList;
CREATE TABLE Table_StakeholderNewsletterList (
    StakeholderID INT,
    Category VARCHAR(20),
    OriginalID NVARCHAR(15),
    ContactName NVARCHAR(30),
    ContactTitle NVARCHAR(30)
);
GO

DECLARE @v_Category VARCHAR(20),
        @v_OriginalID NVARCHAR(15),
        @v_ContactName NVARCHAR(30),
        @v_ContactTitle NVARCHAR(30),
        @nextStakeholderID INT;

-- Cursor using UNION ALL for strict projection across two distinct entity tables.
-- Selecting exact, existing columns only. CAST is strictly used for the structural 
-- type alignment required by the UNION operator, not for transforming the data itself.
DECLARE StakeholderCursor CURSOR FOR 
    SELECT 
        'Customer' AS Category, 
        CAST(CustomerID AS NVARCHAR(15)) AS OriginalID, 
        ContactName, 
        ContactTitle 
    FROM Customers
    UNION ALL
    SELECT 
        'Supplier' AS Category, 
        CAST(SupplierID AS NVARCHAR(15)) AS OriginalID, 
        ContactName, 
        ContactTitle 
    FROM Suppliers;

OPEN StakeholderCursor;
FETCH NEXT FROM StakeholderCursor INTO @v_Category, @v_OriginalID, @v_ContactName, @v_ContactTitle;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextStakeholderID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected contact record
    INSERT INTO Table_StakeholderNewsletterList (StakeholderID, Category, OriginalID, ContactName, ContactTitle)
    VALUES (@nextStakeholderID, @v_Category, @v_OriginalID, @v_ContactName, @v_ContactTitle);

    -- Conditionally log Row-Level Lineage based on which table the contact originated from
    IF @v_Category = 'Customer'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Customers', 'CustomerID', @v_OriginalID, 'Table_StakeholderNewsletterList', 'StakeholderID', CAST(@nextStakeholderID AS VARCHAR));
    END
    ELSE IF @v_Category = 'Supplier'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Suppliers', 'SupplierID', @v_OriginalID, 'Table_StakeholderNewsletterList', 'StakeholderID', CAST(@nextStakeholderID AS VARCHAR));
    END
    
    FETCH NEXT FROM StakeholderCursor INTO @v_Category, @v_OriginalID, @v_ContactName, @v_ContactTitle;
END;

CLOSE StakeholderCursor; 
DEALLOCATE StakeholderCursor;
GO