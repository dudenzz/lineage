-- Section: Create a Physical Table combining native hierarchical relationships using UNION with Strict Projection
-- Scenario: Compiling a Universal Hierarchical Reference Ledger to map all parent-child entity ownership across the system.
-- Rule: Multiple inputs combined via UNION ALL, strictly projection. No Joins, zero data manipulation (native INT to INT alignment).
IF OBJECT_ID('Table_UniversalHierarchyLedger', 'U') IS NOT NULL DROP TABLE Table_UniversalHierarchyLedger;
CREATE TABLE Table_UniversalHierarchyLedger (
    HierarchyID INT,
    RelationshipContext VARCHAR(30),
    ChildEntityID INT,  -- Natively matches ProductID, EmployeeID, and OrderID
    ParentEntityID INT  -- Natively matches CategoryID, ReportsTo, and EmployeeID
);
GO

DECLARE @v_RelationshipContext VARCHAR(30),
        @v_ChildEntityID INT,
        @v_ParentEntityID INT,
        @nextHierarchyID INT;

-- Cursor using UNION ALL for strict projection across three distinct operational domains.
-- Because all Child and Parent IDs are natively INT, this query extracts the exact 
-- structural relationships with absolute zero data type casting or logical manipulation.
DECLARE HierarchyCursor CURSOR FOR 
    SELECT 
        'ProductToCategory' AS RelationshipContext,
        ProductID AS ChildEntityID,
        CategoryID AS ParentEntityID
    FROM Products
    UNION ALL
    SELECT 
        'EmployeeToManager' AS RelationshipContext,
        EmployeeID AS ChildEntityID,
        ReportsTo AS ParentEntityID  -- 'ReportsTo' is a self-referencing INT foreign key
    FROM Employees
    UNION ALL
    SELECT 
        'OrderToSalesRep' AS RelationshipContext,
        OrderID AS ChildEntityID,
        EmployeeID AS ParentEntityID -- Represents the employee who "owns" the order
    FROM Orders;

OPEN HierarchyCursor;
FETCH NEXT FROM HierarchyCursor INTO @v_RelationshipContext, @v_ChildEntityID, @v_ParentEntityID;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Fetch the next ID for our target table
    SELECT @nextHierarchyID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert the strictly projected hierarchical record
    -- Note: NULL parents (e.g., the CEO who reports to no one) are inserted exactly as they exist
    INSERT INTO Table_UniversalHierarchyLedger (HierarchyID, RelationshipContext, ChildEntityID, ParentEntityID)
    VALUES (@nextHierarchyID, @v_RelationshipContext, @v_ChildEntityID, @v_ParentEntityID);

    -- Conditionally log Row-Level Lineage based on the specific operational table the relationship was extracted from
    -- (CAST is used here exclusively to meet the standardized DataLineage tracking table requirements, 
    -- ensuring the actual hierarchical payload passing into the new table remains completely unmanipulated)
    IF @v_RelationshipContext = 'ProductToCategory'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Products', 'ProductID', CAST(@v_ChildEntityID AS VARCHAR), 'Table_UniversalHierarchyLedger', 'HierarchyID', CAST(@nextHierarchyID AS VARCHAR));
    END
    ELSE IF @v_RelationshipContext = 'EmployeeToManager'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Employees', 'EmployeeID', CAST(@v_ChildEntityID AS VARCHAR), 'Table_UniversalHierarchyLedger', 'HierarchyID', CAST(@nextHierarchyID AS VARCHAR));
    END
    ELSE IF @v_RelationshipContext = 'OrderToSalesRep'
    BEGIN
        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Orders', 'OrderID', CAST(@v_ChildEntityID AS VARCHAR), 'Table_UniversalHierarchyLedger', 'HierarchyID', CAST(@nextHierarchyID AS VARCHAR));
    END
    
    FETCH NEXT FROM HierarchyCursor INTO @v_RelationshipContext, @v_ChildEntityID, @v_ParentEntityID;
END;

CLOSE HierarchyCursor; 
DEALLOCATE HierarchyCursor;
GO