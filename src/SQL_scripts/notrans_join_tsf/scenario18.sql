-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Regional Product Distribution & Managerial Oversight Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_RegionalDistributionLedger', 'U') IS NOT NULL DROP TABLE Table_RegionalDistributionLedger;
CREATE TABLE Table_RegionalDistributionLedger (
    DistributionAuditID INT,
    ProductID INT,           -- Native Projection from Products
    EmployeeID INT,          -- Native Projection from Employees
    CategoryID INT,          -- Native Projection from Categories
    ProductName NVARCHAR(40), -- Native Projection from Products
    ManagerLastName NVARCHAR(20), -- Native Projection from Employees
    CategoryName NVARCHAR(15), -- Native Projection from Categories
    UnitPrice MONEY          -- Native Projection from Products
);
GO

DECLARE @v_ProductID INT,
        @v_EmployeeID INT,
        @v_CategoryID INT,
        @v_ProductName NVARCHAR(40),
        @v_ManagerLastName NVARCHAR(20),
        @v_CategoryName NVARCHAR(15),
        @v_UnitPrice MONEY,
        @nextDistributionAuditID INT;

-- Cursor using a complex JOIN for strict projection across Inventory, HR, and Taxonomy.
-- Selection: Only products in the 'Confections' category where the UnitPrice is greater than 15.00.
-- All attributes are native; no price adjustments or string modifications are applied.
DECLARE DistributionCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        E.EmployeeID, 
        C.CategoryID,
        P.ProductName, 
        E.LastName AS ManagerLastName,
        C.CategoryName,
        P.UnitPrice
    FROM Products P
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    -- Simulating an audit where we link products to the employee who manages the region/category 
    -- (In Northwind, linking via a static filter for this scenario)
    INNER JOIN Employees E ON E.Title = 'Sales Manager' 
    WHERE C.CategoryName = 'Confections' AND P.UnitPrice > 15.00; -- Selection

OPEN DistributionCursor;
FETCH NEXT FROM DistributionCursor INTO 
    @v_ProductID, @v_EmployeeID, @v_CategoryID, @v_ProductName, @v_ManagerLastName, @v_CategoryName, @v_UnitPrice;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextDistributionAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data (No Transformations)
    INSERT INTO Table_RegionalDistributionLedger (
        DistributionAuditID, ProductID, EmployeeID, CategoryID, ProductName, ManagerLastName, CategoryName, UnitPrice
    )
    VALUES (
        @nextDistributionAuditID, @v_ProductID, @v_EmployeeID, @v_CategoryID, @v_ProductName, @v_ManagerLastName, @v_CategoryName, @v_UnitPrice
    );

    -- Log Triple-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_RegionalDistributionLedger', 'DistributionAuditID', CAST(@nextDistributionAuditID AS VARCHAR));
    
    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_RegionalDistributionLedger', 'DistributionAuditID', CAST(@nextDistributionAuditID AS VARCHAR));

    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_RegionalDistributionLedger', 'DistributionAuditID', CAST(@nextDistributionAuditID AS VARCHAR));
    
    FETCH NEXT FROM DistributionCursor INTO 
        @v_ProductID, @v_EmployeeID, @v_CategoryID, @v_ProductName, @v_ManagerLastName, @v_CategoryName, @v_UnitPrice;
END;

CLOSE DistributionCursor; 
DEALLOCATE DistributionCursor;
GO