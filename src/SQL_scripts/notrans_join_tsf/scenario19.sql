-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Customer-Territory Relationship & Account Management Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for all entities involved in the Join relationship.

IF OBJECT_ID('Table_CustomerTerritoryAuditLedger', 'U') IS NOT NULL DROP TABLE Table_CustomerTerritoryAuditLedger;
CREATE TABLE Table_CustomerTerritoryAuditLedger (
    AuditLineID INT,
    CustomerID NCHAR(5),      -- Native Projection from Customers
    OrderID INT,              -- Native Projection from Orders
    EmployeeID INT,           -- Native Projection from Employees
    CompanyName NVARCHAR(40), -- Native Projection from Customers
    ShipCountry NVARCHAR(15), -- Native Projection from Orders
    EmployeeTitle NVARCHAR(30) -- Native Projection from Employees
);
GO

DECLARE @v_CustomerID NCHAR(5),
        @v_OrderID INT,
        @v_EmployeeID INT,
        @v_CompanyName NVARCHAR(40),
        @v_ShipCountry NVARCHAR(15),
        @v_EmployeeTitle NVARCHAR(30),
        @nextAuditLineID INT;

-- Cursor using a complex JOIN for strict projection across Customer sales history and Internal HR ownership.
-- Selection: Only customers in 'Sweden' or 'Norway' (Nordic market audit) with orders handled by 'Sales Representatives'.
-- All attributes are native; no linear transformations or localized string replacements are applied.
DECLARE CustomerTerritoryCursor CURSOR FOR 
    SELECT 
        C.CustomerID, 
        O.OrderID, 
        E.EmployeeID, 
        C.CompanyName, 
        O.ShipCountry, 
        E.Title
    FROM Customers C
    INNER JOIN Orders O ON C.CustomerID = O.CustomerID
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID
    WHERE C.Country IN ('Sweden', 'Norway') 
      AND E.Title = 'Sales Representative'; -- Selection

OPEN CustomerTerritoryCursor;
FETCH NEXT FROM CustomerTerritoryCursor INTO 
    @v_CustomerID, @v_OrderID, @v_EmployeeID, @v_CompanyName, @v_ShipCountry, @v_EmployeeTitle;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditLineID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data (No Transformations)
    INSERT INTO Table_CustomerTerritoryAuditLedger (
        AuditLineID, CustomerID, OrderID, EmployeeID, CompanyName, ShipCountry, EmployeeTitle
    )
    VALUES (
        @nextAuditLineID, @v_CustomerID, @v_OrderID, @v_EmployeeID, @v_CompanyName, @v_ShipCountry, @v_EmployeeTitle
    );

    -- Log Triple-Source Lineage
    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_CustomerTerritoryAuditLedger', 'AuditLineID', CAST(@nextAuditLineID AS VARCHAR));
    
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_CustomerTerritoryAuditLedger', 'AuditLineID', CAST(@nextAuditLineID AS VARCHAR));

    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_CustomerTerritoryAuditLedger', 'AuditLineID', CAST(@nextAuditLineID AS VARCHAR));
    
    FETCH NEXT FROM CustomerTerritoryCursor INTO 
        @v_CustomerID, @v_OrderID, @v_EmployeeID, @v_CompanyName, @v_ShipCountry, @v_EmployeeTitle;
END;

CLOSE CustomerTerritoryCursor; 
DEALLOCATE CustomerTerritoryCursor;
GO