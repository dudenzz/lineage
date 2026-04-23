-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling an Enterprise Customer-Support Employee Service Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_CustomerSupportLedger', 'U') IS NOT NULL DROP TABLE Table_CustomerSupportLedger;
CREATE TABLE Table_CustomerSupportLedger (
    SupportAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    CustomerID NCHAR(5),     -- Native Projection from Customers
    EmployeeID INT,          -- Native Projection from Employees
    ContactName NVARCHAR(30),-- Native Projection from Customers
    EmployeeLastName NVARCHAR(20), -- Native Projection from Employees
    OrderDate DATETIME       -- Native Projection from Orders
);
GO

DECLARE @v_OrderID INT,
        @v_CustomerID NCHAR(5),
        @v_EmployeeID INT,
        @v_ContactName NVARCHAR(30),
        @v_EmployeeLastName NVARCHAR(20),
        @v_OrderDate DATETIME,
        @nextSupportAuditID INT;

-- Cursor using JOIN for strict projection across Order history, Customer accounts, and HR representatives.
-- Selection: Only orders placed after 1997-01-01 for customers in 'Germany'.
-- All attributes are native; no date formatting or string concatenation is applied.
DECLARE SupportCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        C.CustomerID, 
        E.EmployeeID, 
        C.ContactName, 
        E.LastName,
        O.OrderDate
    FROM Orders O
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID
    WHERE C.Country = 'Germany' AND O.OrderDate > '1997-01-01'; -- Selection

OPEN SupportCursor;
FETCH NEXT FROM SupportCursor INTO 
    @v_OrderID, @v_CustomerID, @v_EmployeeID, @v_ContactName, @v_EmployeeLastName, @v_OrderDate;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextSupportAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data (No Transformations)
    INSERT INTO Table_CustomerSupportLedger (
        SupportAuditID, OrderID, CustomerID, EmployeeID, ContactName, EmployeeLastName, OrderDate
    )
    VALUES (
        @nextSupportAuditID, @v_OrderID, @v_CustomerID, @v_EmployeeID, @v_ContactName, @v_EmployeeLastName, @v_OrderDate
    );

    -- Log Triple-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_CustomerSupportLedger', 'SupportAuditID', CAST(@nextSupportAuditID AS VARCHAR));
    
    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_CustomerSupportLedger', 'SupportAuditID', CAST(@nextSupportAuditID AS VARCHAR));

    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_CustomerSupportLedger', 'SupportAuditID', CAST(@nextSupportAuditID AS VARCHAR));
    
    FETCH NEXT FROM SupportCursor INTO 
        @v_OrderID, @v_CustomerID, @v_EmployeeID, @v_ContactName, @v_EmployeeLastName, @v_OrderDate;
END;

CLOSE SupportCursor; 
DEALLOCATE SupportCursor;
GO