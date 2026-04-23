-- Section: Create a Physical Table using Selection, Projection, and Joins (Zero Transformations)
-- Scenario: Compiling a Regional Shipping Logistics & Employee Responsibility Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Zero data manipulation. All columns must be native projections only.
-- Lineage: Tracks specific source table and primary key for every entity in the Join.

IF OBJECT_ID('Table_RegionalLogisticsLedger', 'U') IS NOT NULL DROP TABLE Table_RegionalLogisticsLedger;
CREATE TABLE Table_RegionalLogisticsLedger (
    LogisticsAuditID INT,
    OrderID INT,            -- Native Projection from Orders
    EmployeeID INT,         -- Native Projection from Employees
    ShipName NVARCHAR(40),  -- Native Projection from Orders
    LastName NVARCHAR(20),  -- Native Projection from Employees
    Title NVARCHAR(30),      -- Native Projection from Employees
    ShipCity NVARCHAR(15)   -- Native Projection from Orders
);
GO

DECLARE @v_OrderID INT,
        @v_EmployeeID INT,
        @v_ShipName NVARCHAR(40),
        @v_LastName NVARCHAR(20),
        @v_Title NVARCHAR(30),
        @v_ShipCity NVARCHAR(15),
        @nextLogisticsAuditID INT;

-- Cursor using JOIN for strict projection across Order fulfillment and Employee ownership.
-- Selection: Only orders shipped to London where the handling employee is a Sales Representative.
-- All attributes are native; no linear transformations or calculations are applied.
DECLARE LogisticsCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        E.EmployeeID, 
        O.ShipName, 
        E.LastName, 
        E.Title, 
        O.ShipCity
    FROM Orders O
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID
    WHERE O.ShipCity = 'London' AND E.Title = 'Sales Representative'; -- Selection

OPEN LogisticsCursor;
FETCH NEXT FROM LogisticsCursor INTO 
    @v_OrderID, @v_EmployeeID, @v_ShipName, @v_LastName, @v_Title, @v_ShipCity;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextLogisticsAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Insert strictly projected native data
    INSERT INTO Table_RegionalLogisticsLedger (
        LogisticsAuditID, OrderID, EmployeeID, ShipName, LastName, Title, ShipCity
    )
    VALUES (
        @nextLogisticsAuditID, @v_OrderID, @v_EmployeeID, @v_ShipName, @v_LastName, @v_Title, @v_ShipCity
    );

    -- Log Dual-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_RegionalLogisticsLedger', 'LogisticsAuditID', CAST(@nextLogisticsAuditID AS VARCHAR));
    
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_RegionalLogisticsLedger', 'LogisticsAuditID', CAST(@nextLogisticsAuditID AS VARCHAR));
    
    FETCH NEXT FROM LogisticsCursor INTO 
        @v_OrderID, @v_EmployeeID, @v_ShipName, @v_LastName, @v_Title, @v_ShipCity;
END;

CLOSE LogisticsCursor; 
DEALLOCATE LogisticsCursor;
GO