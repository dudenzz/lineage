-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Customer-Support Interaction & Response Priority Ledger.
-- Rule: Combine inputs via INNER JOIN. Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_CustomerPriorityAuditLedger', 'U') IS NOT NULL DROP TABLE Table_CustomerPriorityAuditLedger;
CREATE TABLE Table_CustomerPriorityAuditLedger (
    AuditID INT,
    OrderID INT,             -- Native Projection from Orders
    CustomerID NCHAR(5),     -- Native Projection from Customers
    EmployeeID INT,          -- Native Projection from Employees
    WeightedOrderValue MONEY, -- Linearly transformed column (Orders.Freight)
    PriorityServiceScore DECIMAL(18,2), -- Linearly transformed column (Orders.Freight)
    AccountSeniorityIndex DECIMAL(18,2) -- Linearly transformed column (Employees.EmployeeID)
);
GO

DECLARE @v_OrderID INT,
        @v_CustomerID NCHAR(5),
        @v_EmployeeID INT,
        @v_WeightedOrderValue MONEY,
        @v_PriorityServiceScore DECIMAL(18,2),
        @v_AccountSeniorityIndex DECIMAL(18,2),
        @nextAuditID INT;

-- Linear Transformation Constants:
-- 1. Weighted Order Value: Adjusts freight costs to reflect total logistics overhead (y = 1.20 * Freight + 35.00)
-- 2. Priority Service Score: A metric for urgency based on shipping expense (y = 0.75 * Freight + 10.00)
-- 3. Account Seniority Index: Normalizes employee ID as a proxy for experience in handling (y = 2.50 * EmployeeID + 100.00)
DECLARE @ValueScalar DECIMAL(10,2) = 1.20;
DECLARE @ValueBase MONEY = 35.00;
DECLARE @PriorityScalar DECIMAL(10,2) = 0.75;
DECLARE @PriorityBase DECIMAL(10,2) = 10.00;
DECLARE @SeniorityScalar DECIMAL(10,2) = 2.50;
DECLARE @SeniorityBase DECIMAL(10,2) = 100.00;

-- Cursor using JOIN to integrate Sales Records with Account Management Personnel.
-- Selection: Only orders for customers in 'Brazil', 'Mexico', or 'Argentina' handled by 'Sales Representatives'.
DECLARE CustomerPriorityCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        C.CustomerID, 
        E.EmployeeID, 
        (O.Freight * @ValueScalar) + @ValueBase AS WeightedOrderValue,
        (CAST(O.Freight AS DECIMAL(18,2)) * @PriorityScalar) + @PriorityBase AS PriorityServiceScore,
        (CAST(E.EmployeeID AS DECIMAL(18,2)) * @SeniorityScalar) + @SeniorityBase AS AccountSeniorityIndex
    FROM Orders O
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID
    WHERE C.Country IN ('Brazil', 'Mexico', 'Argentina') 
      AND E.Title = 'Sales Representative'; -- Selection

OPEN CustomerPriorityCursor;
FETCH NEXT FROM CustomerPriorityCursor INTO 
    @v_OrderID, @v_CustomerID, @v_EmployeeID, @v_WeightedOrderValue, @v_PriorityServiceScore, @v_AccountSeniorityIndex;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_CustomerPriorityAuditLedger (
        AuditID, OrderID, CustomerID, EmployeeID, WeightedOrderValue, PriorityServiceScore, AccountSeniorityIndex
    )
    VALUES (
        @nextAuditID, @v_OrderID, @v_CustomerID, @v_EmployeeID, @v_WeightedOrderValue, @v_PriorityServiceScore, @v_AccountSeniorityIndex
    );

    -- Log Triple-Source Lineage
    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_CustomerPriorityAuditLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    -- Record source for Customers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_CustomerPriorityAuditLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));

    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_CustomerPriorityAuditLedger', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM CustomerPriorityCursor INTO 
        @v_OrderID, @v_CustomerID, @v_EmployeeID, @v_WeightedOrderValue, @v_PriorityServiceScore, @v_AccountSeniorityIndex;
END;

CLOSE CustomerPriorityCursor; 
DEALLOCATE CustomerPriorityCursor;
GO