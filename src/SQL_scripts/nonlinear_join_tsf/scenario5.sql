-- Section: Create a Physical Table using Selection, Projection, Joins, and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Employee-Productivity & Territory Yield Entropy Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_TerritoryYieldEntropyLedger', 'U') IS NOT NULL DROP TABLE Table_TerritoryYieldEntropyLedger;
CREATE TABLE Table_TerritoryYieldEntropyLedger (
    YieldAuditID INT,
    EmployeeID INT,           -- Native Projection from Employees
    TerritoryID NVARCHAR(20), -- Native Projection from Territories
    OrderID INT,              -- Native Projection from Orders
    LastName NVARCHAR(20),    -- Native Projection from Employees
    RevenueMomentum MONEY,    -- Non-Linear Transformation: (Freight * EmployeeID)
    TerritoryComplexity FLOAT, -- Non-Linear Transformation: SQRT(CAST(TerritoryID AS INT) * Freight) -- Note: Assumes numeric TerritoryID mapping
    ProductivityDecay FLOAT    -- Non-Linear Transformation: LOG(Freight + 5.0) / (EmployeeID + 1)
);
GO

DECLARE @v_EmployeeID INT,
        @v_TerritoryID NVARCHAR(20),
        @v_OrderID INT,
        @v_LastName NVARCHAR(20),
        @v_Freight MONEY,
        @v_RevenueMomentum MONEY,
        @v_TerritoryComplexity FLOAT,
        @v_ProductivityDecay FLOAT,
        @nextYieldAuditID INT;

-- Cursor using JOIN to integrate Staff Performance, Territory Assignments, and Regional Sales.
-- Selection: Only employees in 'Sales Representative' or 'Sales Manager' roles in 'Western' or 'Eastern' regions.
-- Transformations:
-- 1. Revenue Momentum: A non-linear scaling of logistical cost by personnel seniority (A' = A * B).
-- 2. Territory Complexity: Square root interaction between the territory identifier and shipment overhead.
-- 3. Productivity Decay: Logarithmic dampening of freight volume relative to personnel ID to model management span.
DECLARE YieldCursor CURSOR FOR 
    SELECT 
        E.EmployeeID, 
        T.TerritoryID, 
        O.OrderID,
        E.LastName, 
        O.Freight
    FROM Employees E
    INNER JOIN EmployeeTerritories ET ON E.EmployeeID = ET.EmployeeID
    INNER JOIN Territories T ON ET.TerritoryID = T.TerritoryID
    INNER JOIN Orders O ON E.EmployeeID = O.EmployeeID
    WHERE E.Title LIKE 'Sales%' 
      AND T.RegionID IN (2, 4); -- Selection (Region 2: Central, 4: Southern - adjusted for variety)

OPEN YieldCursor;
FETCH NEXT FROM YieldCursor INTO 
    @v_EmployeeID, @v_TerritoryID, @v_OrderID, @v_LastName, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextYieldAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_RevenueMomentum = @v_Freight * CAST(@v_EmployeeID AS FLOAT);
    
    -- Using TRY_CAST to handle non-numeric TerritoryID descriptions safely for the SQRT function
    SET @v_TerritoryComplexity = SQRT(ABS(ISNULL(TRY_CAST(@v_TerritoryID AS FLOAT), 1.0)) * CAST(@v_Freight AS FLOAT));
    
    SET @v_ProductivityDecay = LOG(CAST(@v_Freight AS FLOAT) + 5.0) / (CAST(@v_EmployeeID AS FLOAT) + 1.0);

    INSERT INTO Table_TerritoryYieldEntropyLedger (
        YieldAuditID, EmployeeID, TerritoryID, OrderID, LastName, 
        RevenueMomentum, TerritoryComplexity, ProductivityDecay
    )
    VALUES (
        @nextYieldAuditID, @v_EmployeeID, @v_TerritoryID, @v_OrderID, @v_LastName, 
        @v_RevenueMomentum, @v_TerritoryComplexity, @v_ProductivityDecay
    );

    -- Log Triple-Source Lineage
    -- Record source for Employees
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_TerritoryYieldEntropyLedger', 'YieldAuditID', CAST(@nextYieldAuditID AS VARCHAR));
    
    -- Record source for Territories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', CAST(@v_TerritoryID AS VARCHAR), 'Table_TerritoryYieldEntropyLedger', 'YieldAuditID', CAST(@nextYieldAuditID AS VARCHAR));

    -- Record source for Orders
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_TerritoryYieldEntropyLedger', 'YieldAuditID', CAST(@nextYieldAuditID AS VARCHAR));
    
    FETCH NEXT FROM YieldCursor INTO 
        @v_EmployeeID, @v_TerritoryID, @v_OrderID, @v_LastName, @v_Freight;
END;

CLOSE YieldCursor; 
DEALLOCATE YieldCursor;
GO