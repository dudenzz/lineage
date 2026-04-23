-- Section: Create a Physical Table using Selection, Projection, Joins (2 links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Employee-Productivity & Territory Yield Entropy Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Employees -> Orders, Employees -> EmployeeTerritories -> Territories).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_StaffTerritoryYieldLedger', 'U') IS NOT NULL DROP TABLE Table_StaffTerritoryYieldLedger;
CREATE TABLE Table_StaffTerritoryYieldLedger (
    YieldAuditID INT,
    EmployeeID INT,           -- Native Projection from Employees
    OrderID INT,              -- Native Projection from Orders
    TerritoryID NVARCHAR(20), -- Native Projection from Territories
    EmployeeLastName NVARCHAR(20), -- Native Projection from Employees
    RevenueForce MONEY,       -- Non-Linear Transformation: (Freight * EmployeeID)
    TerritoryComplexityIndex FLOAT, -- Non-Linear Transformation: SQRT(Freight * CAST(LEN(TerritoryID) AS INT))
    ProductivityGradient FLOAT      -- Non-Linear Transformation: POWER(EmployeeID, 1.1) / LOG(Freight + 5.0)
);
GO

DECLARE @v_EmployeeID INT,
        @v_OrderID INT,
        @v_TerritoryID NVARCHAR(20),
        @v_EmployeeLastName NVARCHAR(20),
        @v_Freight MONEY,
        @v_RevenueForce MONEY,
        @v_TerritoryComplexityIndex FLOAT,
        @v_ProductivityGradient FLOAT,
        @nextYieldAuditID INT;

-- Cursor using two table links branched from the central Employees entity.
-- Selection: Only employees in 'Sales' roles managing 'Northern' or 'Western' territories with Freight > 15.00.
-- Transformations:
-- 1. Revenue Force: Models the magnitude of impact as a product of shipping volume and staff ID (A' = A * B).
-- 2. Territory Complexity: Square root of freight cost scaled by the identifier length of the assigned territory.
-- 3. Productivity Gradient: Exponential staff growth relative to the logarithmic dampening of logistical overhead.
DECLARE YieldLinkCursor CURSOR FOR 
    SELECT 
        E.EmployeeID, 
        O.OrderID, 
        T.TerritoryID,
        E.LastName,
        O.Freight
    FROM Employees E
    INNER JOIN Orders O ON E.EmployeeID = O.EmployeeID -- Link 1: Sales Transactions
    INNER JOIN EmployeeTerritories ET ON E.EmployeeID = ET.EmployeeID
    INNER JOIN Territories T ON ET.TerritoryID = T.TerritoryID -- Link 2: Geographic Reach
    WHERE E.Title LIKE '%Sales%' 
      AND O.Freight > 15.00
      AND T.RegionID IN (1, 3); -- Region 1: Northern, 3: Western

OPEN YieldLinkCursor;
FETCH NEXT FROM YieldLinkCursor INTO 
    @v_EmployeeID, @v_OrderID, @v_TerritoryID, @v_EmployeeLastName, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextYieldAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_RevenueForce = @v_Freight * CAST(@v_EmployeeID AS FLOAT);
    SET @v_TerritoryComplexityIndex = SQRT(CAST(@v_Freight AS FLOAT) * CAST(LEN(@v_TerritoryID) AS FLOAT));
    SET @v_ProductivityGradient = POWER(CAST(@v_EmployeeID AS FLOAT), 1.1) / LOG(CAST(@v_Freight AS FLOAT) + 5.0);

    INSERT INTO Table_StaffTerritoryYieldLedger (
        YieldAuditID, EmployeeID, OrderID, TerritoryID, EmployeeLastName, 
        RevenueForce, TerritoryComplexityIndex, ProductivityGradient
    )
    VALUES (
        @nextYieldAuditID, @v_EmployeeID, @v_OrderID, @v_TerritoryID, @v_EmployeeLastName, 
        @v_RevenueForce, @v_TerritoryComplexityIndex, @v_ProductivityGradient
    );

    -- Log Triple-Source Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_StaffTerritoryYieldLedger', 'YieldAuditID', CAST(@nextYieldAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_StaffTerritoryYieldLedger', 'YieldAuditID', CAST(@nextYieldAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', CAST(@v_TerritoryID AS VARCHAR), 'Table_StaffTerritoryYieldLedger', 'YieldAuditID', CAST(@nextYieldAuditID AS VARCHAR));
    
    FETCH NEXT FROM YieldLinkCursor INTO 
        @v_EmployeeID, @v_OrderID, @v_TerritoryID, @v_EmployeeLastName, @v_Freight;
END;

CLOSE YieldLinkCursor; 
DEALLOCATE YieldLinkCursor;
GO