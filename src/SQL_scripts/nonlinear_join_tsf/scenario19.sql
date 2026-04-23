-- Section: Create a Physical Table using Selection, Projection, Joins (3 table links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Employee-Product-Territory Sales Efficiency & Geographic Momentum Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link: Orders -> Employees, Orders -> [Order Details] -> Products, Orders -> Employees -> EmployeeTerritories -> Territories).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Logarithmic scaling.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_GeographicSalesMomentumLedger', 'U') IS NOT NULL DROP TABLE Table_GeographicSalesMomentumLedger;
CREATE TABLE Table_GeographicSalesMomentumLedger (
    MomentumAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    EmployeeID INT,            -- Native Projection from Employees
    ProductID INT,             -- Native Projection from Products
    TerritoryID NVARCHAR(20),  -- Native Projection from Territories
    EmployeeLastName NVARCHAR(20), -- Native Projection from Employees
    TransactionFinancialMass MONEY, -- Non-Linear Transformation: (UnitPrice * Quantity)
    RegionalExpansionVelocity FLOAT, -- Non-Linear Transformation: SQRT(Freight * ABS(CHECKSUM(TerritoryID) % 100))
    PersonnelYieldExponent FLOAT    -- Non-Linear Transformation: POWER(EmployeeID, 1.15) / LOG(Freight + 4.0)
);
GO

DECLARE @v_OrderID INT,
        @v_EmployeeID INT,
        @v_ProductID INT,
        @v_TerritoryID NVARCHAR(20),
        @v_EmployeeLastName NVARCHAR(20),
        @v_UnitPrice MONEY,
        @v_Quantity SMALLINT,
        @v_Freight MONEY,
        @v_TransactionFinancialMass MONEY,
        @v_RegionalExpansionVelocity FLOAT,
        @v_PersonnelYieldExponent FLOAT,
        @nextMomentumAuditID INT;

-- Cursor using three distinct table links branched from the central Orders/Personnel intersection.
-- Selection: Only transactions involving 'Beverages' products handled by 'Sales Representatives' in Region 3 (Western).
-- Transformations:
-- 1. Transaction Financial Mass: Models total order value as a product of unit price and volume (A' = A * B).
-- 2. Regional Expansion Velocity: Square root interaction between logistics cost and territory metadata to model market spread.
-- 3. Personnel Yield Exponent: Exponential staff growth relative to logarithmic freight dampening to assess per-employee efficiency.
DECLARE MomentumLinkCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        E.EmployeeID, 
        P.ProductID,
        T.TerritoryID,
        E.LastName,
        OD.UnitPrice,
        OD.Quantity,
        O.Freight
    FROM Orders O
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID -- Link 1: Internal Staff
    INNER JOIN [Order Details] OD ON O.OrderID = OD.OrderID
    INNER JOIN Products P ON OD.ProductID = P.ProductID   -- Link 2: Product Inventory
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    INNER JOIN EmployeeTerritories ET ON E.EmployeeID = ET.EmployeeID
    INNER JOIN Territories T ON ET.TerritoryID = T.TerritoryID -- Link 3: Regional Coverage
    WHERE E.Title = 'Sales Representative' 
      AND C.CategoryName = 'Beverages'
      AND T.RegionID = 3;

OPEN MomentumLinkCursor;
FETCH NEXT FROM MomentumLinkCursor INTO 
    @v_OrderID, @v_EmployeeID, @v_ProductID, @v_TerritoryID, @v_EmployeeLastName, @v_UnitPrice, @v_Quantity, @v_Freight;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextMomentumAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_TransactionFinancialMass = @v_UnitPrice * @v_Quantity;
    SET @v_RegionalExpansionVelocity = SQRT(CAST(@v_Freight AS FLOAT) * ABS(CHECKSUM(@v_TerritoryID) % 100));
    SET @v_PersonnelYieldExponent = POWER(CAST(@v_EmployeeID AS FLOAT), 1.15) / LOG(CAST(@v_Freight AS FLOAT) + 4.0);

    INSERT INTO Table_GeographicSalesMomentumLedger (
        MomentumAuditID, OrderID, EmployeeID, ProductID, TerritoryID, EmployeeLastName, 
        TransactionFinancialMass, RegionalExpansionVelocity, PersonnelYieldExponent
    )
    VALUES (
        @nextMomentumAuditID, @v_OrderID, @v_EmployeeID, @v_ProductID, @v_TerritoryID, @v_EmployeeLastName, 
        @v_TransactionFinancialMass, @v_RegionalExpansionVelocity, @v_PersonnelYieldExponent
    );

    -- Log Quad-Source Lineage (Central Table + Three Strategic Links)
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_GeographicSalesMomentumLedger', 'MomentumAuditID', CAST(@nextMomentumAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID AS VARCHAR), 'Table_GeographicSalesMomentumLedger', 'MomentumAuditID', CAST(@nextMomentumAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_GeographicSalesMomentumLedger', 'MomentumAuditID', CAST(@nextMomentumAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Territories', 'TerritoryID', CAST(@v_TerritoryID AS VARCHAR), 'Table_GeographicSalesMomentumLedger', 'MomentumAuditID', CAST(@nextMomentumAuditID AS VARCHAR));
    
    FETCH NEXT FROM MomentumLinkCursor INTO 
        @v_OrderID, @v_EmployeeID, @v_ProductID, @v_TerritoryID, @v_EmployeeLastName, @v_UnitPrice, @v_Quantity, @v_Freight;
END;

CLOSE MomentumLinkCursor; 
DEALLOCATE MomentumLinkCursor;
GO