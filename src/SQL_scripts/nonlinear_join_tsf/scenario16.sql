-- Section: Create a Physical Table using Selection, Projection, Joins (2 links), and Non-Linear Transformations
-- Scenario: Compiling an Enterprise Customer-Product Affiliation & Purchasing Inertia Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Order Details -> Orders -> Customers, Order Details -> Products).
-- Rule: Apply Non-Linear Transformations (y = f(x)) such as A' = A * B, Square Roots, or Power functions.
-- Lineage: Tracks specific source table and primary key for every entity in the relationship.

IF OBJECT_ID('Table_PurchasingInertiaLedger', 'U') IS NOT NULL DROP TABLE Table_PurchasingInertiaLedger;
CREATE TABLE Table_PurchasingInertiaLedger (
    InertiaAuditID INT,
    OrderID INT,               -- Native Projection from Orders
    CustomerID NCHAR(5),       -- Native Projection from Customers
    ProductID INT,             -- Native Projection from Products
    CompanyName NVARCHAR(40),  -- Native Projection from Customers
    AcquisitionMass MONEY,     -- Non-Linear Transformation: (UnitPrice * Quantity)
    ClientEngagementCurvature FLOAT, -- Non-Linear Transformation: SQRT(Quantity * LEN(CustomerID))
    UnitEconomicExponent FLOAT -- Non-Linear Transformation: POWER(UnitPrice, 1.1) / LOG(Quantity + 1.5)
);
GO

DECLARE @v_OrderID INT,
        @v_CustomerID NCHAR(5),
        @v_ProductID INT,
        @v_CompanyName NVARCHAR(40),
        @v_UnitPrice MONEY,
        @v_Quantity SMALLINT,
        @v_AcquisitionMass MONEY,
        @v_ClientEngagementCurvature FLOAT,
        @v_UnitEconomicExponent FLOAT,
        @nextInertiaAuditID INT;

-- Cursor using two table links branched from the central Order Details entity.
-- Selection: Only transactions involving 'Beverages' or 'Dairy Products' for customers in 'USA' or 'Mexico'.
-- Transformations:
-- 1. Acquisition Mass: Non-linear product of unit value and volume (A' = A * B).
-- 2. Client Engagement Curvature: Square root interaction between quantity and customer ID length.
-- 3. Unit Economic Exponent: Price-to-volume ratio scaling to identify high-value/low-volume anomalies.
DECLARE InertiaLinkCursor CURSOR FOR 
    SELECT 
        OD.OrderID, 
        C.CustomerID, 
        P.ProductID,
        C.CompanyName,
        OD.UnitPrice,
        OD.Quantity
    FROM [Order Details] OD
    INNER JOIN Orders O ON OD.OrderID = O.OrderID
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID -- Link 1: Customer Demographics
    INNER JOIN Products P ON OD.ProductID = P.ProductID   -- Link 2: Product Catalog
    INNER JOIN Categories CAT ON P.CategoryID = CAT.CategoryID
    WHERE O.ShipCountry IN ('USA', 'Mexico') 
      AND CAT.CategoryName IN ('Beverages', 'Dairy Products')
      AND OD.Quantity > 5;

OPEN InertiaLinkCursor;
FETCH NEXT FROM InertiaLinkCursor INTO 
    @v_OrderID, @v_CustomerID, @v_ProductID, @v_CompanyName, @v_UnitPrice, @v_Quantity;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextInertiaAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Execute Non-Linear Calculations
    SET @v_AcquisitionMass = @v_UnitPrice * @v_Quantity;
    SET @v_ClientEngagementCurvature = SQRT(CAST(@v_Quantity AS FLOAT) * CAST(LEN(@v_CustomerID) AS FLOAT));
    SET @v_UnitEconomicExponent = POWER(CAST(@v_UnitPrice AS FLOAT), 1.1) / LOG(CAST(@v_Quantity AS FLOAT) + 1.5);

    INSERT INTO Table_PurchasingInertiaLedger (
        InertiaAuditID, OrderID, CustomerID, ProductID, CompanyName, 
        AcquisitionMass, ClientEngagementCurvature, UnitEconomicExponent
    )
    VALUES (
        @nextInertiaAuditID, @v_OrderID, @v_CustomerID, @v_ProductID, @v_CompanyName, 
        @v_AcquisitionMass, @v_ClientEngagementCurvature, @v_UnitEconomicExponent
    );

    -- Log Triple-Source Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID AS VARCHAR), 'Table_PurchasingInertiaLedger', 'InertiaAuditID', CAST(@nextInertiaAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID AS VARCHAR), 'Table_PurchasingInertiaLedger', 'InertiaAuditID', CAST(@nextInertiaAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_PurchasingInertiaLedger', 'InertiaAuditID', CAST(@nextInertiaAuditID AS VARCHAR));
    
    FETCH NEXT FROM InertiaLinkCursor INTO 
        @v_OrderID, @v_CustomerID, @v_ProductID, @v_CompanyName, @v_UnitPrice, @v_Quantity;
END;

CLOSE InertiaLinkCursor; 
DEALLOCATE InertiaLinkCursor;
GO