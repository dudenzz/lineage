-- Simple Select-Based Lineage

-- Create a new table with a direct selection of columns from the Orders table
GO
SELECT OrderID, CustomerID, OrderDate
INTO NewOrdersSimpleLin
FROM Orders;

-- Create a table with orders placed in 1997
GO
SELECT OrderID, CustomerID, OrderDate
INTO Orders1997SimpleLin
FROM Orders
WHERE YEAR(OrderDate) = 1997;

-- Create a view that combines customer and order information
GO
CREATE VIEW CustomerOrdersSimpleLin AS
SELECT C.CustomerID, C.CompanyName, O.OrderID, O.OrderDate
FROM Customers C
JOIN Orders O ON C.CustomerID = O.CustomerID;

-- Create a table summarizing total sales by year
GO
SELECT YEAR(OrderDate) AS OrderYear, SUM(Freight) AS TotalFreight
INTO AnnualFreightSimpleLin
FROM Orders
GROUP BY YEAR(OrderDate);

-- Create a table with unique ship countries from Orders
GO
SELECT DISTINCT ShipCountry
INTO UniqueShipCountriesSimpleLin
FROM Orders;

-- Create a view with calculated fields for each order
GO
CREATE VIEW OrderDetailsWithTotalSimpleLin AS
SELECT OrderID, ProductID, Quantity, UnitPrice, (Quantity * UnitPrice) AS TotalPrice
FROM [Order Details];

-- Create a table with top 10 expensive products
GO
SELECT TOP 10 ProductID, ProductName, UnitPrice
INTO TopExpensiveProductsSimpleLin
FROM Products
ORDER BY UnitPrice DESC;

-- Create a view with alias for columns
GO
CREATE VIEW SalesOverviewSimpleLin AS
SELECT O.OrderID AS SaleID, C.CompanyName AS CustomerName, P.ProductName, OD.Quantity
FROM Orders O
JOIN [Order Details] OD ON O.OrderID = OD.OrderID
JOIN Products P ON OD.ProductID = P.ProductID
JOIN Customers C ON O.CustomerID = C.CustomerID;


-- Join Operations


-- Create a view with alias for columns
GO
CREATE VIEW CustomerOrdersJoinOps AS
SELECT Customers.CustomerID, Customers.ContactName, Orders.OrderID, Orders.OrderDate
FROM Customers
INNER JOIN Orders ON Customers.CustomerID = Orders.CustomerID;

GO
CREATE VIEW CustomerAndOrdersSummaryJoinOps AS
SELECT Customers.CustomerID, Customers.CompanyName, Orders.OrderID, Orders.OrderDate
FROM Customers
LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID;

GO
CREATE VIEW OrderDetailsAndProductsJoinOps AS
   SELECT [Order Details].OrderID, [Order Details].ProductID, Products.ProductName, Products.CategoryID
   FROM [Order Details]
   RIGHT JOIN Products ON [Order Details].ProductID = Products.ProductID;

GO
CREATE VIEW CompleteOrderCustomerDetailsJoinOps AS
   SELECT Customers.CustomerID, Customers.CompanyName, Orders.OrderID, Orders.ShippedDate
   FROM Customers
   FULL OUTER JOIN Orders ON Customers.CustomerID = Orders.CustomerID;

GO
CREATE VIEW CrossProductCustomersTerritories AS
   SELECT Customers.CustomerID, Customers.ContactName, Territories.TerritoryID, Territories.TerritoryDescription
   FROM Customers
   CROSS JOIN Territories;

--Simple Union of Datasets

GO
SELECT City, Country FROM Customers
UNION
SELECT City, Country FROM Suppliers;

GO
SELECT Phone FROM Employees
UNION
SELECT Phone FROM Shippers;

GO
SELECT ProductName AS Name FROM Products
UNION
SELECT CategoryName AS Name FROM Categories;

GO
SELECT OrderID FROM Orders
UNION
SELECT CustomerID FROM Customers;

-- Combine first names of employees with supplier names
GO
SELECT FirstName AS Name FROM Employees
UNION
SELECT CompanyName AS Name FROM Suppliers


--Subquery ops

SELECT ProductName, UnitPrice, 
       (SELECT AVG(UnitPrice) FROM Products) AS AvgPrice
FROM Products;

SELECT ProductName, UnitPrice 
FROM Products
WHERE UnitPrice > (SELECT AVG(UnitPrice) FROM Products);

SELECT CustomerID, OrderCount
FROM (
    SELECT CustomerID, COUNT(OrderID) AS OrderCount
    FROM Orders
    GROUP BY CustomerID
) AS CustomerOrders;

SELECT OrderID, 
       (SELECT MAX(OrderDate) 
        FROM Orders AS O2 
        WHERE O1.CustomerID = O2.CustomerID) AS LastOrderDate
FROM Orders AS O1;

SELECT ProductName, 
       (SELECT SUM(Quantity) FROM [Order Details] WHERE Products.ProductID = [Order Details].ProductID) AS TotalQuantitySold
FROM Products;


--view based
GO
CREATE VIEW vw_Customers AS
SELECT CustomerID, ContactName, City, Country
FROM Customers;


GO
CREATE VIEW vw_OrderDetailsWithNames AS
SELECT od.OrderID, od.ProductID, p.ProductName, od.Quantity
FROM [Order Details] od
JOIN Products p ON od.ProductID = p.ProductID;

GO
CREATE VIEW vw_ProductsInfo AS
SELECT ProductID, ProductName, SupplierID, CategoryID
FROM Products;

GO
CREATE VIEW vw_ProductsExtended AS
SELECT p.ProductID, p.ProductName, s.CompanyName AS SupplierName, c.CategoryName
FROM vw_ProductsInfo p
JOIN Suppliers s ON p.SupplierID = s.SupplierID
JOIN Categories c ON p.CategoryID = c.CategoryID;

GO
CREATE VIEW vw_CustomerOrderCounts AS
SELECT CustomerID, COUNT(OrderID) AS OrderCount
FROM Orders
GROUP BY CustomerID;

GO
CREATE VIEW vw_ActiveCustomers AS
SELECT CustomerID, CompanyName, CASE WHEN IsActive = 1 THEN 'Active' ELSE 'Inactive' END AS Status
FROM Customers;

--Aggregation
GO
SELECT 
    EmployeeID, 
    SUM(Freight) AS TotalFreight
FROM 
    Orders
GROUP BY 
    EmployeeID;


SELECT 
    CustomerID, 
    COUNT(OrderID) AS NumberOfOrders
FROM 
    Orders
GROUP BY 
    CustomerID
HAVING 
    COUNT(OrderID) > 5;

SELECT 
    ProductID, 
    AVG(UnitPrice) AS AveragePrice
FROM 
    [Order Details]
GROUP BY 
    ProductID
HAVING 
    AVG(UnitPrice) > 50;


SELECT 
    ShipCountry, 
    COUNT(OrderID) AS TotalOrders, 
    SUM(Freight) AS TotalFreight, 
    AVG(Freight) AS AverageFreight
FROM 
    Orders
GROUP BY 
    ShipCountry;

SELECT 
    YEAR(OrderDate) AS OrderYear, 
    SUM(Freight) AS YearlyFreight
FROM 
    Orders
GROUP BY 
    YEAR(OrderDate);

SELECT 
    Categories.CategoryName, 
    COUNT(Products.ProductID) AS NumberOfProducts
FROM 
    Products
JOIN 
    Categories ON Products.CategoryID = Categories.CategoryID
GROUP BY 
    Categories.CategoryName;

SELECT 
    EmployeeID, 
    COUNT(DISTINCT CustomerID) AS UniqueCustomers
FROM 
    Orders
GROUP BY 
    EmployeeID;

-- data transformation

SELECT EmployeeID, SUM(UnitPrice * Quantity) as TotalSales
INTO EmployeeSales
FROM [Order Details]
GROUP BY EmployeeID;


-- Query
SELECT c.CustomerID, c.ContactName, o.OrderID, o.OrderDate
INTO CustomerOrdersDataTrans
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID;

-- Query
SELECT OrderID, ProductID, Quantity, UnitPrice, 
       Quantity * UnitPrice AS TotalPrice
INTO OrderCalculationsDataTrans
FROM [Order Details];

SELECT OrderID, CustomerID, 
       YEAR(OrderDate) AS OrderYear
INTO OrdersByYear
FROM Orders;

-- Query
SELECT ProductID, ProductName, 
       CASE 
           WHEN UnitsInStock > 50 THEN 'High Stock'
           WHEN UnitsInStock BETWEEN 10 AND 50 THEN 'Medium Stock'
           ELSE 'Low Stock'
       END AS StockLevel
INTO StockLevels
FROM Products;

-- Stored procedures
GO
CREATE PROCEDURE InsertNewProduct
    @ProductName NVARCHAR(40), 
    @SupplierID INT, 
    @CategoryID INT,
    @QuantityPerUnit NVARCHAR(20),
    @UnitPrice MONEY, 
    @UnitsInStock SMALLINT
AS
BEGIN
    INSERT INTO Products (ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice, UnitsInStock)
    VALUES (@ProductName, @SupplierID, @CategoryID, @QuantityPerUnit, @UnitPrice, @UnitsInStock);
END;

GO
CREATE PROCEDURE DynamicInsertOrder
    @CustomerID NCHAR(5), 
    @EmployeeID INT, 
    @OrderDate DATETIME
AS
BEGIN
    DECLARE @sqlCommand NVARCHAR(MAX);
    SET @sqlCommand = N'INSERT INTO Orders (CustomerID, EmployeeID, OrderDate) VALUES (''' + @CustomerID + ''', ' + CAST(@EmployeeID AS NVARCHAR) + ', ''' + CAST(@OrderDate AS NVARCHAR) + ''')';
    EXEC sp_executesql @sqlCommand;
END;

GO
CREATE PROCEDURE InsertTransformedTerritory
    @TerritoryID NVARCHAR(20),
    @RegionDescription NVARCHAR(50)
AS
BEGIN
    DECLARE @TransformedRegionDescription NVARCHAR(50);
    SET @TransformedRegionDescription = UPPER(@RegionDescription);
    INSERT INTO Territories (TerritoryID, RegionDescription)
    VALUES (@TerritoryID, @TransformedRegionDescription);
END;

GO
CREATE PROCEDURE InsertOrderDetailsFromCursor
AS
BEGIN
    DECLARE orderCursor CURSOR FOR
    SELECT OrderID, ProductID, Quantity FROM Orders_Products;

    DECLARE @OrderID INT, @ProductID INT, @Quantity SMALLINT;

    OPEN orderCursor;

    FETCH NEXT FROM orderCursor INTO @OrderID, @ProductID, @Quantity;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        INSERT INTO OrderDetails (OrderID, ProductID, Quantity)
        VALUES (@OrderID, @ProductID, @Quantity);

        FETCH NEXT FROM orderCursor INTO @OrderID, @ProductID, @Quantity;
    END;

    CLOSE orderCursor;
    DEALLOCATE orderCursor;
END;

GO
CREATE PROCEDURE ConditionalInsertShipper
    @ShipperName NVARCHAR(40),
    @Phone NVARCHAR(24)
AS
BEGIN
    IF NOT EXISTS (SELECT 1 FROM Shippers WHERE CompanyName = @ShipperName)
    BEGIN
        INSERT INTO Shippers (CompanyName, Phone)
        VALUES (@ShipperName, @Phone);
    END
END;

-- temporary and derived

SELECT OrderID, SUM(Quantity) AS TotalQuantity
INTO #TempOrderSummary
FROM [Order Details]
GROUP BY OrderID;

SELECT o.OrderID, o.OrderDate, os.TotalQuantity
FROM Orders o
JOIN #TempOrderSummary os ON o.OrderID = os.OrderID;

SELECT c.CustomerID, c.ContactName, dt.MaxOrderDate
FROM Customers c
JOIN (
    SELECT CustomerID, MAX(OrderDate) AS MaxOrderDate
    FROM Orders
    GROUP BY CustomerID
) dt ON c.CustomerID = dt.CustomerID;


SELECT o.OrderID, p.ProductName, od.Quantity
INTO #OrderProductDetail
FROM Orders o
JOIN [Order Details] od ON o.OrderID = od.OrderID
JOIN Products p ON od.ProductID = p.ProductID;

SELECT * FROM #OrderProductDetail WHERE Quantity > 10;

WITH CustomerOrderTotals AS (
    SELECT CustomerID, SUM(Freight) AS TotalFreight
    FROM Orders
    GROUP BY CustomerID
)
SELECT c.CustomerID, c.CompanyName, cot.TotalFreight
FROM Customers c
JOIN CustomerOrderTotals cot ON c.CustomerID = cot.CustomerID;

SELECT c.CustomerID, c.ContactName, MaxOrderDate
FROM Customers c
JOIN (
    SELECT CustomerID, MAX(OrderDate) AS MaxOrderDate
    FROM Orders
    WHERE OrderID IN (
        SELECT OrderID FROM [Order Details] WHERE Quantity > 10
    )
    GROUP BY CustomerID
) dt ON c.CustomerID = dt.CustomerID;

-- trigger based

-- Assuming there's a table OrderHistory that logs history of orders
GO
CREATE TRIGGER trg_InsertOrderHistory
ON Orders
AFTER INSERT
AS
BEGIN
    INSERT INTO OrderHistory (OrderID, OrderDate, CustomerID, EmployeeID, ShipVia, Freight)
    SELECT OrderID, OrderDate, CustomerID, EmployeeID, ShipVia, Freight
    FROM inserted;
END;


-- Assuming there's a table EmployeeSales to track total sales per employee
GO
CREATE TRIGGER trg_UpdateEmployeeSales
ON [Order Details]
AFTER UPDATE
AS
BEGIN
    UPDATE e
    SET e.TotalSales = e.TotalSales + (i.Quantity * i.UnitPrice)
    FROM EmployeeSales e
    INNER JOIN inserted i ON e.EmployeeID = i.EmployeeID;
END;


-- Assuming there's a table ArchivedProducts for removed products
GO
CREATE TRIGGER trg_DeleteProductArchive
ON Products
AFTER DELETE
AS
BEGIN
    INSERT INTO ArchivedProducts (ProductID, ProductName, SupplierID, CategoryID)
    SELECT ProductID, ProductName, SupplierID, CategoryID
    FROM deleted;
END;


-- Assuming there's a view OrderDetailsView that abstracts [Order Details]
GO
CREATE TRIGGER trg_ViewInsertOrderDetails
ON OrderDetailsView
INSTEAD OF INSERT
AS
BEGIN
    -- Insert into [Order Details] table
    INSERT INTO [Order Details] (OrderID, ProductID, UnitPrice, Quantity, Discount)
    SELECT OrderID, ProductID, UnitPrice, Quantity, Discount
    FROM inserted;
END;

-- Assuming a CustomerInteractions table exists
CREATE TRIGGER trg_InsertCustomerInteraction
ON CustomerDemographics
AFTER INSERT
AS
BEGIN
    DECLARE @interactionType NVARCHAR(50);
    
    SELECT @interactionType = CASE 
                                WHEN d.CustomerTypeID LIKE '%VIP%' THEN 'VIP Interaction'
                                ELSE 'Regular Interaction'
                              END
    FROM inserted d;

    INSERT INTO CustomerInteractions (CustomerTypeID, InteractionType, InteractionDate)
    SELECT CustomerTypeID, @interactionType, GETDATE()
    FROM inserted;
END;