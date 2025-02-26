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
SELECT 
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
