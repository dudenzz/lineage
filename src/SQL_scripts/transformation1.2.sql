
-- Step 1: Create a view based on a data source (Example: Orders table)
GO 
CREATE OR ALTER VIEW vw_OrderDetails AS
SELECT OrderID, ProductID, Quantity
FROM [Order Details];

-- Step 2: Create a table using the view
GO
IF OBJECT_ID('dbo.OrderSummary', 'U') IS NOT NULL 
  DROP TABLE dbo.OrderSummary; 
GO
SELECT OrderID, SUM(Quantity) AS TotalQuantity
INTO OrderSummary 
FROM vw_OrderDetails
GROUP BY OrderID;



-- Step 3: Create a stored procedure to create a temporary table
GO 
CREATE OR ALTER PROCEDURE CreateTempTable AS
BEGIN
    -- Create a temporary table using the OrderSummary table
    IF OBJECT_ID('tempdb..##TempOrderSummary', 'U') IS NOT NULL 
  DROP TABLE ##TempOrderSummary; 
	SELECT * INTO ##TempOrderSummary FROM OrderSummary;
END;

-- Execute the procedure to create the temporary table
go 
EXEC CreateTempTable;

-- Step 4: Create another stored procedure to create a new table from the temp table
GO 
CREATE OR ALTER PROCEDURE FinalizeOrderSummary AS
BEGIN
	IF OBJECT_ID('dbo.FinalOrderSummary', 'U') IS NOT NULL 
	  DROP TABLE FinalOrderSummary; 
    -- Create the final table from the temporary table
    SELECT * INTO FinalOrderSummary FROM ##TempOrderSummary;
END;

-- Execute the procedure to create the final table
go 
EXEC FinalizeOrderSummary;

-- Clean up
DROP PROCEDURE IF EXISTS CreateTempTable;
DROP PROCEDURE IF EXISTS FinalizeOrderSummary;
DROP TABLE IF EXISTS OrderSummary;
DROP VIEW IF EXISTS vw_OrderDetails;



