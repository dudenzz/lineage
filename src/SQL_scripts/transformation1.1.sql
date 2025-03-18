-- Step 1: Create an Initial View from a Data Source
GO
CREATE OR ALTER VIEW vw_OrderDetails AS
SELECT OrderID, CustomerID, OrderDate
FROM Orders;

-- Step 2: Create a New Table from the View
--Next, create a table using the view created in the previous step.

GO

IF OBJECT_ID('dbo.OrderSummary', 'U') IS NOT NULL 
  DROP TABLE dbo.OrderSummary; 

GO

SELECT *
INTO OrderSummary
FROM vw_OrderDetails;


-- Step 3: Use a Procedure to Create a Temporary Table based on the New Table
-- Create a stored procedure that will generate a temporary table from the `OrderSummary` table.

GO
CREATE OR ALTER PROCEDURE usp_CreateTempTable AS
BEGIN
	IF OBJECT_ID('tempdb..##TempOrderSummary', 'U') IS NOT NULL 
		DROP TABLE ##TempOrderSummary;
    SELECT *
    INTO ##TempOrderSummary
    FROM OrderSummary;
END;


--Execute the procedure to create the temporary table.

GO
EXEC usp_CreateTempTable;


-- Step 4: Use Another Procedure to Create the Final Table from the Temporary Table

GO
CREATE OR ALTER PROCEDURE usp_CreateFinalTable AS
BEGIN
IF OBJECT_ID('dbo.FinalOrderDetails', 'U') IS NOT NULL 
  DROP TABLE dbo.FinalOrderDetails; 
    SELECT *
    INTO FinalOrderDetails
    FROM ##TempOrderSummary;
END;
-- Execute the second procedure to create the final table.
GO
EXEC usp_CreateFinalTable;
