-- Scenario: Compiling an Enterprise Employee-Customer Service Geographic Impact Ledger.
-- Rule: Combine inputs via INNER JOIN (Two-table link: Orders -> Employees, Orders -> Customers).

IF OBJECT_ID('Table_ServiceGeographicLedger', 'U') IS NOT NULL DROP TABLE Table_ServiceGeographicLedger;
CREATE TABLE Table_ServiceGeographicLedger (
    ServiceAuditID INT,
    OrderID INT,             -- Native Projection from Orders
    EmployeeID INT,          -- Native Projection from Employees
    CustomerID NCHAR(5),     -- Native Projection from Customers
    ShipCountry NVARCHAR(15), -- Native Projection from Orders
    LogisticsMomentum FLOAT, -- Non-Linear Transformation: (Freight * EmployeeID)
    RegionalFocusScore FLOAT, -- Non-Linear Transformation: SQRT(Freight * LEN(CustomerID))
    ServiceDecayIndex FLOAT   -- Non-Linear Transformation: POWER(EmployeeID, 1.1) / LOG(Freight + 5)
);
GO

DECLARE @v_OrderID_S INT,
        @v_EmployeeID_S INT,
        @v_CustomerID_S NCHAR(5),
        @v_ShipCountry_S NVARCHAR(15),
        @v_Freight_S MONEY,
        @v_LogisticsMomentum FLOAT,
        @v_RegionalFocusScore FLOAT,
        @v_ServiceDecayIndex FLOAT,
        @nextServiceAuditID INT;

-- Cursor using two table links (Orders as the center-piece).
-- Selection: Orders shipped to 'Germany' or 'Brazil' handled by 'Sales Representatives'.
DECLARE ServiceLinkCursor CURSOR FOR 
    SELECT 
        O.OrderID, 
        E.EmployeeID, 
        C.CustomerID,
        O.ShipCountry,
        O.Freight
    FROM Orders O
    INNER JOIN Employees E ON O.EmployeeID = E.EmployeeID -- Link 1
    INNER JOIN Customers C ON O.CustomerID = C.CustomerID -- Link 2
    WHERE O.ShipCountry IN ('Germany', 'Brazil')
      AND E.Title = 'Sales Representative';

OPEN ServiceLinkCursor;
FETCH NEXT FROM ServiceLinkCursor INTO 
    @v_OrderID_S, @v_EmployeeID_S, @v_CustomerID_S, @v_ShipCountry_S, @v_Freight_S;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextServiceAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    -- Non-Linear Transformations
    SET @v_LogisticsMomentum = CAST(@v_Freight_S AS FLOAT) * @v_EmployeeID_S; -- A' = A * B
    SET @v_RegionalFocusScore = SQRT(CAST(@v_Freight_S AS FLOAT) * LEN(@v_CustomerID_S));
    SET @v_ServiceDecayIndex = POWER(CAST(@v_EmployeeID_S AS FLOAT), 1.1) / LOG(CAST(@v_Freight_S AS FLOAT) + 5.0);

    INSERT INTO Table_ServiceGeographicLedger (
        ServiceAuditID, OrderID, EmployeeID, CustomerID, ShipCountry, 
        LogisticsMomentum, RegionalFocusScore, ServiceDecayIndex
    )
    VALUES (
        @nextServiceAuditID, @v_OrderID_S, @v_EmployeeID_S, @v_CustomerID_S, @v_ShipCountry_S, 
        @v_LogisticsMomentum, @v_RegionalFocusScore, @v_ServiceDecayIndex
    );

    -- Log Dual-Link Lineage
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Orders', 'OrderID', CAST(@v_OrderID_S AS VARCHAR), 'Table_ServiceGeographicLedger', 'ServiceAuditID', CAST(@nextServiceAuditID AS VARCHAR));
    
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Employees', 'EmployeeID', CAST(@v_EmployeeID_S AS VARCHAR), 'Table_ServiceGeographicLedger', 'ServiceAuditID', CAST(@nextServiceAuditID AS VARCHAR));

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Customers', 'CustomerID', CAST(@v_CustomerID_S AS VARCHAR), 'Table_ServiceGeographicLedger', 'ServiceAuditID', CAST(@nextServiceAuditID AS VARCHAR));
    
    FETCH NEXT FROM ServiceLinkCursor INTO 
        @v_OrderID_S, @v_EmployeeID_S, @v_CustomerID_S, @v_ShipCountry_S, @v_Freight_S;
END;

CLOSE ServiceLinkCursor; 
DEALLOCATE ServiceLinkCursor;
GO