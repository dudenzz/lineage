-- Section 1: Initial Data Load into Table Variable
-- Tests: Lineage from a physical table to a memory-resident table variable.
DECLARE @ProductBuffer TABLE (
    ProdID INT,
    Price DECIMAL(10,2),
    CatID INT,
    Status NVARCHAR(20)
);

INSERT INTO @ProductBuffer (ProdID, Price, CatID, Status)
SELECT ProductID, UnitPrice, CategoryID, 'Pending'
FROM Products
WHERE Discontinued = 0;

-- Log Lineage: Physical Source to Memory Variable
-- (In a real benchmark, the tool must identify this link by parsing the INSERT...SELECT)
INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
SELECT 'Products', 'ProductID', CAST(ProdID AS VARCHAR), '@ProductBuffer', 'ProdID', CAST(ProdID AS VARCHAR)
FROM @ProductBuffer;

-- Section 2: Transforming the Table Variable
-- Tests: Intra-batch lineage where an object is modified by a separate source.
DECLARE @DiscountLookup TABLE (CatID INT, DiscountPct DECIMAL(4,2));
INSERT INTO @DiscountLookup VALUES (1, 0.10), (2, 0.05), (3, 0.20);

-- Update the buffer based on the lookup table
-- Tests: 'Used for creation' predicate involving multiple table variables.
UPDATE pb
SET pb.Price = pb.Price * (1 - dl.DiscountPct),
    pb.Status = 'Discounted'
FROM @ProductBuffer pb
JOIN @DiscountLookup dl ON pb.CatID = dl.CatID;

-- Section 3: Final Persistence from Table Variable
-- Tests: Finalizing lineage from a non-persistent variable to a physical table.
IF OBJECT_ID('Final_Discounted_Products', 'U') IS NOT NULL DROP TABLE Final_Discounted_Products;
CREATE TABLE Final_Discounted_Products (
    DiscountID INT PRIMARY KEY,
    OriginalProductID INT,
    FinalPrice DECIMAL(10,2)
);

DECLARE @cur_pid INT, @cur_price DECIMAL(10,2), @nextDiscountID INT;
DECLARE VarCursor CURSOR FOR SELECT ProdID, Price FROM @ProductBuffer WHERE Status = 'Discounted';

OPEN VarCursor;
FETCH NEXT FROM VarCursor INTO @cur_pid, @cur_price;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextDiscountID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Final_Discounted_Products (DiscountID, OriginalProductID, FinalPrice)
    VALUES (@nextDiscountID, @cur_pid, @cur_price);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('@ProductBuffer', 'ProdID', CAST(@cur_pid AS VARCHAR), 'Final_Discounted_Products', 'DiscountID', CAST(@nextDiscountID AS VARCHAR));

    FETCH NEXT FROM VarCursor INTO @cur_pid, @cur_price;
END;
CLOSE VarCursor; DEALLOCATE VarCursor;
GO