-- Section: Create a Physical Table using Selection, Projection, Joins, and Linear Transformations
-- Scenario: Compiling an Enterprise Supplier-Product Quality Control & Environmental Compliance Ledger.
-- Rule: Combine inputs via INNER JOIN (Three-table link). Selection (WHERE) is permitted. 
-- Rule: Apply Linear Transformations (y = cx + d) to native numeric fields.
-- Lineage: Tracks specific source table and primary key for every entity in the Join relationship.

IF OBJECT_ID('Table_EnvironmentalComplianceLedger', 'U') IS NOT NULL DROP TABLE Table_EnvironmentalComplianceLedger;
CREATE TABLE Table_EnvironmentalComplianceLedger (
    ComplianceAuditID INT,
    ProductID INT,           -- Native Projection from Products
    SupplierID INT,          -- Native Projection from Suppliers
    CategoryID INT,          -- Native Projection from Categories
    ProductName NVARCHAR(40), -- Native Projection from Products
    CompanyName NVARCHAR(40), -- Native Projection from Suppliers
    CarbonFootprintIndex DECIMAL(18,2), -- Linearly transformed column (Products.UnitPrice)
    SustainabilityScore DECIMAL(18,2),  -- Linearly transformed column (Suppliers.SupplierID)
    ComplianceBufferCost MONEY          -- Linearly transformed column (Products.UnitPrice)
);
GO

DECLARE @v_ProductID INT,
        @v_SupplierID INT,
        @v_CategoryID INT,
        @v_ProductName NVARCHAR(40),
        @v_CompanyName NVARCHAR(40),
        @v_CarbonFootprintIndex DECIMAL(18,2),
        @v_SustainabilityScore DECIMAL(18,2),
        @v_ComplianceBufferCost MONEY,
        @nextComplianceAuditID INT;

-- Linear Transformation Constants:
-- 1. Carbon Footprint Index: Estimates environmental impact based on product cost/complexity (y = 0.55 * UnitPrice + 12.00)
-- 2. Sustainability Score: Normalizes vendor ID into a historical compliance ranking (y = 2.10 * SupplierID + 45.00)
-- 3. Compliance Buffer Cost: Projected financial reserve for environmental taxes (y = 0.15 * UnitPrice + 8.50)
DECLARE @CarbonScalar DECIMAL(10,2) = 0.55;
DECLARE @CarbonBase DECIMAL(10,2) = 12.00;
DECLARE @SustainabilityScalar DECIMAL(10,2) = 2.10;
DECLARE @SustainabilityBase DECIMAL(10,2) = 45.00;
DECLARE @ComplianceScalar DECIMAL(10,2) = 0.15;
DECLARE @ComplianceBase MONEY = 8.50;

-- Cursor using JOIN to integrate Category taxonomy, Supplier location, and Product financial metrics.
-- Selection: Only products in 'Condiments' or 'Grains/Cereals' from suppliers in 'Italy', 'Germany', or 'France'.
DECLARE ComplianceCursor CURSOR FOR 
    SELECT 
        P.ProductID, 
        S.SupplierID, 
        C.CategoryID,
        P.ProductName, 
        S.CompanyName, 
        (CAST(P.UnitPrice AS DECIMAL(18,2)) * @CarbonScalar) + @CarbonBase AS CarbonFootprintIndex,
        (CAST(S.SupplierID AS DECIMAL(18,2)) * @SustainabilityScalar) + @SustainabilityBase AS SustainabilityScore,
        (P.UnitPrice * @ComplianceScalar) + @ComplianceBase AS ComplianceBufferCost
    FROM Products P
    INNER JOIN Suppliers S ON P.SupplierID = S.SupplierID
    INNER JOIN Categories C ON P.CategoryID = C.CategoryID
    WHERE C.CategoryName IN ('Condiments', 'Grains/Cereals') 
      AND S.Country IN ('Italy', 'Germany', 'France'); -- Selection

OPEN ComplianceCursor;
FETCH NEXT FROM ComplianceCursor INTO 
    @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ProductName, @v_CompanyName, @v_CarbonFootprintIndex, @v_SustainabilityScore, @v_ComplianceBufferCost;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextComplianceAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_EnvironmentalComplianceLedger (
        ComplianceAuditID, ProductID, SupplierID, CategoryID, ProductName, CompanyName, CarbonFootprintIndex, SustainabilityScore, ComplianceBufferCost
    )
    VALUES (
        @nextComplianceAuditID, @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ProductName, @v_CompanyName, @v_CarbonFootprintIndex, @v_SustainabilityScore, @v_ComplianceBufferCost
    );

    -- Log Triple-Source Lineage
    -- Record source for Products
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@v_ProductID AS VARCHAR), 'Table_EnvironmentalComplianceLedger', 'ComplianceAuditID', CAST(@nextComplianceAuditID AS VARCHAR));
    
    -- Record source for Suppliers
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Suppliers', 'SupplierID', CAST(@v_SupplierID AS VARCHAR), 'Table_EnvironmentalComplianceLedger', 'ComplianceAuditID', CAST(@nextComplianceAuditID AS VARCHAR));

    -- Record source for Categories
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Categories', 'CategoryID', CAST(@v_CategoryID AS VARCHAR), 'Table_EnvironmentalComplianceLedger', 'ComplianceAuditID', CAST(@nextComplianceAuditID AS VARCHAR));
    
    FETCH NEXT FROM ComplianceCursor INTO 
        @v_ProductID, @v_SupplierID, @v_CategoryID, @v_ProductName, @v_CompanyName, @v_CarbonFootprintIndex, @v_SustainabilityScore, @v_ComplianceBufferCost;
END;

CLOSE ComplianceCursor; 
DEALLOCATE ComplianceCursor;
GO