-- Section 1: Create a View with Selection and Univariate Non-Linear Transformations
-- Scenario: Calculating Biometric Authenticity Scores and Cryptographic Entropy Factors for security hardware.
-- Rule: Use selection and strictly univariate non-linear transformations (POWER, LOG). No bilinear (A*B).
CREATE OR ALTER VIEW vw_SecurityHardwareMetrics AS
SELECT 
    ProductID,
    -- Copied Columns
    ProductName,
    SupplierID,
    UnitPrice,
    -- Non-linear Transformation 1 (Power): Biometric Authenticity Score f(x) = x^1.35
    -- Models how hardware security assurance scales non-linearly with component price/complexity.
    CAST(POWER(UnitPrice, 1.35) / 10.0 AS DECIMAL(10,2)) AS AuthenticityScore,
    -- Non-linear Transformation 2 (Logarithmic): Cryptographic Entropy Factor f(x) = 25 * ln(x + 5)
    -- Models the diminishing increase in randomness/entropy as processing power (proxied by price) increases.
    CAST(LOG(UnitPrice + 5.0) * 25.00 AS DECIMAL(10,2)) AS EntropyFactor
FROM Products
WHERE SupplierID IN (2, 3, 5); -- Selection applied here (Specific electronics/security component suppliers)
GO

-- Log Row-Level Lineage for View
DECLARE @pid INT;
DECLARE ViewCursor CURSOR FOR SELECT ProductID FROM vw_SecurityHardwareMetrics;
OPEN ViewCursor;
FETCH NEXT FROM ViewCursor INTO @pid;
WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('Products', 'ProductID', CAST(@pid AS VARCHAR), 'vw_SecurityHardwareMetrics', 'ProductID', CAST(@pid AS VARCHAR));
    FETCH NEXT FROM ViewCursor INTO @pid;
END;
CLOSE ViewCursor; DEALLOCATE ViewCursor;
GO

-- Section 2: Create a physical table from the view with an additional WHERE clause
-- Tests: Lineage preservation during a secondary selection filter (High-entropy components only).
IF OBJECT_ID('Table_Secure_Core_Audit', 'U') IS NOT NULL DROP TABLE Table_Secure_Core_Audit;
CREATE TABLE Table_Secure_Core_Audit (
    AuditID INT, 
    OriginalProductID INT, 
    ProductName NVARCHAR(40),
    AuthenticityScore DECIMAL(10,2),
    EntropyFactor DECIMAL(10,2)
);
GO

DECLARE @v_pid INT, @v_pname NVARCHAR(40), @v_auth DECIMAL(10,2), @v_entropy DECIMAL(10,2), @nextAuditID INT;

-- Filter: Only process security metrics for components with an entropy factor exceeding 100
DECLARE TableCursor CURSOR FOR 
    SELECT ProductID, ProductName, AuthenticityScore, EntropyFactor 
    FROM vw_SecurityHardwareMetrics 
    WHERE EntropyFactor > 100.00;

OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_auth, @v_entropy;
WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @nextAuditID = NEXT VALUE FOR GlobalIDSequence;
    
    INSERT INTO Table_Secure_Core_Audit (AuditID, OriginalProductID, ProductName, AuthenticityScore, EntropyFactor)
    VALUES (@nextAuditID, @v_pid, @v_pname, @v_auth, @v_entropy);

    INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
    VALUES ('vw_SecurityHardwareMetrics', 'ProductID', CAST(@v_pid AS VARCHAR), 'Table_Secure_Core_Audit', 'AuditID', CAST(@nextAuditID AS VARCHAR));
    
    FETCH NEXT FROM TableCursor INTO @v_pid, @v_pname, @v_auth, @v_entropy;
END;
CLOSE TableCursor; DEALLOCATE TableCursor;
GO

-- Section 3 & 4: Stored Procedures using Global Temporary Tables
CREATE OR ALTER PROCEDURE proc_FinalizeSecurityAuditReport AS
BEGIN
    IF OBJECT_ID('Final_CybersecurityHardwareRegistry', 'U') IS NOT NULL DROP TABLE Final_CybersecurityHardwareRegistry;
    CREATE TABLE Final_CybersecurityHardwareRegistry (
        ReportID INT, 
        HardwareModel NVARCHAR(40), 
        ValidationScore DECIMAL(10,2), 
        EncryptionStrength DECIMAL(10,2),
        AuditStatus VARCHAR(30)
    );

    DECLARE @t_id INT, @t_pname NVARCHAR(40), @t_auth DECIMAL(10,2), @t_entropy DECIMAL(10,2), @finalID INT;
    DECLARE FinalCursor CURSOR FOR 
        SELECT TempID, ProductName, AuthenticityScore, EntropyFactor 
        FROM ##TempSecurityBuffer;

    OPEN FinalCursor;
    FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_auth, @t_entropy;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @finalID = NEXT VALUE FOR GlobalIDSequence;
        
        INSERT INTO Final_CybersecurityHardwareRegistry (ReportID, HardwareModel, ValidationScore, EncryptionStrength, AuditStatus)
        VALUES (@finalID, @t_pname, @t_auth, @t_entropy, 'Classified - Tier 1');

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('##TempSecurityBuffer', 'TempID', CAST(@t_id AS VARCHAR), 'Final_CybersecurityHardwareRegistry', 'ReportID', CAST(@finalID AS VARCHAR));
        
        FETCH NEXT FROM FinalCursor INTO @t_id, @t_pname, @t_auth, @t_entropy;
    END;
    CLOSE FinalCursor; DEALLOCATE FinalCursor;
END;
GO

CREATE OR ALTER PROCEDURE proc_StageSecurityMetrics AS
BEGIN
    IF OBJECT_ID('tempdb..##TempSecurityBuffer') IS NOT NULL DROP TABLE ##TempSecurityBuffer;
    CREATE TABLE ##TempSecurityBuffer (
        TempID INT, 
        ProductName NVARCHAR(40), 
        AuthenticityScore DECIMAL(10,2),
        EntropyFactor DECIMAL(10,2)
    );

    DECLARE @tid INT, @pname NVARCHAR(40), @auth DECIMAL(10,2), @entropy DECIMAL(10,2), @newTempID INT;
    DECLARE ProcCursor CURSOR FOR 
        SELECT AuditID, ProductName, AuthenticityScore, EntropyFactor 
        FROM Table_Secure_Core_Audit;

    OPEN ProcCursor;
    FETCH NEXT FROM ProcCursor INTO @tid, @pname, @auth, @entropy;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @newTempID = NEXT VALUE FOR GlobalIDSequence;
        INSERT INTO ##TempSecurityBuffer VALUES (@newTempID, @pname, @auth, @entropy);

        INSERT INTO DataLineage (SourceName, SourcePKName, SourceID, TargetName, TargetPKName, TargetID)
        VALUES ('Table_Secure_Core_Audit', 'AuditID', CAST(@tid AS VARCHAR), '##TempSecurityBuffer', 'TempID', CAST(@newTempID AS VARCHAR));

        FETCH NEXT FROM ProcCursor INTO @tid, @pname, @auth, @entropy;
    END;
    CLOSE ProcCursor; DEALLOCATE ProcCursor;

    EXEC proc_FinalizeSecurityAuditReport;
END;
GO

EXEC proc_StageSecurityMetrics;