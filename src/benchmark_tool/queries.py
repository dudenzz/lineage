from config import db_database
from os import getcwd
drop_database = f"""IF EXISTS (SELECT name from sys.databases WHERE (name = '{db_database}'))
    BEGIN
        ALTER DATABASE {db_database} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE {db_database};
    END;"""
create_database = f"""
CREATE DATABASE {db_database} ON
(NAME = Sales_dat,
    FILENAME = '{getcwd()}\databases\{db_database}.mdf',
    SIZE = 10,
    MAXSIZE = 50,
    FILEGROWTH = 5)
LOG ON
(NAME = Sales_log,
    FILENAME = '{getcwd()}\databases\{db_database}.ldf',
    SIZE = 5 MB,
    MAXSIZE = 25 MB,
    FILEGROWTH = 5 MB);
"""

create_lineage_structure = f"""
IF OBJECT_ID('dbo.DataLineage', 'U') IS NOT NULL DROP TABLE dbo.DataLineage; 
CREATE TABLE DataLineage (
    SourceName VARCHAR(255), SourcePKName VARCHAR(255), SourceID VARCHAR(255), 
    TargetName VARCHAR(255), TargetPKName VARCHAR(255), TargetID VARCHAR(255)
);

IF EXISTS (SELECT * FROM sys.sequences WHERE name = 'GlobalIDSequence')
    DROP SEQUENCE GlobalIDSequence;
CREATE SEQUENCE GlobalIDSequence START WITH 1 INCREMENT BY 1;
"""

get_all_tables = f"""
SELECT * 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE='BASE TABLE'
"""

get_all_views = f"""
  SELECT *
  FROM INFORMATION_SCHEMA.VIEWS 
"""

def get_table_schema_generator(table_name, table_schema = None):
    return f"""
SELECT 
    column_name, 
    data_type, 
    character_maximum_length AS max_length, 
    is_nullable,
    column_default
FROM 
    information_schema.columns
WHERE 
    table_name = '{table_name}' 
    -- AND table_schema = {table_schema} -- Uncomment and change if you use specific schemas (e.g., 'dbo' in SQL Server)
ORDER BY 
    ordinal_position;
    """

def get_table_constraints_generator(table_name, table_schema = None):
    return f"""
SELECT 
    tc.constraint_type,
    tc.constraint_name,
    kcu.column_name
FROM 
    information_schema.table_constraints AS tc
JOIN 
    information_schema.key_column_usage AS kcu 
    ON tc.constraint_name = kcu.constraint_name 
    AND tc.table_schema = kcu.table_schema
WHERE 
    tc.table_name = '{table_name}'
ORDER BY 
    tc.constraint_type, 
    tc.constraint_name, 
    kcu.ordinal_position;
    """

def get_table_fks_generator(table_name, table_schema = None):
    return f"""
SELECT 
    tc.constraint_name,
    kcu.column_name AS foreign_key_column,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM 
    information_schema.table_constraints AS tc
JOIN 
    information_schema.key_column_usage AS kcu 
    ON tc.constraint_name = kcu.constraint_name
JOIN 
    information_schema.constraint_column_usage AS ccu 
    ON ccu.constraint_name = tc.constraint_name
WHERE 
    tc.constraint_type = 'FOREIGN KEY' 
    AND tc.table_name = '{table_name}';
    """
scenario_select_1 = open('src/SQL_scripts/select/scenario1.sql').read()
scenario_select_2 = open('src/SQL_scripts/select/scenario2.sql').read()
scenario_select_3 = open('src/SQL_scripts/select/scenario3.sql').read()
scenario_select_4 = open('src/SQL_scripts/select/scenario4.sql').read()
scenario_select_5 = open('src/SQL_scripts/select/scenario5.sql').read()

scenario_select_linear_1 = open('src/SQL_scripts/linear_sel_tsf/scenario1.sql').read()
scenario_select_linear_2 = open('src/SQL_scripts/linear_sel_tsf/scenario2.sql').read()
scenario_select_linear_3 = open('src/SQL_scripts/linear_sel_tsf/scenario3.sql').read()
scenario_select_linear_4 = open('src/SQL_scripts/linear_sel_tsf/scenario4.sql').read()
scenario_select_linear_5 = open('src/SQL_scripts/linear_sel_tsf/scenario5.sql').read()
scenario_select_linear_6 = open('src/SQL_scripts/linear_sel_tsf/scenario6.sql').read()
scenario_select_linear_7 = open('src/SQL_scripts/linear_sel_tsf/scenario7.sql').read()
scenario_select_linear_8 = open('src/SQL_scripts/linear_sel_tsf/scenario8.sql').read()
scenario_select_linear_9 = open('src/SQL_scripts/linear_sel_tsf/scenario9.sql').read()
scenario_select_linear_10 = open('src/SQL_scripts/linear_sel_tsf/scenario10.sql').read()
scenario_select_linear_11 = open('src/SQL_scripts/linear_sel_tsf/scenario11.sql').read()
scenario_select_linear_12 = open('src/SQL_scripts/linear_sel_tsf/scenario12.sql').read()
scenario_select_linear_13 = open('src/SQL_scripts/linear_sel_tsf/scenario13.sql').read()
scenario_select_linear_14 = open('src/SQL_scripts/linear_sel_tsf/scenario14.sql').read()
scenario_select_linear_15 = open('src/SQL_scripts/linear_sel_tsf/scenario15.sql').read()
scenario_select_linear_16 = open('src/SQL_scripts/linear_sel_tsf/scenario16.sql').read()
scenario_select_linear_17 = open('src/SQL_scripts/linear_sel_tsf/scenario17.sql').read()
scenario_select_linear_18 = open('src/SQL_scripts/linear_sel_tsf/scenario18.sql').read()
scenario_select_linear_19 = open('src/SQL_scripts/linear_sel_tsf/scenario19.sql').read()
scenario_select_linear_20 = open('src/SQL_scripts/linear_sel_tsf/scenario20.sql').read()

scenario_select_notrans_1 = open('src/SQL_scripts/notrans_sel_tsf/scenario1.sql').read()
scenario_select_notrans_2 = open('src/SQL_scripts/notrans_sel_tsf/scenario2.sql').read()
scenario_select_notrans_3 = open('src/SQL_scripts/notrans_sel_tsf/scenario3.sql').read()
scenario_select_notrans_4 = open('src/SQL_scripts/notrans_sel_tsf/scenario4.sql').read()
scenario_select_notrans_5 = open('src/SQL_scripts/notrans_sel_tsf/scenario5.sql').read()
scenario_select_notrans_6 = open('src/SQL_scripts/notrans_sel_tsf/scenario6.sql').read()
scenario_select_notrans_7 = open('src/SQL_scripts/notrans_sel_tsf/scenario7.sql').read()
scenario_select_notrans_8 = open('src/SQL_scripts/notrans_sel_tsf/scenario8.sql').read()
scenario_select_notrans_9 = open('src/SQL_scripts/notrans_sel_tsf/scenario9.sql').read()
scenario_select_notrans_10 = open('src/SQL_scripts/notrans_sel_tsf/scenario10.sql').read()
scenario_select_notrans_11 = open('src/SQL_scripts/notrans_sel_tsf/scenario11.sql').read()
scenario_select_notrans_12 = open('src/SQL_scripts/notrans_sel_tsf/scenario12.sql').read()
scenario_select_notrans_13 = open('src/SQL_scripts/notrans_sel_tsf/scenario13.sql').read()
scenario_select_notrans_14 = open('src/SQL_scripts/notrans_sel_tsf/scenario14.sql').read()
scenario_select_notrans_15 = open('src/SQL_scripts/notrans_sel_tsf/scenario15.sql').read()
scenario_select_notrans_16 = open('src/SQL_scripts/notrans_sel_tsf/scenario16.sql').read()
scenario_select_notrans_17 = open('src/SQL_scripts/notrans_sel_tsf/scenario17.sql').read()
scenario_select_notrans_18 = open('src/SQL_scripts/notrans_sel_tsf/scenario18.sql').read()
scenario_select_notrans_19 = open('src/SQL_scripts/notrans_sel_tsf/scenario19.sql').read()
scenario_select_notrans_20 = open('src/SQL_scripts/notrans_sel_tsf/scenario20.sql').read()

scenario_select_nonlin_1 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario1.sql').read()
scenario_select_nonlin_2 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario2.sql').read()
scenario_select_nonlin_3 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario3.sql').read()
scenario_select_nonlin_4 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario4.sql').read()
scenario_select_nonlin_5 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario5.sql').read()
scenario_select_nonlin_6 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario6.sql').read()
scenario_select_nonlin_7 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario7.sql').read()
scenario_select_nonlin_8 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario8.sql').read()
scenario_select_nonlin_9 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario9.sql').read()
scenario_select_nonlin_10 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario10.sql').read()
scenario_select_nonlin_11 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario11.sql').read()
scenario_select_nonlin_12 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario12.sql').read()
scenario_select_nonlin_13 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario13.sql').read()
scenario_select_nonlin_14 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario14.sql').read()
scenario_select_nonlin_15 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario15.sql').read()
scenario_select_nonlin_16 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario16.sql').read()
scenario_select_nonlin_17 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario17.sql').read()
scenario_select_nonlin_18 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario18.sql').read()
scenario_select_nonlin_19 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario19.sql').read()
scenario_select_nonlin_20 = open('src/SQL_scripts/nonlinear_sel_tsf/scenario20.sql').read()

scenario_join_linear_1 = open('src/SQL_scripts/linear_join_tsf/scenario1.sql').read()
scenario_join_linear_2 = open('src/SQL_scripts/linear_join_tsf/scenario2.sql').read()
scenario_join_linear_3 = open('src/SQL_scripts/linear_join_tsf/scenario3.sql').read()
scenario_join_linear_4 = open('src/SQL_scripts/linear_join_tsf/scenario4.sql').read()
scenario_join_linear_5 = open('src/SQL_scripts/linear_join_tsf/scenario5.sql').read()
scenario_join_linear_6 = open('src/SQL_scripts/linear_join_tsf/scenario6.sql').read()
scenario_join_linear_7 = open('src/SQL_scripts/linear_join_tsf/scenario7.sql').read()
scenario_join_linear_8 = open('src/SQL_scripts/linear_join_tsf/scenario8.sql').read()
scenario_join_linear_9 = open('src/SQL_scripts/linear_join_tsf/scenario9.sql').read()
scenario_join_linear_10 = open('src/SQL_scripts/linear_join_tsf/scenario10.sql').read()
scenario_join_linear_11 = open('src/SQL_scripts/linear_join_tsf/scenario11.sql').read()
scenario_join_linear_12 = open('src/SQL_scripts/linear_join_tsf/scenario12.sql').read()
scenario_join_linear_13 = open('src/SQL_scripts/linear_join_tsf/scenario13.sql').read()
scenario_join_linear_14 = open('src/SQL_scripts/linear_join_tsf/scenario14.sql').read()
scenario_join_linear_15 = open('src/SQL_scripts/linear_join_tsf/scenario15.sql').read()
scenario_join_linear_16 = open('src/SQL_scripts/linear_join_tsf/scenario16.sql').read()
scenario_join_linear_17 = open('src/SQL_scripts/linear_join_tsf/scenario17.sql').read()
scenario_join_linear_18 = open('src/SQL_scripts/linear_join_tsf/scenario18.sql').read()
scenario_join_linear_19 = open('src/SQL_scripts/linear_join_tsf/scenario19.sql').read()
scenario_join_linear_20 = open('src/SQL_scripts/linear_join_tsf/scenario20.sql').read()

scenario_join_notrans_1 = open('src/SQL_scripts/notrans_join_tsf/scenario1.sql').read()
scenario_join_notrans_2 = open('src/SQL_scripts/notrans_join_tsf/scenario2.sql').read()
scenario_join_notrans_3 = open('src/SQL_scripts/notrans_join_tsf/scenario3.sql').read()
scenario_join_notrans_4 = open('src/SQL_scripts/notrans_join_tsf/scenario4.sql').read()
scenario_join_notrans_5 = open('src/SQL_scripts/notrans_join_tsf/scenario5.sql').read()
scenario_join_notrans_6 = open('src/SQL_scripts/notrans_join_tsf/scenario6.sql').read()
scenario_join_notrans_7 = open('src/SQL_scripts/notrans_join_tsf/scenario7.sql').read()
scenario_join_notrans_8 = open('src/SQL_scripts/notrans_join_tsf/scenario8.sql').read()
scenario_join_notrans_9 = open('src/SQL_scripts/notrans_join_tsf/scenario9.sql').read()
scenario_join_notrans_10 = open('src/SQL_scripts/notrans_join_tsf/scenario10.sql').read()
scenario_join_notrans_11 = open('src/SQL_scripts/notrans_join_tsf/scenario11.sql').read()
scenario_join_notrans_12 = open('src/SQL_scripts/notrans_join_tsf/scenario12.sql').read()
scenario_join_notrans_13 = open('src/SQL_scripts/notrans_join_tsf/scenario13.sql').read()
scenario_join_notrans_14 = open('src/SQL_scripts/notrans_join_tsf/scenario14.sql').read()
scenario_join_notrans_15 = open('src/SQL_scripts/notrans_join_tsf/scenario15.sql').read()
scenario_join_notrans_16 = open('src/SQL_scripts/notrans_join_tsf/scenario16.sql').read()
scenario_join_notrans_17 = open('src/SQL_scripts/notrans_join_tsf/scenario17.sql').read()
scenario_join_notrans_18 = open('src/SQL_scripts/notrans_join_tsf/scenario18.sql').read()
scenario_join_notrans_19 = open('src/SQL_scripts/notrans_join_tsf/scenario19.sql').read()
scenario_join_notrans_20 = open('src/SQL_scripts/notrans_join_tsf/scenario20.sql').read()

scenario_join_nonlin_1 = open('src/SQL_scripts/nonlinear_join_tsf/scenario1.sql').read()
scenario_join_nonlin_2 = open('src/SQL_scripts/nonlinear_join_tsf/scenario2.sql').read()
scenario_join_nonlin_3 = open('src/SQL_scripts/nonlinear_join_tsf/scenario3.sql').read()
scenario_join_nonlin_4 = open('src/SQL_scripts/nonlinear_join_tsf/scenario4.sql').read()
scenario_join_nonlin_5 = open('src/SQL_scripts/nonlinear_join_tsf/scenario5.sql').read()
scenario_join_nonlin_6 = open('src/SQL_scripts/nonlinear_join_tsf/scenario6.sql').read()
scenario_join_nonlin_7 = open('src/SQL_scripts/nonlinear_join_tsf/scenario7.sql').read()
scenario_join_nonlin_8 = open('src/SQL_scripts/nonlinear_join_tsf/scenario8.sql').read()
scenario_join_nonlin_9 = open('src/SQL_scripts/nonlinear_join_tsf/scenario9.sql').read()
scenario_join_nonlin_10 = open('src/SQL_scripts/nonlinear_join_tsf/scenario10.sql').read()
scenario_join_nonlin_11 = open('src/SQL_scripts/nonlinear_join_tsf/scenario11.sql').read()
scenario_join_nonlin_12 = open('src/SQL_scripts/nonlinear_join_tsf/scenario12.sql').read()
scenario_join_nonlin_13 = open('src/SQL_scripts/nonlinear_join_tsf/scenario13.sql').read()
scenario_join_nonlin_14 = open('src/SQL_scripts/nonlinear_join_tsf/scenario14.sql').read()
scenario_join_nonlin_15 = open('src/SQL_scripts/nonlinear_join_tsf/scenario15.sql').read()
scenario_join_nonlin_16 = open('src/SQL_scripts/nonlinear_join_tsf/scenario16.sql').read()
scenario_join_nonlin_17 = open('src/SQL_scripts/nonlinear_join_tsf/scenario17.sql').read()
scenario_join_nonlin_18 = open('src/SQL_scripts/nonlinear_join_tsf/scenario18.sql').read()
scenario_join_nonlin_19 = open('src/SQL_scripts/nonlinear_join_tsf/scenario19.sql').read()
scenario_join_nonlin_20 = open('src/SQL_scripts/nonlinear_join_tsf/scenario20.sql').read()


scenario_uni_linear_1 = open('src/SQL_scripts/linear_uni_tsf/scenario1.sql').read()
scenario_uni_linear_2 = open('src/SQL_scripts/linear_uni_tsf/scenario2.sql').read()
scenario_uni_linear_3 = open('src/SQL_scripts/linear_uni_tsf/scenario3.sql').read()
scenario_uni_linear_4 = open('src/SQL_scripts/linear_uni_tsf/scenario4.sql').read()
scenario_uni_linear_5 = open('src/SQL_scripts/linear_uni_tsf/scenario5.sql').read()
scenario_uni_linear_6 = open('src/SQL_scripts/linear_uni_tsf/scenario6.sql').read()
scenario_uni_linear_7 = open('src/SQL_scripts/linear_uni_tsf/scenario7.sql').read()
scenario_uni_linear_8 = open('src/SQL_scripts/linear_uni_tsf/scenario8.sql').read()
scenario_uni_linear_9 = open('src/SQL_scripts/linear_uni_tsf/scenario9.sql').read()
scenario_uni_linear_10 = open('src/SQL_scripts/linear_uni_tsf/scenario10.sql').read()
scenario_uni_linear_11 = open('src/SQL_scripts/linear_uni_tsf/scenario11.sql').read()
scenario_uni_linear_12 = open('src/SQL_scripts/linear_uni_tsf/scenario12.sql').read()
scenario_uni_linear_13 = open('src/SQL_scripts/linear_uni_tsf/scenario13.sql').read()
scenario_uni_linear_14 = open('src/SQL_scripts/linear_uni_tsf/scenario14.sql').read()
scenario_uni_linear_15 = open('src/SQL_scripts/linear_uni_tsf/scenario15.sql').read()
scenario_uni_linear_16 = open('src/SQL_scripts/linear_uni_tsf/scenario16.sql').read()
scenario_uni_linear_17 = open('src/SQL_scripts/linear_uni_tsf/scenario17.sql').read()
scenario_uni_linear_18 = open('src/SQL_scripts/linear_uni_tsf/scenario18.sql').read()
scenario_uni_linear_19 = open('src/SQL_scripts/linear_uni_tsf/scenario19.sql').read()
scenario_uni_linear_20 = open('src/SQL_scripts/linear_uni_tsf/scenario20.sql').read()

scenario_uni_notrans_1 = open('src/SQL_scripts/notrans_uni_tsf/scenario1.sql').read()
scenario_uni_notrans_2 = open('src/SQL_scripts/notrans_uni_tsf/scenario2.sql').read()
scenario_uni_notrans_3 = open('src/SQL_scripts/notrans_uni_tsf/scenario3.sql').read()
scenario_uni_notrans_4 = open('src/SQL_scripts/notrans_uni_tsf/scenario4.sql').read()
scenario_uni_notrans_5 = open('src/SQL_scripts/notrans_uni_tsf/scenario5.sql').read()
scenario_uni_notrans_6 = open('src/SQL_scripts/notrans_uni_tsf/scenario6.sql').read()
scenario_uni_notrans_7 = open('src/SQL_scripts/notrans_uni_tsf/scenario7.sql').read()
scenario_uni_notrans_8 = open('src/SQL_scripts/notrans_uni_tsf/scenario8.sql').read()
scenario_uni_notrans_9 = open('src/SQL_scripts/notrans_uni_tsf/scenario9.sql').read()
scenario_uni_notrans_10 = open('src/SQL_scripts/notrans_uni_tsf/scenario10.sql').read()
scenario_uni_notrans_11 = open('src/SQL_scripts/notrans_uni_tsf/scenario11.sql').read()
scenario_uni_notrans_12 = open('src/SQL_scripts/notrans_uni_tsf/scenario12.sql').read()
scenario_uni_notrans_13 = open('src/SQL_scripts/notrans_uni_tsf/scenario13.sql').read()
scenario_uni_notrans_14 = open('src/SQL_scripts/notrans_uni_tsf/scenario14.sql').read()
scenario_uni_notrans_15 = open('src/SQL_scripts/notrans_uni_tsf/scenario15.sql').read()
scenario_uni_notrans_16 = open('src/SQL_scripts/notrans_uni_tsf/scenario16.sql').read()
scenario_uni_notrans_17 = open('src/SQL_scripts/notrans_uni_tsf/scenario17.sql').read()
scenario_uni_notrans_18 = open('src/SQL_scripts/notrans_uni_tsf/scenario18.sql').read()
scenario_uni_notrans_19 = open('src/SQL_scripts/notrans_uni_tsf/scenario19.sql').read()
scenario_uni_notrans_20 = open('src/SQL_scripts/notrans_uni_tsf/scenario20.sql').read()

scenario_uni_nonlin_1 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario1.sql').read()
scenario_uni_nonlin_2 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario2.sql').read()
scenario_uni_nonlin_3 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario3.sql').read()
scenario_uni_nonlin_4 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario4.sql').read()
scenario_uni_nonlin_5 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario5.sql').read()
scenario_uni_nonlin_6 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario6.sql').read()
scenario_uni_nonlin_7 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario7.sql').read()
scenario_uni_nonlin_8 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario8.sql').read()
scenario_uni_nonlin_9 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario9.sql').read()
scenario_uni_nonlin_10 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario10.sql').read()
scenario_uni_nonlin_11 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario11.sql').read()
scenario_uni_nonlin_12 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario12.sql').read()
scenario_uni_nonlin_13 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario13.sql').read()
scenario_uni_nonlin_14 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario14.sql').read()
scenario_uni_nonlin_15 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario15.sql').read()
scenario_uni_nonlin_16 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario16.sql').read()
scenario_uni_nonlin_17 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario17.sql').read()
scenario_uni_nonlin_18 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario18.sql').read()
scenario_uni_nonlin_19 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario19.sql').read()
scenario_uni_nonlin_20 = open('src/SQL_scripts/nonlinear_uni_tsf/scenario20.sql').read()

scenario_transformation_1 = open('src/SQL_scripts/transformation/scenario1.sql').read()
scenario_transformation_2 = open('src/SQL_scripts/transformation/scenario2.sql').read()
scenario_transformation_3 = open('src/SQL_scripts/transformation/scenario3.sql').read()
scenario_transformation_4 = open('src/SQL_scripts/transformation/scenario4.sql').read()
scenario_transformation_5 = open('src/SQL_scripts/transformation/scenario5.sql').read()

scenario_join_1 = open('src/SQL_scripts/join/scenario1.sql').read()
scenario_join_2 = open('src/SQL_scripts/join/scenario2.sql').read()
scenario_join_3 = open('src/SQL_scripts/join/scenario3.sql').read()
scenario_join_4 = open('src/SQL_scripts/join/scenario4.sql').read()
scenario_join_5 = open('src/SQL_scripts/join/scenario5.sql').read()

# scenario_dedup_1 = open('src/SQL_scripts/deduplication/scenario1.sql').read()
# scenario_dedup_2 = open('src/SQL_scripts/deduplication/scenario2.sql').read()
# scenario_dedup_3 = open('src/SQL_scripts/deduplication/scenario3.sql').read()
# scenario_dedup_4 = open('src/SQL_scripts/deduplication/scenario4.sql').read()
# scenario_dedup_5 = open('src/SQL_scripts/deduplication/scenario5.sql').read()

# scenario_derived_1 = open('src/SQL_scripts/derived_views/scenario1.sql').read()
# scenario_derived_2 = open('src/SQL_scripts/derived_views/scenario2.sql').read()
# scenario_derived_3 = open('src/SQL_scripts/derived_views/scenario3.sql').read()
# scenario_derived_4 = open('src/SQL_scripts/derived_views/scenario4.sql').read()
# scenario_derived_5 = open('src/SQL_scripts/derived_views/scenario5.sql').read()

# scenario_materialized_1 = open('src/SQL_scripts/materialized/scenario1.sql').read()
# scenario_materialized_2 = open('src/SQL_scripts/materialized/scenario2.sql').read()
# scenario_materialized_3 = open('src/SQL_scripts/materialized/scenario3.sql').read()
# scenario_materialized_4 = open('src/SQL_scripts/materialized/scenario4.sql').read()
# scenario_materialized_5 = open('src/SQL_scripts/materialized/scenario5.sql').read()

# scenario_partitioning_1 = open('src/SQL_scripts/partitioning/scenario1.sql').read()
# scenario_partitioning_2 = open('src/SQL_scripts/partitioning/scenario2.sql').read()
# scenario_partitioning_3 = open('src/SQL_scripts/partitioning/scenario3.sql').read()
# scenario_partitioning_4 = open('src/SQL_scripts/partitioning/scenario4.sql').read()
# scenario_partitioning_5 = open('src/SQL_scripts/partitioning/scenario5.sql').read()

# scenario_recursive_1 = open('src/SQL_scripts/recursive/scenario1.sql').read()
# scenario_recursive_2 = open('src/SQL_scripts/recursive/scenario2.sql').read()
# scenario_recursive_3 = open('src/SQL_scripts/recursive/scenario3.sql').read()
# scenario_recursive_4 = open('src/SQL_scripts/recursive/scenario4.sql').read()
# scenario_recursive_5 = open('src/SQL_scripts/recursive/scenario5.sql').read()

# scenario_tabular_1 = open('src/SQL_scripts/tabular/scenario1.sql').read()
# scenario_tabular_2 = open('src/SQL_scripts/tabular/scenario2.sql').read()
# scenario_tabular_3 = open('src/SQL_scripts/tabular/scenario3.sql').read()
# scenario_tabular_4 = open('src/SQL_scripts/tabular/scenario4.sql').read()
# scenario_tabular_5 = open('src/SQL_scripts/tabular/scenario5.sql').read()

# scenario_temporary_1 = open('src/SQL_scripts/temporary/scenario1.sql').read()
# scenario_temporary_2 = open('src/SQL_scripts/temporary/scenario2.sql').read()
# scenario_temporary_3 = open('src/SQL_scripts/temporary/scenario3.sql').read()
# scenario_temporary_4 = open('src/SQL_scripts/temporary/scenario4.sql').read()
# scenario_temporary_5 = open('src/SQL_scripts/temporary/scenario5.sql').read()


scenarios = {
    'select' : [
    {
        'script': scenario_select_1,
        'lineage': [
            ('Employees', 'vw_EmployeeBasicInfo'),
            ('vw_EmployeeBasicInfo', 'Table_US_Employees'),
            ('Table_US_Employees', '##TempStaffBuffer'),
            ('##TempStaffBuffer', 'Final_ActiveStaffReport')
        ]
    },
    {
        'script': scenario_select_2,
        'lineage': [
            ('Products', 'vw_ProductsAllStatus'),
            ('vw_ProductsAllStatus', 'Table_DiscontinuedStaging'),
            ('Table_DiscontinuedStaging', '##TempDiscontinuedBuffer'),
            ('##TempDiscontinuedBuffer', 'Final_DiscontinuedReport')
        ]
    },
    {
        'script': scenario_select_3,
        'lineage': [
            ('Suppliers', 'vw_SupplierContactDetails'),
            ('vw_SupplierContactDetails', 'Table_InternationalSuppliers'),
            ('Table_InternationalSuppliers', '##TempSupplierBuffer'),
            ('##TempSupplierBuffer', 'Final_ExternalSupplierList')
        ]
    },
    {
        'script': scenario_select_4,
        'lineage': [
            ('Customers', 'vw_CustomerBase'),
            ('vw_CustomerBase', 'Table_ExecutiveAccounts'),
            ('Table_ExecutiveAccounts', '##TempMailingBuffer'),
            ('##TempMailingBuffer', 'Final_OwnerMailingList')
        ]
    },
    {
        'script': scenario_select_5,
        'lineage': [
            ('Orders', 'vw_OrderFreightDetails'),
            ('vw_OrderFreightDetails', 'Table_HighFreightStaging'),
            ('Table_HighFreightStaging', '##TempFreightBuffer'),
            ('##TempFreightBuffer', 'Final_PriorityShipmentReport')
        ]
    }
    ],
    'transformation' : [
    {
        'script' : scenario_transformation_1,
        'lineage' : [
            ('[Order Details]', 'vw_OrderTotals'),
            ('vw_OrderTotals','Table_OrderCategories'),
            ('Table_OrderCategories','##TempHighValue'),
            ('##TempHighValue','Final_HighValueReport')
        ]
    },
    {
        'script' : scenario_transformation_2,
        'lineage' : [
            ('Employees', 'vw_EmployeeSalesSummary'),
            ('Orders', 'vw_EmployeeSalesSummary'),
            ('[Order Details]', 'vw_EmployeeSalesSummary'),
            ('vw_EmployeeSalesSummary', 'Table_SalesPerformance'),
            ('Table_SalesPerformance', '##TempEliteSales'),
            ('##TempEliteSales', 'Final_CommissionReport')
        ]
    },
    {
        'script': scenario_transformation_3,
        'lineage': [
            ('Products', 'vw_SupplierStockValue'),
            ('Suppliers', 'vw_SupplierStockValue'),
            ('vw_SupplierStockValue', 'Table_WarehouseInventory'),
            ('Table_WarehouseInventory', 'Final_LogisticsReport')
        ]
    },
    {
        'script': scenario_transformation_4,
        'lineage': [
            ('Products', 'vw_CategoryStockAnalysis'),
            ('vw_CategoryStockAnalysis', 'Table_CategoryTiers'),
            ('Table_CategoryTiers', '##TempPriceBuffer'),
            ('##TempPriceBuffer', 'Final_CategoryReport')
        ]
    },
    {
        'script': scenario_transformation_5,
        'lineage': [
            ('Orders', 'vw_EmployeeOrderVolume'),
            ('vw_EmployeeOrderVolume', 'Table_EmployeeTiers'),
            ('Table_EmployeeTiers', '##TempBonusBuffer'),
            ('##TempBonusBuffer', 'Final_BonusAudit')
            ]
    }
    ],
    'join' : [
    {
        'script' : scenario_join_1,
        'lineage' : [
            ('[Order Details]', 'vw_OrderTotals'),
            ('vw_OrderTotals','Table_OrderCategories'),
            ('Table_OrderCategories','##TempHighValue'),
            ('##TempHighValue','Final_HighValueReport')
    ]},
    {
        'script': scenario_join_2,
        'lineage': [
            ('Orders', 'vw_OrderEmployeeContext'),
            ('Employees', 'vw_OrderEmployeeContext'),
            ('vw_OrderEmployeeContext', 'Table_RegionalAssignments'),
            ('Table_RegionalAssignments', '##TempJoinBuffer'),
            ('##TempJoinBuffer', 'Final_ManagerialSalesAudit')
        ]
    },
    {
        'script': scenario_join_3,
        'lineage': [
            ('Products', 'vw_ProductFullCatalog'),
            ('Categories', 'vw_ProductFullCatalog'),
            ('Suppliers', 'vw_ProductFullCatalog'),
            ('vw_ProductFullCatalog', 'Table_InventoryStaging'),
            ('Table_InventoryStaging', '##TempProcurementBuffer'),
            ('##TempProcurementBuffer', 'Final_ProcurementAudit')
        ]
    },
    {
        'script': scenario_join_4,
        'lineage': [
            ('Products', 'vw_ProductSupplierMatch'),
            ('Suppliers', 'vw_ProductSupplierMatch'),
            ('vw_ProductSupplierMatch', 'Table_SupplyChainStaging'),
            ('Table_SupplyChainStaging', '##TempIntegrityBuffer'),
            ('##TempIntegrityBuffer', 'Final_SupplyChainAudit')
        ]
    },
    {
        'script': scenario_join_5,
        'lineage': [
            ('Employees', 'vw_EmployeeHierarchy'), # Manager Role
            ('Employees', 'vw_EmployeeHierarchy'), # Subordinate Role
            ('vw_EmployeeHierarchy', 'Table_SupervisionStaging'),
            ('Table_SupervisionStaging', '##TempSupervisionBuffer'),
            ('##TempSupervisionBuffer', 'Final_SupervisionAudit')
        ]
    },
    ],
    # 'deduplication' : [ {'script' : scenario_dedup_1}, 
    #                     {'script' : scenario_dedup_2},
    #                     {'script' : scenario_dedup_3},
    #                     {'script' : scenario_dedup_4},
    #                     {'script' : scenario_dedup_5}],
    # 'derived' : [       {'script' : scenario_derived_1}, 
    #                     {'script' : scenario_derived_2},
    #                     {'script' : scenario_derived_3},
    #                     {'script' : scenario_derived_4},
    #                     {'script' : scenario_derived_5}],
    # 'materialized' : [  {'script' : scenario_materialized_1}, 
    #                     {'script' : scenario_materialized_2},
    #                     {'script' : scenario_materialized_3},
    #                     {'script' : scenario_materialized_4},
    #                     {'script' : scenario_materialized_5}],
    # 'partitioning' : [  {'script' : scenario_partitioning_1}, 
    #                     {'script' : scenario_partitioning_2},
    #                     {'script' : scenario_partitioning_3},
    #                     {'script' : scenario_partitioning_4},
    #                     {'script' : scenario_partitioning_5}],
    # 'tabular' : [       {'script' : scenario_tabular_1}, 
    #                     {'script' : scenario_tabular_2},
    #                     {'script' : scenario_tabular_3},
    #                     {'script' : scenario_tabular_4},
    #                     {'script' : scenario_tabular_5}],
    # 'temporary' : [     {'script' : scenario_temporary_1}, 
    #                     {'script' : scenario_temporary_2},
    #                     {'script' : scenario_temporary_3},
    #                     {'script' : scenario_temporary_4},
    #                     {'script' : scenario_temporary_5}],
    # 'recursive' : [     {'script' : scenario_recursive_1}, 
    #                     {'script' : scenario_recursive_2},
    #                     {'script' : scenario_recursive_3},
    #                     {'script' : scenario_recursive_4},
    #                     {'script' : scenario_recursive_5}],
    'linear_sel_tsf' : [
        {'script' : scenario_select_linear_1} ,
        {'script' : scenario_select_linear_2} ,
        {'script' : scenario_select_linear_3} ,
        {'script' : scenario_select_linear_4} ,
        {'script' : scenario_select_linear_5} ,
        {'script' : scenario_select_linear_6} ,
        {'script' : scenario_select_linear_7} ,
        {'script' : scenario_select_linear_8} ,
        {'script' : scenario_select_linear_9} ,
        {'script' : scenario_select_linear_10} ,
        {'script' : scenario_select_linear_11} ,
        {'script' : scenario_select_linear_12} ,
        {'script' : scenario_select_linear_13} ,
        {'script' : scenario_select_linear_14} ,
        {'script' : scenario_select_linear_15} ,
        {'script' : scenario_select_linear_16} ,
        {'script' : scenario_select_linear_17} ,
        {'script' : scenario_select_linear_18} ,
        {'script' : scenario_select_linear_19} ,
        {'script' : scenario_select_linear_20} 
    ],
    'notrans_sel_tsf' : [
        {'script' : scenario_select_notrans_1} ,
        {'script' : scenario_select_notrans_2} ,
        {'script' : scenario_select_notrans_3} ,
        {'script' : scenario_select_notrans_4} ,
        {'script' : scenario_select_notrans_5} ,
        {'script' : scenario_select_notrans_6} ,
        {'script' : scenario_select_notrans_7} ,
        {'script' : scenario_select_notrans_8} ,
        {'script' : scenario_select_notrans_9} ,
        {'script' : scenario_select_notrans_10} ,
        {'script' : scenario_select_notrans_11} ,
        {'script' : scenario_select_notrans_12} ,
        {'script' : scenario_select_notrans_13} ,
        {'script' : scenario_select_notrans_14} ,
        {'script' : scenario_select_notrans_15} ,
        {'script' : scenario_select_notrans_16} ,
        {'script' : scenario_select_notrans_17} ,
        {'script' : scenario_select_notrans_18} ,
        {'script' : scenario_select_notrans_19} ,
        {'script' : scenario_select_notrans_20} 
    ],
    'nonlinear_sel_tsf' : [
        {'script' : scenario_select_nonlin_1} ,
        {'script' : scenario_select_nonlin_2} ,
        {'script' : scenario_select_nonlin_3} ,
        {'script' : scenario_select_nonlin_4} ,
        {'script' : scenario_select_nonlin_5} ,
        {'script' : scenario_select_nonlin_6} ,
        {'script' : scenario_select_nonlin_7} ,
        {'script' : scenario_select_nonlin_8} ,
        {'script' : scenario_select_nonlin_9} ,
        {'script' : scenario_select_nonlin_10} ,
        {'script' : scenario_select_nonlin_11} ,
        {'script' : scenario_select_nonlin_12} ,
        {'script' : scenario_select_nonlin_13} ,
        {'script' : scenario_select_nonlin_14} ,
        {'script' : scenario_select_nonlin_15} ,
        {'script' : scenario_select_nonlin_16} ,
        {'script' : scenario_select_nonlin_17} ,
        {'script' : scenario_select_nonlin_18} ,
        {'script' : scenario_select_nonlin_19} ,
        {'script' : scenario_select_nonlin_20} 
    ],
    
    'linear_uni_tsf' : [
        {'script' : scenario_uni_linear_1} ,
        {'script' : scenario_uni_linear_2} ,
        {'script' : scenario_uni_linear_3} ,
        {'script' : scenario_uni_linear_4} ,
        {'script' : scenario_uni_linear_5} ,
        {'script' : scenario_uni_linear_6} ,
        {'script' : scenario_uni_linear_7} ,
        {'script' : scenario_uni_linear_8} ,
        {'script' : scenario_uni_linear_9} ,
        {'script' : scenario_uni_linear_10} ,
        {'script' : scenario_uni_linear_11} ,
        {'script' : scenario_uni_linear_12} ,
        {'script' : scenario_uni_linear_13} ,
        {'script' : scenario_uni_linear_14} ,
        {'script' : scenario_uni_linear_15} ,
        {'script' : scenario_uni_linear_16} ,
        {'script' : scenario_uni_linear_17} ,
        {'script' : scenario_uni_linear_18} ,
        {'script' : scenario_uni_linear_19} ,
        {'script' : scenario_uni_linear_20} 
    ],
    'notrans_uni_tsf' : [
        {'script' : scenario_uni_notrans_1} ,
        {'script' : scenario_uni_notrans_2} ,
        {'script' : scenario_uni_notrans_3} ,
        {'script' : scenario_uni_notrans_4} ,
        {'script' : scenario_uni_notrans_5} ,
        {'script' : scenario_uni_notrans_6} ,
        {'script' : scenario_uni_notrans_7} ,
        {'script' : scenario_uni_notrans_8} ,
        {'script' : scenario_uni_notrans_9} ,
        {'script' : scenario_uni_notrans_10} ,
        {'script' : scenario_uni_notrans_11} ,
        {'script' : scenario_uni_notrans_12} ,
        {'script' : scenario_uni_notrans_13} ,
        {'script' : scenario_uni_notrans_14} ,
        {'script' : scenario_uni_notrans_15} ,
        {'script' : scenario_uni_notrans_16} ,
        {'script' : scenario_uni_notrans_17} ,
        {'script' : scenario_uni_notrans_18} ,
        {'script' : scenario_uni_notrans_19} ,
        {'script' : scenario_uni_notrans_20} 
    ],
    'nonlinear_uni_tsf' : [
        {'script' : scenario_uni_nonlin_1} ,
        {'script' : scenario_uni_nonlin_2} ,
        {'script' : scenario_uni_nonlin_3} ,
        {'script' : scenario_uni_nonlin_4} ,
        {'script' : scenario_uni_nonlin_5} ,
        {'script' : scenario_uni_nonlin_6} ,
        {'script' : scenario_uni_nonlin_7} ,
        {'script' : scenario_uni_nonlin_8} ,
        {'script' : scenario_uni_nonlin_9} ,
        {'script' : scenario_uni_nonlin_10} ,
        {'script' : scenario_uni_nonlin_11} ,
        {'script' : scenario_uni_nonlin_12} ,
        {'script' : scenario_uni_nonlin_13} ,
        {'script' : scenario_uni_nonlin_14} ,
        {'script' : scenario_uni_nonlin_15} ,
        {'script' : scenario_uni_nonlin_16} ,
        {'script' : scenario_uni_nonlin_17} ,
        {'script' : scenario_uni_nonlin_18} ,
        {'script' : scenario_uni_nonlin_19} ,
        {'script' : scenario_uni_nonlin_20} 
    ], 
    'linear_join_tsf' : [
        {'script' : scenario_join_linear_1} ,
        {'script' : scenario_join_linear_2} ,
        {'script' : scenario_join_linear_3} ,
        {'script' : scenario_join_linear_4} ,
        {'script' : scenario_join_linear_5} ,
        {'script' : scenario_join_linear_6} ,
        {'script' : scenario_join_linear_7} ,
        {'script' : scenario_join_linear_8} ,
        {'script' : scenario_join_linear_9} ,
        {'script' : scenario_join_linear_10} ,
        {'script' : scenario_join_linear_11} ,
        {'script' : scenario_join_linear_12} ,
        {'script' : scenario_join_linear_13} ,
        {'script' : scenario_join_linear_14} ,
        {'script' : scenario_join_linear_15} ,
        {'script' : scenario_join_linear_16} ,
        {'script' : scenario_join_linear_17} ,
        {'script' : scenario_join_linear_18} ,
        {'script' : scenario_join_linear_19} ,
        {'script' : scenario_join_linear_20} 
    ],
    'notrans_join_tsf' : [
        {'script' : scenario_join_notrans_1} ,
        {'script' : scenario_join_notrans_2} ,
        {'script' : scenario_join_notrans_3} ,
        {'script' : scenario_join_notrans_4} ,
        {'script' : scenario_join_notrans_5} ,
        {'script' : scenario_join_notrans_6} ,
        {'script' : scenario_join_notrans_7} ,
        {'script' : scenario_join_notrans_8} ,
        {'script' : scenario_join_notrans_9} ,
        {'script' : scenario_join_notrans_10} ,
        {'script' : scenario_join_notrans_11} ,
        {'script' : scenario_join_notrans_12} ,
        {'script' : scenario_join_notrans_13} ,
        {'script' : scenario_join_notrans_14} ,
        {'script' : scenario_join_notrans_15} ,
        {'script' : scenario_join_notrans_16} ,
        {'script' : scenario_join_notrans_17} ,
        {'script' : scenario_join_notrans_18} ,
        {'script' : scenario_join_notrans_19} ,
        {'script' : scenario_join_notrans_20} 
    ],
    'nonlinear_join_tsf' : [
        {'script' : scenario_join_nonlin_1} ,
        {'script' : scenario_join_nonlin_2} ,
        {'script' : scenario_join_nonlin_3} ,
        {'script' : scenario_join_nonlin_4} ,
        {'script' : scenario_join_nonlin_5} ,
        {'script' : scenario_join_nonlin_6} ,
        {'script' : scenario_join_nonlin_7} ,
        {'script' : scenario_join_nonlin_8} ,
        {'script' : scenario_join_nonlin_9} ,
        {'script' : scenario_join_nonlin_10} ,
        {'script' : scenario_join_nonlin_11} ,
        {'script' : scenario_join_nonlin_12} ,
        {'script' : scenario_join_nonlin_13} ,
        {'script' : scenario_join_nonlin_14} ,
        {'script' : scenario_join_nonlin_15} ,
        {'script' : scenario_join_nonlin_16} ,
        {'script' : scenario_join_nonlin_17} ,
        {'script' : scenario_join_nonlin_18} ,
        {'script' : scenario_join_nonlin_19} ,
        {'script' : scenario_join_nonlin_20} 
    ]
}

