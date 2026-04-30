-- =====================================================
-- SQL Templates for Silver Layer Configuration
-- =====================================================
-- Copy and modify these templates to add new silver tables
-- File: SQL_TEMPLATES_SILVER.sql
-- =====================================================

-- -----------------------------------------------------
-- TEMPLATE 1: Basic Configuration
-- -----------------------------------------------------
-- Use this when you want a simple silver table with minimal transformations

INSERT INTO ctrl_dev.metadata.silver_config 
(
    source_bronze_table, 
    target_silver_table, 
    staging_table_name, 
    cdc_keys, 
    scd_type, 
    sequence_by_column, 
    column_mappings, 
    transformations, 
    is_active
)
VALUES 
(
    'data_warehouse_tables.bronze.<TABLE_NAME>',
    'data_warehouse_tables.silver.<TABLE_NAME>',
    '<table_name>_silver_staging',
    array('<primary_key>'),
    1,  -- SCD Type 1: Latest only
    'silver_processed_timestamp',
    '{}',  -- No column renames
    '{}',  -- No transformations
    true
);


-- -----------------------------------------------------
-- TEMPLATE 2: With Column Renames
-- -----------------------------------------------------
-- Use this when you need to standardize column names

INSERT INTO ctrl_dev.metadata.silver_config 
(
    source_bronze_table, 
    target_silver_table, 
    staging_table_name, 
    cdc_keys, 
    scd_type, 
    sequence_by_column, 
    column_mappings, 
    transformations, 
    is_active
)
VALUES 
(
    'data_warehouse_tables.bronze.<TABLE_NAME>',
    'data_warehouse_tables.silver.<TABLE_NAME>',
    '<table_name>_silver_staging',
    array('<primary_key>'),
    1,
    'silver_processed_timestamp',
    '{
        "old_column_1": "new_column_1",
        "old_column_2": "new_column_2",
        "old_column_3": "new_column_3"
    }',
    '{}',
    true
);


-- -----------------------------------------------------
-- TEMPLATE 3: With Transformations
-- -----------------------------------------------------
-- Use this when you need computed columns

INSERT INTO ctrl_dev.metadata.silver_config 
(
    source_bronze_table, 
    target_silver_table, 
    staging_table_name, 
    cdc_keys, 
    scd_type, 
    sequence_by_column, 
    column_mappings, 
    transformations, 
    is_active
)
VALUES 
(
    'data_warehouse_tables.bronze.<TABLE_NAME>',
    'data_warehouse_tables.silver.<TABLE_NAME>',
    '<table_name>_silver_staging',
    array('<primary_key>'),
    1,
    'silver_processed_timestamp',
    '{}',
    '{
        "computed_col_1": "column_a * column_b",
        "computed_col_2": "concat(first_name, '' '', last_name)",
        "computed_col_3": "CASE WHEN status = ''active'' THEN true ELSE false END"
    }',
    true
);


-- -----------------------------------------------------
-- TEMPLATE 4: SCD Type 2 (With History)
-- -----------------------------------------------------
-- Use this when you need to track historical changes

INSERT INTO ctrl_dev.metadata.silver_config 
(
    source_bronze_table, 
    target_silver_table, 
    staging_table_name, 
    cdc_keys, 
    scd_type, 
    sequence_by_column, 
    column_mappings, 
    transformations, 
    is_active
)
VALUES 
(
    'data_warehouse_tables.bronze.<TABLE_NAME>',
    'data_warehouse_tables.silver.<TABLE_NAME>',
    '<table_name>_silver_staging',
    array('<primary_key>'),
    2,  -- SCD Type 2: Track history
    'silver_processed_timestamp',
    '{
        "old_column_1": "new_column_1"
    }',
    '{
        "computed_col_1": "expression_here"
    }',
    true
);


-- -----------------------------------------------------
-- TEMPLATE 5: Composite Keys
-- -----------------------------------------------------
-- Use this when multiple columns form the primary key

INSERT INTO ctrl_dev.metadata.silver_config 
(
    source_bronze_table, 
    target_silver_table, 
    staging_table_name, 
    cdc_keys, 
    scd_type, 
    sequence_by_column, 
    column_mappings, 
    transformations, 
    is_active
)
VALUES 
(
    'data_warehouse_tables.bronze.<TABLE_NAME>',
    'data_warehouse_tables.silver.<TABLE_NAME>',
    '<table_name>_silver_staging',
    array('<key_column_1>', '<key_column_2>', '<key_column_3>'),  -- Composite key
    1,
    'silver_processed_timestamp',
    '{}',
    '{}',
    true
);


-- -----------------------------------------------------
-- TEMPLATE 6: Full Example (CRM Customers)
-- -----------------------------------------------------
-- Complete example with renames and transformations

INSERT INTO ctrl_dev.metadata.silver_config 
(
    source_bronze_table, 
    target_silver_table, 
    staging_table_name, 
    cdc_keys, 
    scd_type, 
    sequence_by_column, 
    column_mappings, 
    transformations, 
    is_active
)
VALUES 
(
    'data_warehouse_tables.bronze.crm_customer',
    'data_warehouse_tables.silver.crm_customers',
    'crm_customers_silver_staging',
    array('cst_id'),
    1,
    'silver_processed_timestamp',
    '{
        "cst_id": "customer_id",
        "cst_firstname": "first_name",
        "cst_lastname": "last_name",
        "cst_gndr": "gender",
        "cst_marital_status": "marital_status",
        "cst_create_date": "created_date"
    }',
    '{
        "full_name": "concat(first_name, '' '', last_name)",
        "is_married": "CASE WHEN marital_status = ''Married'' THEN true ELSE false END",
        "customer_age_years": "year(current_date()) - year(created_date)"
    }',
    true
);


-- =====================================================
-- MANAGEMENT QUERIES
-- =====================================================

-- View all configurations
SELECT 
    config_id,
    source_bronze_table,
    target_silver_table,
    staging_table_name,
    scd_type,
    is_active,
    created_date
FROM ctrl_dev.metadata.silver_config
ORDER BY config_id;


-- View active configurations only
SELECT 
    source_bronze_table,
    target_silver_table,
    scd_type
FROM ctrl_dev.metadata.silver_config
WHERE is_active = true;


-- Disable a silver table
UPDATE ctrl_dev.metadata.silver_config
SET 
    is_active = false,
    updated_date = current_timestamp()
WHERE staging_table_name = '<table_name>_silver_staging';


-- Enable a silver table
UPDATE ctrl_dev.metadata.silver_config
SET 
    is_active = true,
    updated_date = current_timestamp()
WHERE staging_table_name = '<table_name>_silver_staging';


-- Update transformations
UPDATE ctrl_dev.metadata.silver_config
SET 
    transformations = '{
        "new_computed_col": "new_expression"
    }',
    updated_date = current_timestamp()
WHERE staging_table_name = '<table_name>_silver_staging';


-- Update column mappings
UPDATE ctrl_dev.metadata.silver_config
SET 
    column_mappings = '{
        "old_col": "new_col"
    }',
    updated_date = current_timestamp()
WHERE staging_table_name = '<table_name>_silver_staging';


-- Delete a configuration (use with caution!)
DELETE FROM ctrl_dev.metadata.silver_config
WHERE staging_table_name = '<table_name>_silver_staging';


-- =====================================================
-- TRANSFORMATION EXPRESSION EXAMPLES
-- =====================================================

/*
String Operations:
  "concat(col1, ' ', col2)"
  "upper(column_name)"
  "lower(column_name)"
  "trim(column_name)"
  "substring(column_name, 1, 10)"
  "replace(column_name, 'old', 'new')"

Date/Time Operations:
  "to_date(timestamp_column)"
  "year(date_column)"
  "month(date_column)"
  "day(date_column)"
  "date_format(date_column, 'yyyy-MM-dd')"
  "datediff(current_date(), date_column)"

Numeric Operations:
  "column_a + column_b"
  "column_a * column_b"
  "round(column_a, 2)"
  "cast(column_a as double)"

Conditional Logic:
  "CASE WHEN condition THEN value1 ELSE value2 END"
  "CASE WHEN col > 100 THEN 'high' WHEN col > 50 THEN 'medium' ELSE 'low' END"
  "IF(condition, true_value, false_value)"

Boolean Conversions:
  "CASE WHEN status = 'active' THEN true ELSE false END"
  "col IS NOT NULL"
  "col = 'value'"

Null Handling:
  "coalesce(col1, col2, 'default')"
  "nvl(column, 'default_value')"
  "CASE WHEN col IS NULL THEN 'missing' ELSE col END"
*/


-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

-- Check if source bronze table exists
SHOW TABLES IN data_warehouse_tables.bronze;

-- Check bronze table schema
DESCRIBE data_warehouse_tables.bronze.<table_name>;

-- Sample bronze data
SELECT * FROM data_warehouse_tables.bronze.<table_name> LIMIT 5;

-- Check if staging table was created (after pipeline run)
SHOW TABLES IN data_warehouse_pipeline.impodance LIKE '%_silver_staging';

-- Check staging table data
SELECT * FROM data_warehouse_pipeline.impodance.<table_name>_silver_staging LIMIT 5;

-- Check silver table was created
SHOW TABLES IN data_warehouse_tables.silver;

-- Check silver table data
SELECT * FROM data_warehouse_tables.silver.<table_name> LIMIT 5;

-- Check SCD Type 2 history (if applicable)
SELECT 
    *,
    __START_AT as valid_from,
    __END_AT as valid_to,
    CASE WHEN __END_AT IS NULL THEN true ELSE false END as is_current
FROM data_warehouse_tables.silver.<table_name>
ORDER BY <primary_key>, __START_AT;


-- =====================================================
-- End of Templates
-- =====================================================
