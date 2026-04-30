-- ============================================================
-- GOLD LAYER SQL TEMPLATES
-- Copy and modify these templates for common gold layer tasks
-- ============================================================

-- ------------------------------------------------------------
-- 1. REGISTER NEW GOLD TABLE METADATA
-- ------------------------------------------------------------

INSERT INTO ctrl_dev.metadata.gold_config (
    target_gold_table,
    table_type,
    description,
    source_tables,
    join_config,
    column_selections,
    filters,
    data_quality_checks,
    is_active
) VALUES (
    'gold_fact_YOUR_TABLE',                              -- Gold table name
    'MATERIALIZED_VIEW',                                  -- Type: MATERIALIZED_VIEW or STREAMING_TABLE
    'Description of your gold table',                     -- Business description
    '["silver_table1", "silver_table2"]',                -- Source tables (JSON array)
    '{"type": "inner", "condition": "t1.key = t2.key"}', -- Join logic (JSON)
    '{"col1": "source_col1", "col2": "UPPER(col2)"}',    -- Column mappings (JSON)
    '["__END_AT IS NULL", "status = active"]',           -- Filters (JSON array)
    '[{"name": "valid_id", "condition": "id IS NOT NULL", "action": "DROP"}]',  -- Expectations
    true                                                  -- Active flag
);

-- ------------------------------------------------------------
-- 2. VIEW ACTIVE GOLD CONFIGURATIONS
-- ------------------------------------------------------------

SELECT 
    config_id,
    target_gold_table,
    table_type,
    description,
    is_active
FROM ctrl_dev.metadata.gold_config
WHERE is_active = true
ORDER BY target_gold_table;

-- ------------------------------------------------------------
-- 3. UPDATE GOLD TABLE METADATA
-- ------------------------------------------------------------

UPDATE ctrl_dev.metadata.gold_config
SET 
    description = 'Updated description',
    source_tables = '["new_source_table"]',
    is_active = true
WHERE config_id = 1;

-- ------------------------------------------------------------
-- 4. DISABLE GOLD TABLE
-- ------------------------------------------------------------

UPDATE ctrl_dev.metadata.gold_config
SET is_active = false
WHERE target_gold_table = 'gold_table_name';

-- ------------------------------------------------------------
-- 5. VIEW GOLD TABLE DETAILS
-- ------------------------------------------------------------

-- Full table details
DESCRIBE EXTENDED data_warehouse_pipeline.impodance.gold_dim_customers;

-- Table properties only
SHOW TBLPROPERTIES data_warehouse_pipeline.impodance.gold_dim_customers;

-- Column details
DESCRIBE TABLE data_warehouse_pipeline.impodance.gold_dim_customers;

-- ------------------------------------------------------------
-- 6. QUERY GOLD TABLES
-- ------------------------------------------------------------

-- Sample data
SELECT *
FROM data_warehouse_pipeline.impodance.gold_dim_customers
LIMIT 10;

-- Count records
SELECT COUNT(*) AS total_records
FROM data_warehouse_pipeline.impodance.gold_dim_customers;

-- Check for nulls
SELECT 
    COUNT(*) AS total,
    COUNT(customer_id) AS non_null_customer_id,
    COUNT(*) - COUNT(customer_id) AS null_customer_id
FROM data_warehouse_pipeline.impodance.gold_dim_customers;

-- ------------------------------------------------------------
-- 7. CUSTOMER DIMENSION QUERIES
-- ------------------------------------------------------------

-- Customer summary by gender
SELECT 
    gender,
    marital_status,
    COUNT(*) AS customer_count
FROM data_warehouse_pipeline.impodance.gold_dim_customers
GROUP BY gender, marital_status
ORDER BY customer_count DESC;

-- Age distribution
SELECT 
    CASE 
        WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365.25 < 25 THEN '<25'
        WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365.25 < 35 THEN '25-34'
        WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365.25 < 45 THEN '35-44'
        WHEN DATEDIFF(CURRENT_DATE(), birth_date) / 365.25 < 55 THEN '45-54'
        ELSE '55+'
    END AS age_group,
    COUNT(*) AS customer_count
FROM data_warehouse_pipeline.impodance.gold_dim_customers
WHERE birth_date IS NOT NULL
GROUP BY age_group
ORDER BY age_group;

-- Recent customers
SELECT 
    customer_id,
    first_name,
    last_name,
    created_date,
    DATEDIFF(CURRENT_DATE(), created_date) AS days_since_created
FROM data_warehouse_pipeline.impodance.gold_dim_customers
WHERE created_date IS NOT NULL
ORDER BY created_date DESC
LIMIT 20;

-- ------------------------------------------------------------
-- 8. DATA QUALITY CHECKS
-- ------------------------------------------------------------

-- Check for duplicate customer IDs
SELECT 
    customer_id,
    COUNT(*) AS count
FROM data_warehouse_pipeline.impodance.gold_dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check for missing critical fields
SELECT 
    'Missing customer_id' AS check_name,
    COUNT(*) AS count
FROM data_warehouse_pipeline.impodance.gold_dim_customers
WHERE customer_id IS NULL

UNION ALL

SELECT 
    'Missing customer_number',
    COUNT(*)
FROM data_warehouse_pipeline.impodance.gold_dim_customers
WHERE customer_number IS NULL

UNION ALL

SELECT 
    'Missing both names',
    COUNT(*)
FROM data_warehouse_pipeline.impodance.gold_dim_customers
WHERE first_name IS NULL AND last_name IS NULL;

-- ------------------------------------------------------------
-- 9. COMPARE SILVER VS GOLD COUNTS
-- ------------------------------------------------------------

-- Verify data flow from silver to gold
SELECT 
    'Silver CRM Customers' AS source,
    COUNT(*) AS record_count
FROM data_warehouse_tables.silver.crm_customer
WHERE __END_AT IS NULL

UNION ALL

SELECT 
    'Gold Dim Customers',
    COUNT(*)
FROM data_warehouse_pipeline.impodance.gold_dim_customers;

-- ------------------------------------------------------------
-- 10. DROP GOLD TABLE (CAUTION!)
-- ------------------------------------------------------------

-- Use with caution - this will permanently delete the table
-- DROP TABLE IF EXISTS data_warehouse_pipeline.impodance.gold_table_name;

-- Also disable in metadata after dropping
-- UPDATE ctrl_dev.metadata.gold_config
-- SET is_active = false
-- WHERE target_gold_table = 'gold_table_name';

-- ------------------------------------------------------------
-- 11. GOLD FACT TABLE TEMPLATE (for future use)
-- ------------------------------------------------------------

/*
-- Example: Sales fact table joining silver sales with gold dimensions

INSERT INTO ctrl_dev.metadata.gold_config (
    target_gold_table,
    table_type,
    description,
    source_tables,
    is_active
) VALUES (
    'gold_fact_sales',
    'MATERIALIZED_VIEW',
    'Sales fact table with customer and product dimensions',
    '["data_warehouse_tables.silver.crm_sale", "gold_dim_customers", "gold_dim_products"]',
    true
);

-- Python code for gold_layer.py:

@dp.materialized_view(
    name="gold_fact_sales",
    comment="Sales fact table with dimensions"
)
@dp.expect_or_drop("valid_sale", "sale_id IS NOT NULL")
def gold_fact_sales():
    sales = spark.read.table("data_warehouse_tables.silver.crm_sale")
    customers = spark.read.table("gold_dim_customers")
    
    df = (
        sales.join(customers, sales.sls_cust_id == customers.customer_id, "inner")
             .select(
                 F.col("sls_ord_num").alias("sale_id"),
                 F.col("customer_id"),
                 F.col("sls_order_dt").alias("order_date"),
                 F.col("sls_sales").alias("amount"),
                 F.col("sls_quantity").alias("quantity"),
                 F.current_timestamp().alias("gold_processed_timestamp")
             )
    )
    return df
*/

-- ============================================================
-- END OF TEMPLATES
-- ============================================================
