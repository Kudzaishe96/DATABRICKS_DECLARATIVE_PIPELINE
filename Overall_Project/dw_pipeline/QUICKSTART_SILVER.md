# Silver Layer Quick Start Guide

## 🚀 Quick Commands

### View Current Configurations
```sql
SELECT 
    source_bronze_table,
    target_silver_table,
    staging_table_name,
    scd_type,
    is_active
FROM ctrl_dev.metadata.silver_config
ORDER BY config_id;
```

### Add New Silver Table (Template)
```sql
INSERT INTO ctrl_dev.metadata.silver_config 
(source_bronze_table, target_silver_table, staging_table_name, cdc_keys, scd_type, sequence_by_column, column_mappings, transformations, is_active)
VALUES 
(
    'data_warehouse_tables.bronze.YOUR_TABLE',
    'data_warehouse_tables.silver.YOUR_TABLE',
    'YOUR_TABLE_silver_staging',
    array('your_key_column'),
    1,  -- 1 = latest only, 2 = with history
    'silver_processed_timestamp',
    '{"old_col_name": "new_col_name"}',
    '{"new_computed_col": "your_expression"}',
    true
);
```

### Disable a Table
```sql
UPDATE ctrl_dev.metadata.silver_config
SET is_active = false
WHERE staging_table_name = 'YOUR_TABLE_silver_staging';
```

### Update Transformations
```sql
UPDATE ctrl_dev.metadata.silver_config
SET transformations = '{"col1": "expression1", "col2": "expression2"}'
WHERE staging_table_name = 'YOUR_TABLE_silver_staging';
```

---

## 📋 Real Examples

### Example 1: Simple Pass-Through
```sql
INSERT INTO ctrl_dev.metadata.silver_config 
VALUES (
    default,  -- config_id (auto-generated)
    'data_warehouse_tables.bronze.products',
    'data_warehouse_tables.silver.products',
    'products_silver_staging',
    array('product_id'),
    1,
    'silver_processed_timestamp',
    '{}',  -- No renames
    '{}',  -- No transformations
    null,  -- No data quality checks yet
    true,
    'silver',
    current_timestamp(),
    current_timestamp()
);
```

### Example 2: With Column Renames
```sql
INSERT INTO ctrl_dev.metadata.silver_config 
(source_bronze_table, target_silver_table, staging_table_name, cdc_keys, scd_type, sequence_by_column, column_mappings, transformations, is_active)
VALUES (
    'data_warehouse_tables.bronze.crm_customer',
    'data_warehouse_tables.silver.crm_customers',
    'crm_customers_silver_staging',
    array('cst_id'),
    1,
    'silver_processed_timestamp',
    '{"cst_id": "customer_id", "cst_firstname": "first_name", "cst_lastname": "last_name"}',
    '{"full_name": "concat(first_name, '' '', last_name)"}',
    true
);
```

### Example 3: SCD Type 2 with History
```sql
INSERT INTO ctrl_dev.metadata.silver_config 
(source_bronze_table, target_silver_table, staging_table_name, cdc_keys, scd_type, sequence_by_column, column_mappings, transformations, is_active)
VALUES (
    'data_warehouse_tables.bronze.erp_customer',
    'data_warehouse_tables.silver.erp_customers',
    'erp_customers_silver_staging',
    array('cid'),
    2,  -- SCD Type 2: Track history
    'silver_processed_timestamp',
    '{"CID": "cid", "GEN": "gender"}',
    '{"birthday": "BDATE", "cid_no": "substring(cid, 4, length(cid))"}',
    true
);
```

---

## 🔧 Common Patterns

### Pattern: String Concatenation
```json
{"transformations": {"full_address": "concat(street, ', ', city, ', ', state)"}}
```

### Pattern: Date Conversion
```json
{"transformations": {"order_date": "to_date(order_timestamp)"}}
```

### Pattern: Conditional Logic
```json
{"transformations": {"is_active": "CASE WHEN status = 'active' THEN true ELSE false END"}}
```

### Pattern: String Extraction
```json
{"transformations": {"area_code": "substring(phone, 1, 3)"}}
```

### Pattern: Numeric Calculation
```json
{"transformations": {"total_price": "quantity * unit_price"}}
```

### Pattern: Age Calculation
```json
{"transformations": {"age_years": "year(current_date()) - year(birth_date)"}}
```

---

## 📊 Metadata Table Schema

 Column | Type | Required | Description |
--------|------|----------|-------------|
 source_bronze_table | STRING | ✅ | Bronze table to read from |
 target_silver_table | STRING | ✅ | Silver table to write to |
 staging_table_name | STRING | ✅ | Staging table name |
 cdc_keys | ARRAY<STRING> | ✅ | Primary keys for CDC |
 scd_type | INT | ✅ | 1 or 2 |
 sequence_by_column | STRING | ✅ | CDC sequence column |
 column_mappings | STRING | ❌ | JSON: `{"old": "new"}` |
 transformations | STRING | ❌ | JSON: `{"col": "expr"}` |
 is_active | BOOLEAN | ✅ | Enable/disable |

---

## ⚡ Workflow

1. **Insert Configuration** (SQL above)
2. **Run Pipeline**
   ```python
   startPipelineDryRun()  # Validate first
   startPipelineUpdate()  # Run full
   ```
3. **Check Results**
   ```sql
   SELECT * FROM data_warehouse_tables.silver.YOUR_TABLE LIMIT 10;
   ```

---

## ❗ Important Notes

1. **Use renamed columns** in transformations
   ```json
   // ❌ Wrong
   {"column_mappings": {"OLD": "new"}, "transformations": {"x": "OLD * 2"}}
   
   // ✅ Correct
   {"column_mappings": {"OLD": "new"}, "transformations": {"x": "new * 2"}}
   ```

2. **Escape quotes in JSON**
   ```json
   {"col": "CASE WHEN x = ''value'' THEN 1 END"}
   ```

3. **CDC keys must be unique**
   - Use composite keys if needed: `array('key1', 'key2')`

4. **Pipeline reload required**
   - After inserting config, run pipeline update to load new table

---

## 📖 Full Documentation

See `README_SILVER_LAYER.md` for complete documentation.
