# Metadata-Driven Silver Layer Pipeline

## 📖 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Metadata Table Structure](#metadata-table-structure)
- [How It Works](#how-it-works)
- [Setup Instructions](#setup-instructions)
- [Adding New Silver Tables](#adding-new-silver-tables)
- [Configuration Examples](#configuration-examples)
- [Code Walkthrough](#code-walkthrough)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## 📚 Overview

This metadata-driven silver layer solution enables you to create and manage silver tables dynamically without writing repetitive pipeline code. By storing transformation logic in a metadata table, you can:

- **Add new silver tables** by inserting rows into a metadata table (no code changes)
- **Apply transformations** like column renames, computed columns, and data quality rules
- **Enable Auto CDC** with SCD Type 1 or Type 2 automatically
- **Maintain consistency** across all silver layer transformations

**Key Components:**
1. **Metadata Table**: `ctrl_dev.metadata.silver_config` - stores transformation configurations
2. **Pipeline Code**: `transformations/silver_layer.py` - reads metadata and generates datasets
3. **Bronze Layer**: Source tables from `bronze_layer.py`

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Bronze Layer                              │
│  (Auto Loader ingestion from raw files)                     │
│  Tables: bronze.erp_customer, bronze.crm_customer, etc.     │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ Reads from
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              ctrl_dev.metadata.silver_config                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ source_bronze_table: "bronze.erp_customer"           │  │
│  │ target_silver_table: "silver.erp_customers"          │  │
│  │ cdc_keys: ["cid"]                                    │  │
│  │ column_mappings: {"CID": "cid", "GEN": "gender"}     │  │
│  │ transformations: {"birthday": "BDATE", ...}          │  │
│  │ scd_type: 2                                          │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ Configures
                      ▼
┌─────────────────────────────────────────────────────────────┐
│          transformations/silver_layer.py                     │
│  - Reads active configs                                      │
│  - Creates staging tables with transformations               │
│  - Applies Auto CDC (SCD Type 1 or 2)                       │
│  - Writes to silver layer                                    │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ Outputs to
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    Silver Layer                              │
│  - Staging tables: {table_name}_silver_staging               │
│  - Target tables: silver.erp_customers (with CDC enabled)   │
│  - Cleaned, transformed, and historized data                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 🗄️ Metadata Table Structure

### Table: `ctrl_dev.metadata.silver_config`

 Column | Type | Description |
--------|------|-------------|
 `config_id` | BIGINT | Auto-generated unique identifier |
 `source_bronze_table` | STRING | Fully qualified bronze table name (e.g., `data_warehouse_tables.bronze.erp_customer`) |
 `target_silver_table` | STRING | Fully qualified silver table name (e.g., `data_warehouse_tables.silver.erp_customers`) |
 `staging_table_name` | STRING | Name for intermediate staging table (e.g., `erp_customers_silver_staging`) |
 `cdc_keys` | ARRAY<STRING> | Primary key columns for CDC (e.g., `["cid"]` or `["customer_id", "order_id"]`) |
 `scd_type` | INT | Slowly Changing Dimension type: `1` (upsert only) or `2` (with history tracking) |
 `sequence_by_column` | STRING | Column used to sequence CDC operations (e.g., `silver_processed_timestamp`) |
 `column_mappings` | STRING | JSON string for column renames: `{"old_name": "new_name"}` |
 `transformations` | STRING | JSON string for computed columns: `{"new_col": "SQL expression"}` |
 `data_quality_checks` | STRING | JSON string for expectations (future use) |
 `is_active` | BOOLEAN | Enable/disable this configuration (default: `true`) |
 `layer` | STRING | Layer identifier (default: `silver`) |
 `created_date` | TIMESTAMP | Record creation timestamp |
 `updated_date` | TIMESTAMP | Last update timestamp |

---

## ⚙️ How It Works

### Step-by-Step Process:

1. **Load Metadata**
   ```python
   silver_configs = spark.table("ctrl_dev.metadata.silver_config").filter("is_active = true").collect()
   ```
   - Reads all active silver configurations from the metadata table

2. **For Each Configuration:**
   - **Create Staging Table** (`@dp.table`)
     - Reads from bronze table with streaming
     - Applies column renames (e.g., `CID` → `cid`)
     - Applies transformations (e.g., computed columns like `cid_no`)
     - Adds processing timestamp for CDC sequencing
   
   - **Create Target Streaming Table** (`dp.create_streaming_table`)
     - Creates the silver table with Delta table properties
     - Enables Change Data Feed for downstream consumers
     - Optimizes writes and enables auto-compaction
   
   - **Create Auto CDC Flow** (`dp.create_auto_cdc_flow`)
     - Reads from staging table
     - Applies upserts based on `cdc_keys`
     - Sequences operations using `sequence_by_column`
     - Implements SCD Type 1 (overwrite) or Type 2 (historize)

3. **Output**
   - Staging table: `{staging_table_name}` (e.g., `erp_customers_silver_staging`)
   - Silver table: `{target_silver_table}` (e.g., `data_warehouse_tables.silver.erp_customers`)

---

## 🚀 Setup Instructions

### Step 1: Create the Metadata Table

```sql
CREATE TABLE IF NOT EXISTS ctrl_dev.metadata.silver_config (
    config_id BIGINT GENERATED ALWAYS AS IDENTITY,
    source_bronze_table STRING,
    target_silver_table STRING,
    staging_table_name STRING,
    cdc_keys ARRAY<STRING>,
    scd_type INT DEFAULT 1,
    sequence_by_column STRING,
    column_mappings STRING,
    transformations STRING,
    data_quality_checks STRING,
    is_active BOOLEAN DEFAULT true,
    layer STRING DEFAULT 'silver',
    created_date TIMESTAMP DEFAULT current_timestamp(),
    updated_date TIMESTAMP DEFAULT current_timestamp()
)
TBLPROPERTIES(
    'delta.feature.allowColumnDefaults' = 'supported',
    'description' = 'Metadata configuration for silver layer transformations'
);
```

### Step 2: Verify Bronze Tables Exist

Ensure your bronze tables are created and populated by `bronze_layer.py`:
```sql
SHOW TABLES IN data_warehouse_tables.bronze;
```

### Step 3: Deploy Silver Layer Code

The file `transformations/silver_layer.py` is already included in your pipeline.

### Step 4: Insert Configuration

Add your first silver table configuration (see examples below).

### Step 5: Run the Pipeline

```python
# Dry run to validate
startPipelineDryRun()

# Full run to process data
startPipelineUpdate()
```

---

## ➕ Adding New Silver Tables

### Template Query

```sql
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
    '<source_bronze_table>',           -- e.g., 'data_warehouse_tables.bronze.crm_customer'
    '<target_silver_table>',           -- e.g., 'data_warehouse_tables.silver.crm_customers'
    '<staging_table_name>',            -- e.g., 'crm_customers_silver_staging'
    array('<key1>', '<key2>'),         -- e.g., array('customer_id')
    <scd_type>,                        -- 1 or 2
    '<sequence_by_column>',            -- e.g., 'silver_processed_timestamp'
    '<column_mappings_json>',          -- e.g., '{"cst_id": "customer_id", "cst_firstname": "first_name"}'
    '<transformations_json>',          -- e.g., '{"full_name": "concat(first_name, '' '', last_name)"}'
    true                               -- is_active
);
```

### No Code Changes Needed!

Once you insert a new row, the pipeline automatically:
- Creates the staging table
- Applies your transformations
- Sets up Auto CDC
- Writes to the target silver table

Just run the pipeline update and your new silver table is live!

---

## 📋 Configuration Examples

### Example 1: ERP Customers (SCD Type 2 - with history)

```sql
INSERT INTO ctrl_dev.metadata.silver_config 
(source_bronze_table, target_silver_table, staging_table_name, cdc_keys, scd_type, sequence_by_column, column_mappings, transformations, is_active)
VALUES 
(
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

**What it does:**
- Renames `CID` → `cid`, `GEN` → `gender`
- Creates `birthday` column from `BDATE`
- Extracts `cid_no` from position 4 onwards
- Tracks historical changes (SCD Type 2)

---

### Example 2: CRM Customers (SCD Type 1 - latest only)

```sql
INSERT INTO ctrl_dev.metadata.silver_config 
(source_bronze_table, target_silver_table, staging_table_name, cdc_keys, scd_type, sequence_by_column, column_mappings, transformations, is_active)
VALUES 
(
    'data_warehouse_tables.bronze.crm_customer',
    'data_warehouse_tables.silver.crm_customers',
    'crm_customers_silver_staging',
    array('cst_id'),
    1,  -- SCD Type 1: Latest only (no history)
    'silver_processed_timestamp',
    '{"cst_id": "customer_id", "cst_firstname": "first_name", "cst_lastname": "last_name", "cst_gndr": "gender", "cst_marital_status": "marital_status"}',
    '{"full_name": "concat(first_name, '' '', last_name)", "is_married": "CASE WHEN marital_status = ''Married'' THEN true ELSE false END"}',
    true
);
```

**What it does:**
- Renames multiple columns (cst_id → customer_id, etc.)
- Creates `full_name` by concatenating first and last name
- Creates `is_married` boolean flag
- Keeps only latest record (SCD Type 1)

---

### Example 3: Sales Data (Composite Keys)

```sql
INSERT INTO ctrl_dev.metadata.silver_config 
(source_bronze_table, target_silver_table, staging_table_name, cdc_keys, scd_type, sequence_by_column, column_mappings, transformations, is_active)
VALUES 
(
    'data_warehouse_tables.bronze.crm_sales',
    'data_warehouse_tables.silver.crm_sales',
    'crm_sales_silver_staging',
    array('order_id', 'line_item_id'),  -- Composite key
    1,
    'silver_processed_timestamp',
    '{"sls_order_id": "order_id", "sls_line_item_id": "line_item_id", "sls_amount": "amount"}',
    '{"revenue": "amount * 1.0", "order_date": "to_date(sls_create_date)"}',
    true
);
```

**What it does:**
- Uses composite key: `order_id` + `line_item_id`
- Renames sales columns
- Creates computed columns for revenue and date conversion

---

### Example 4: Minimal Configuration (No Transformations)

```sql
INSERT INTO ctrl_dev.metadata.silver_config 
(source_bronze_table, target_silver_table, staging_table_name, cdc_keys, scd_type, sequence_by_column, column_mappings, transformations, is_active)
VALUES 
(
    'data_warehouse_tables.bronze.products',
    'data_warehouse_tables.silver.products',
    'products_silver_staging',
    array('product_id'),
    1,
    'silver_processed_timestamp',
    '{}',  -- No column renames
    '{}',  -- No transformations
    true
);
```

**What it does:**
- Simple pass-through from bronze to silver
- Only adds CDC capability and processing timestamp

---

## 🔍 Code Walkthrough

### File: `transformations/silver_layer.py`

#### Section 1: Load Configurations
```python
silver_configs = spark.table("ctrl_dev.metadata.silver_config").filter("is_active = true").collect()
```
- Reads all active configurations from metadata table
- Returns a list of Row objects

#### Section 2: Pipeline Creation Function
```python
def create_silver_pipeline(config):
```
This function is called once per configuration row.

**Extract Configuration:**
```python
source_table = config["source_bronze_table"]
target_table = config["target_silver_table"]
staging_name = config["staging_table_name"]
cdc_keys = config["cdc_keys"]
scd_type = config["scd_type"]
sequence_col = config["sequence_by_column"]
```

**Parse JSON Strings:**
```python
column_mappings = json.loads(config["column_mappings"]) if config["column_mappings"] else {}
transformations = json.loads(config["transformations"]) if config["transformations"] else {}
```
- Converts JSON strings to Python dictionaries
- Handles empty/null values gracefully

**Create Staging Table:**
```python
@dp.table(name=staging_name, comment=f"Staging table for {target_table}")
def staging_table():
    df = spark.readStream.option("ignoreDeletes", "true").table(source_table)
    
    # Apply column renames
    for old_col, new_col in column_mappings.items():
        df = df.withColumnRenamed(old_col, new_col)
    
    # Apply transformations
    for new_col, expr_str in transformations.items():
        df = df.withColumn(new_col, F.expr(expr_str))
    
    # Drop original BDATE if birthday was created
    if "birthday" in transformations and "BDATE" in [c.upper() for c in df.columns]:
        df = df.drop("BDATE")
    
    # Add CDC sequence timestamp
    df = df.withColumn(sequence_col, F.current_timestamp())
    
    return df
```

**Create Target Table:**
```python
dp.create_streaming_table(
    name=target_table,
    comment=f"Silver layer table with SCD Type {scd_type}",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
```

**Create CDC Flow:**
```python
dp.create_auto_cdc_flow(
    target=target_table,
    source=staging_name,
    keys=cdc_keys,
    sequence_by=F.col(sequence_col),
    stored_as_scd_type=scd_type,
    except_column_list=[],
)
```

#### Section 3: Execute for All Configs
```python
for config in silver_configs:
    create_silver_pipeline(config)
```
- Loops through all active configurations
- Generates silver tables dynamically

---

## ✅ Best Practices

### 1. **Naming Conventions**

**Staging Tables:**
```
{entity}_silver_staging
```
Examples: `erp_customers_silver_staging`, `crm_sales_silver_staging`

**Target Tables:**
```
{catalog}.silver.{entity}
```
Examples: `data_warehouse_tables.silver.erp_customers`, `data_warehouse_tables.silver.crm_sales`

### 2. **CDC Keys**

- Always use the **renamed** column name in `cdc_keys` (after column_mappings are applied)
- For composite keys, use array: `array('key1', 'key2')`
- Keys should be unique and non-null

### 3. **SCD Type Selection**

**Use SCD Type 1 when:**
- You only need the latest/current state
- Historical changes are not important
- Examples: reference data, lookup tables

**Use SCD Type 2 when:**
- You need to track historical changes
- Auditing requirements
- Examples: customer profiles, product pricing

### 4. **Transformations**

**Column Order Matters:**
1. First: Apply `column_mappings` (renames)
2. Then: Apply `transformations` (computed columns)
3. Transformations can reference renamed columns

**Expression Syntax:**
- Use Spark SQL expressions (not Python)
- Example: `"concat(first_name, ' ', last_name)"` ✅
- Not: `"F.concat('first_name', ' ', 'last_name')"` ❌

### 5. **Testing**

Before inserting production configs:
1. Test with `is_active = false` first
2. Run dry run: `startPipelineDryRun()`
3. Check staging table output
4. Set `is_active = true` when validated
5. Run full update: `startPipelineUpdate()`

### 6. **Maintenance**

**Disable a silver table:**
```sql
UPDATE ctrl_dev.metadata.silver_config
SET is_active = false, updated_date = current_timestamp()
WHERE staging_table_name = 'table_name_silver_staging';
```

**Update transformations:**
```sql
UPDATE ctrl_dev.metadata.silver_config
SET transformations = '{"new_col": "new_expression"}',
    updated_date = current_timestamp()
WHERE staging_table_name = 'table_name_silver_staging';
```

### 7. **Performance**

- Keep transformations simple in staging tables
- Complex aggregations → move to gold layer
- Use partitioning for large tables (add `partition_cols` column to metadata if needed)
- Monitor CDC performance with `sequence_by_column` timestamps

---

## 🐛 Troubleshooting

### Issue: "Column not found" error

**Cause:** Transformation references a column before rename is applied.

**Solution:** Ensure transformations use column names **after** `column_mappings` are applied.

Example:
```json
// ❌ Wrong - references old column name
{"column_mappings": {"OLD_NAME": "new_name"}, "transformations": {"computed": "OLD_NAME * 2"}}

// ✅ Correct - references new column name
{"column_mappings": {"OLD_NAME": "new_name"}, "transformations": {"computed": "new_name * 2"}}
```

---

### Issue: CDC flow fails with "duplicate key" error

**Cause:** `cdc_keys` don't uniquely identify records.

**Solution:** 
1. Check if you need a composite key: `array('key1', 'key2')`
2. Add deduplication in staging table if source has duplicates

---

### Issue: Transformation expression syntax error

**Cause:** Invalid Spark SQL expression in `transformations` JSON.

**Solution:**
1. Test expressions separately:
   ```sql
   SELECT <your_expression> FROM <table> LIMIT 1;
   ```
2. Escape single quotes in JSON:
   ```json
   {"col": "CASE WHEN x = ''value'' THEN 1 ELSE 0 END"}
   ```

---

### Issue: Pipeline doesn't create new table after insert

**Cause:** Pipeline code was already loaded before insert.

**Solution:**
1. The pipeline reads metadata at startup
2. After inserting new config, run pipeline update to reload
3. New tables will be created in the next run

---

### Issue: SCD Type 2 creates too many versions

**Cause:** Timestamp precision causes every record to look like a change.

**Solution:**
1. Use a coarser `sequence_by_column` (date instead of timestamp)
2. Or add `except_column_list` to ignore columns that change frequently but don't matter

---

## 📚 Additional Resources

### Related Files
- `transformations/bronze_layer.py` - Bronze ingestion (upstream)
- `transformations/silver_layer_scd1.py` - Original silver code (now replaced by metadata-driven approach)
- `ctrl_dev.metadata.pipeline_config` - Bronze layer metadata table

### Key Concepts
- **Auto CDC**: Automatic Change Data Capture for upserts and deletes
- **SCD Type 1**: Slowly Changing Dimension - overwrite (latest only)
- **SCD Type 2**: Slowly Changing Dimension - historize (track changes)
- **Staging Tables**: Intermediate tables for transformations before CDC

### Pipeline Settings
- Catalog: `data_warehouse_pipeline`
- Schema: `impodance`
- Root Path: `/Workspace/Users/kudzaishemanyanya1@gmail.com/dw_pipeline`

---

## 📞 Support

For issues or questions:
1. Check pipeline issues: `readPipelineIssues()`
2. Review event logs in pipeline monitoring page
3. Validate metadata table configurations
4. Test transformations independently before adding to metadata

---

**Last Updated:** April 24, 2026  
**Pipeline ID:** f77c85e4-2cb1-4a93-9b3b-4e9104d5a9c8  
**Version:** 1.0
