# 🥇 Gold Layer Documentation

## Overview

The Gold layer is the **business-ready** layer of the Medallion architecture. It contains aggregated, enriched, and denormalized datasets optimized for analytics and reporting.

**Key Characteristics:**
- **Metadata-driven** - Configure via `ctrl_dev.metadata.gold_config` ✨
- **Materialized Views** for batch processing
- **Business logic** applied (joins, aggregations, calculations)
- **Denormalized** for query performance
- **SCD-aware** reads current records from silver tables
- **Data quality** enforced via expectations
- **Fully integrated** with bronze and silver layers

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Metadata-Driven)              │
│                  (Business-Ready Analytics)                  │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Metadata Table: ctrl_dev.metadata.gold_config               │
│  ├─ Stores: joins, transformations, expectations            │
│  └─ Code: gold_layer.py reads and executes configs          │
│                                                               │
│  gold_dim_customers (Materialized View)                      │
│  ├─ Joins: CRM + ERP customers                              │
│  ├─ Logic: Gender enrichment, name formatting               │
│  └─ Quality: Drop NULL customer_id                          │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ spark.read.table()
                              │ (batch read, current records only)
                              │
┌─────────────────────────────────────────────────────────────┐
│                 SILVER LAYER (Metadata-Driven)               │
│                  (Cleaned & Validated)                       │
├─────────────────────────────────────────────────────────────┤
│  Metadata Table: ctrl_dev.metadata.silver_config             │
│  ├─ crm_customer (SCD Type 2)                               │
│  ├─ erp_customers (SCD Type 2)                              │
│  └─ crm_sale (SCD Type 1)                                   │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│                 BRONZE LAYER (Metadata-Driven)               │
│                     (Raw Ingestion)                          │
├─────────────────────────────────────────────────────────────┤
│  Metadata Table: ctrl_dev.metadata.pipeline_config           │
│  ├─ crm_customers                                           │
│  ├─ crm_sales                                               │
│  └─ crm_products                                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 Metadata Table Structure

### `ctrl_dev.metadata.gold_config`

| Column | Type | Description |
|--------|------|-------------|
| config_id | BIGINT | Auto-generated unique identifier |
| target_gold_table | STRING | Gold table name (without catalog/schema) |
| table_type | STRING | MATERIALIZED_VIEW or STREAMING_TABLE |
| description | STRING | Business description |
| source_tables | STRING | JSON array: ["table1", "table2"] |
| join_config | STRING | JSON: {"type": "left", "left_table": "t1", "right_table": "t2", "condition": "t1.key = t2.key"} |
| column_selections | STRING | JSON: {"output_col": "source_expr"} |
| filters | STRING | JSON array: ["__END_AT IS NULL", "status = 'active'"] |
| data_quality_checks | STRING | JSON array: [{"name": "check1", "condition": "col IS NOT NULL", "action": "DROP"}] |
| is_active | BOOLEAN | Whether configuration is active |
| created_at | TIMESTAMP | Configuration creation timestamp |

---

## 📄 Current Gold Tables

### gold_dim_customers

**Type:** Materialized View  
**Location:** `data_warehouse_pipeline.impodance.gold_dim_customers`  
**Description:** Business-ready customer dimension enriched with ERP data  
**Rows:** 18,484 (1 dropped)

#### Configuration (config_id = 1)

```json
{
  "source_tables": [
    "data_warehouse_tables.silver.crm_customer",
    "data_warehouse_tables.silver.erp_customers"
  ],
  "join_config": {
    "type": "left",
    "left_table": "crm",
    "right_table": "erp_cust",
    "condition": "erp_cust.cid == crm.cst_key"
  },
  "column_selections": {
    "customer_id": "cst_id",
    "customer_number": "cst_key",
    "first_name": "cst_firstname",
    "last_name": "cst_lastname",
    "marital_status": "cst_marital_status",
    "gender": "CASE WHEN cst_gndr != 'n/a' THEN cst_gndr ELSE COALESCE(gender, 'n/a') END",
    "birth_date": "birthday",
    "created_date": "cst_create_date"
  },
  "filters": ["__END_AT IS NULL"],
  "data_quality_checks": [
    {"name": "valid_customer_id", "condition": "customer_id IS NOT NULL", "action": "DROP"}
  ]
}
```

#### Schema

| Column | Type | Description |
|--------|------|-------------|
| customer_id | INTEGER | Primary customer identifier (from CRM) |
| customer_number | STRING | Customer reference key (joins to ERP) |
| first_name | STRING | Customer first name |
| last_name | STRING | Customer last name |
| marital_status | STRING | Marital status |
| gender | STRING | Gender (CRM preferred, ERP fallback) |
| birth_date | DATE | Date of birth (from ERP) |
| created_date | DATE | Account creation date |
| gold_processed_timestamp | TIMESTAMP | Gold layer processing timestamp |

---

## 🔧 How to Add New Gold Tables

### Step 1: Register Configuration in Metadata

```python
import json

# Define configuration
config_data = [(
    "gold_fact_sales",  # target_gold_table
    "MATERIALIZED_VIEW",  # table_type
    "Sales fact table with customer dimensions",  # description
    json.dumps([
        "data_warehouse_tables.silver.crm_sale",
        "data_warehouse_pipeline.impodance.gold_dim_customers"
    ]),  # source_tables
    json.dumps({
        "type": "inner",
        "left_table": "sales",
        "right_table": "customers",
        "condition": "sales.sls_cust_id == customers.customer_id"
    }),  # join_config
    json.dumps({
        "sale_id": "sls_ord_num",
        "customer_id": "customer_id",
        "customer_name": "CONCAT(first_name, ' ', last_name)",
        "order_date": "sls_order_dt",
        "amount": "sls_sales",
        "quantity": "sls_quantity"
    }),  # column_selections
    json.dumps([]),  # filters (empty for this example)
    json.dumps([
        {"name": "valid_sale_id", "condition": "sale_id IS NOT NULL", "action": "DROP"}
    ]),  # data_quality_checks
    True  # is_active
)]

# Insert configuration
columns = [
    "target_gold_table", "table_type", "description", "source_tables",
    "join_config", "column_selections", "filters", "data_quality_checks", "is_active"
]

config_df = spark.createDataFrame(config_data, columns)
config_df.write.mode("append").saveAsTable("ctrl_dev.metadata.gold_config")
```

### Step 2: Run Pipeline

```python
# Validate syntax
startPipelineDryRun()

# Create the table
startPipelineUpdate()
```

### Step 3: Verify Results

```sql
-- Check table was created
SELECT * FROM data_warehouse_pipeline.impodance.gold_fact_sales LIMIT 10;

-- Verify record count
SELECT COUNT(*) FROM data_warehouse_pipeline.impodance.gold_fact_sales;
```

---

## 📈 Configuration Patterns

### Pattern 1: Simple Single-Table Gold View

```python
# Basic aggregation from one silver table
config = {
    "target_gold_table": "gold_sales_summary",
    "table_type": "MATERIALIZED_VIEW",
    "description": "Daily sales summary",
    "source_tables": json.dumps(["data_warehouse_tables.silver.crm_sale"]),
    "join_config": json.dumps({}),  # No joins
    "column_selections": json.dumps({
        "order_date": "sls_order_dt",
        "total_sales": "sls_sales",
        "quantity": "sls_quantity"
    }),
    "filters": json.dumps(["sls_order_dt >= '2024-01-01'"]),
    "data_quality_checks": json.dumps([])
}
```

### Pattern 2: Two-Table Join (Dimension Enrichment)

```python
# Join fact with dimension
config = {
    "target_gold_table": "gold_enriched_sales",
    "source_tables": json.dumps([
        "data_warehouse_tables.silver.crm_sale",
        "gold_dim_customers"
    ]),
    "join_config": json.dumps({
        "type": "inner",
        "left_table": "sales",
        "right_table": "cust",
        "condition": "sales.sls_cust_id == cust.customer_id"
    }),
    "column_selections": json.dumps({
        "sale_id": "sls_ord_num",
        "customer_name": "CONCAT(first_name, ' ', last_name)",
        "amount": "sls_sales"
    }),
    "filters": json.dumps(["__END_AT IS NULL"])
}
```

### Pattern 3: SCD Type 2 Current Records Only

```python
# Filter for current records from SCD Type 2 tables
config = {
    "target_gold_table": "gold_current_customers",
    "source_tables": json.dumps(["data_warehouse_tables.silver.crm_customer"]),
    "filters": json.dumps(["__END_AT IS NULL"]),  # Current records only
    "column_selections": json.dumps({
        "customer_id": "cst_id",
        "full_name": "CONCAT(cst_firstname, ' ', cst_lastname)"
    })
}
```

---

## 📚 JSON Configuration Reference

### source_tables (JSON Array)
```json
["table1", "table2", "table3"]
```
- First table is the primary/left table
- Additional tables are joined in order

### join_config (JSON Object)
```json
{
  "type": "inner",           // inner, left, right, outer
  "left_table": "alias1",    // Alias for left table
  "right_table": "alias2",   // Alias for right table
  "condition": "alias1.key == alias2.key"  // Join condition (PySpark syntax)
}
```

### column_selections (JSON Object)
```json
{
  "output_col1": "source_col1",                    // Simple column
  "output_col2": "UPPER(source_col2)",            // Expression
  "output_col3": "CASE WHEN ... THEN ... END",    // Complex logic
  "output_col4": "CONCAT(col1, ' ', col2)"        // Multi-column
}
```

### filters (JSON Array)
```json
[
  "__END_AT IS NULL",              // SCD Type 2 current records
  "order_date >= '2024-01-01'",    // Date filter
  "status = 'active'"              // Status filter
]
```

### data_quality_checks (JSON Array)
```json
[
  {
    "name": "check_name",
    "condition": "column IS NOT NULL",
    "action": "DROP"    // DROP, FAIL, or WARN
  }
]
```

---

## 🐛 Troubleshooting

### Issue: Join condition not working
**Solution:** Use PySpark expression syntax with `==` (not SQL `=`)
```json
// ✅ CORRECT
"condition": "t1.customer_id == t2.customer_id"

// ❌ WRONG
"condition": "t1.customer_id = t2.customer_id"
```

### Issue: Column not found after join
**Solution:** Use table aliases in column selections
```json
"column_selections": {
  "customer_name": "cust.first_name",  // Use alias prefix
  "sale_amount": "sales.sls_sales"
}
```

### Issue: Duplicate records in gold
**Solution:** Ensure SCD Type 2 tables are filtered with `__END_AT IS NULL`
```json
"filters": ["__END_AT IS NULL"]
```

### Issue: Expression syntax error
**Solution:** Use SQL expression syntax in column_selections (not Python)
```json
// ✅ CORRECT
"full_name": "CONCAT(first_name, ' ', last_name)"

// ❌ WRONG
"full_name": "first_name + ' ' + last_name"
```

---

## 📋 Best Practices

### 1. **Always Filter SCD Type 2 Tables**
```json
"filters": ["__END_AT IS NULL"]  // Get current records only
```

### 2. **Use Descriptive Table Aliases**
```json
"join_config": {
  "left_table": "sales",      // ✅ Clear
  "right_table": "customers"  // ✅ Clear
}
// Better than "t1", "t2"
```

### 3. **Add Data Quality Checks**
```json
"data_quality_checks": [
  {"name": "valid_id", "condition": "id IS NOT NULL", "action": "DROP"}
]
```

### 4. **Document Business Logic**
```json
"description": "Customer dimension joining CRM and ERP data with gender enrichment logic"
```

### 5. **Test with is_active = false First**
```python
# Set is_active = false initially
# Test with dry run
# Enable when validated
```

---

## 🎯 Example Workflows

### Add Customer Dimension
```python
import json

config = [(
    "gold_dim_customers",
    "MATERIALIZED_VIEW",
    "Customer dimension with ERP enrichment",
    json.dumps([
        "data_warehouse_tables.silver.crm_customer",
        "data_warehouse_tables.silver.erp_customers"
    ]),
    json.dumps({
        "type": "left",
        "left_table": "crm",
        "right_table": "erp",
        "condition": "erp.cid == crm.cst_key"
    }),
    json.dumps({
        "customer_id": "cst_id",
        "customer_number": "cst_key",
        "first_name": "cst_firstname",
        "last_name": "cst_lastname",
        "gender": "CASE WHEN cst_gndr != 'n/a' THEN cst_gndr ELSE COALESCE(gender, 'n/a') END",
        "birth_date": "birthday"
    }),
    json.dumps(["__END_AT IS NULL"]),
    json.dumps([{"name": "valid_id", "condition": "customer_id IS NOT NULL", "action": "DROP"}]),
    True
)]

# Insert and run pipeline
```

---

## 📞 Related Documentation

- [INDEX.md](INDEX.md) - Navigation hub
- [README_SILVER_LAYER.md](README_SILVER_LAYER.md) - Silver layer documentation
- [QUICKSTART_GOLD.md](QUICKSTART_GOLD.md) - Quick reference guide
- [SQL_TEMPLATES_GOLD.sql](SQL_TEMPLATES_GOLD.sql) - Copy-paste SQL templates

---

## 🚀 What Makes This Metadata-Driven?

Unlike traditional gold layers where each table requires separate Python/SQL code, this implementation:

✅ **Single code file** (`gold_layer.py`) handles all gold tables  
✅ **Add tables via INSERT** - no code changes needed  
✅ **Consistent patterns** across all gold tables  
✅ **Easy to maintain** - update metadata, not code  
✅ **Self-documenting** - metadata serves as documentation  
✅ **Version controlled** - metadata changes are tracked  
✅ **Searchable** - query metadata to find tables  

**Complete Metadata-Driven Pipeline:**
- ✅ Bronze Layer: Metadata-driven via `pipeline_config`
- ✅ Silver Layer: Metadata-driven via `silver_config`
- ✅ Gold Layer: Metadata-driven via `gold_config`

---

**Last Updated:** 2026-04-27  
**Pipeline ID:** f77c85e4-2cb1-4a93-9b3b-4e9104d5a9c8  
**Catalog:** data_warehouse_pipeline  
**Schema:** impodance  
**Architecture:** Fully Metadata-Driven Medallion (Bronze → Silver → Gold)
