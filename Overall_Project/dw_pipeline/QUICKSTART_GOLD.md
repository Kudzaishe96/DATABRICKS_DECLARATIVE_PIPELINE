# ⚡ Gold Layer Quick Reference

## Common Tasks

### View Active Gold Tables
```sql
SELECT config_id, target_gold_table, description, is_active
FROM ctrl_dev.metadata.gold_config
WHERE is_active = true
ORDER BY target_gold_table;
```

### Check Gold Table Data Quality
```sql
-- View expectations and dropped records
DESCRIBE DETAIL data_warehouse_pipeline.impodance.gold_dim_customers;
```

### Query Current Customers
```sql
SELECT *
FROM data_warehouse_pipeline.impodance.gold_dim_customers
LIMIT 100;
```

---

## Adding a New Gold Table

### 1. Register Metadata
```sql
INSERT INTO ctrl_dev.metadata.gold_config (
    target_gold_table, description, source_tables, is_active
) VALUES (
    'gold_fact_sales',
    'Sales fact table',
    '["data_warehouse_tables.silver.crm_sale"]',
    true
);
```

### 2. Add Code to gold_layer.py
```python
@dp.materialized_view(name="gold_fact_sales")
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
def gold_fact_sales():
    return spark.read.table("data_warehouse_tables.silver.crm_sale")
```

### 3. Run Pipeline
```python
startPipelineDryRun()    # Validate syntax
startPipelineUpdate()    # Create table
```

---

## Common Patterns

### Reading SCD Type 2 Current Records
```python
df = (spark.read.table("silver_table")
      .filter(F.col("__END_AT").isNull()))
```

### Enrichment with Left Join
```python
df = primary.join(supplementary, "key", "left")
```

### Conditional Logic
```python
df = df.withColumn("field",
    F.when(F.col("source1").isNotNull(), F.col("source1"))
     .otherwise(F.coalesce(F.col("source2"), F.lit("default"))))
```

### Add Timestamp
```python
df = df.withColumn("processed_at", F.current_timestamp())
```

---

## Validation Commands

```bash
# View pipeline datasets
readPipelineDatasets()

# Check specific table details
readPipelineDatasetDetails("gold_dim_customers")

# View pipeline issues
readPipelineIssues()
```

---

## Important Notes

⚠️ **Gold uses batch reads** - Use `spark.read` not `spark.readStream`  
⚠️ **Filter SCD Type 2** - Always add `WHERE __END_AT IS NULL`  
⚠️ **Use expect_or_drop** - Avoid `expect_or_fail` in gold layer  
✅ **Document metadata** - Register all gold tables in `gold_config`  
✅ **Add timestamps** - Include `gold_processed_timestamp`
