from pyspark import pipelines as dp
from pyspark.sql import functions as F
import json

# Load active silver layer configurations etc
silver_configs = spark.table("ctrl_dev.metadata.silver_config").filter("is_active = true").collect()

def create_silver_pipeline(config):
    """
    foo
    Dynamically create silver layer datasets based on metadata configuration.
    Supports column mappings, transformations, and Auto CDC.
    """
    # Extracts configuration
    source_table = config["source_bronze_table"]
    target_table = config["target_silver_table"]
    staging_name = config["staging_table_name"]
    cdc_keys = config["cdc_keys"] 
    scd_type = config["scd_type"]
    sequence_col = config["sequence_by_column"]
    
    # Parse JSON configurations
    column_mappings = json.loads(config["column_mappings"]) if config["column_mappings"] else {}
    transformations = json.loads(config["transformations"]) if config["transformations"] else {}
    
    # Create staging table with transformations--
    @dp.table(
        name=staging_name,
        comment=f"Staging table for {target_table}"
    )
    def staging_table():
        # Read from bronze with streaming
        df = spark.readStream.option("ignoreDeletes", "true").table(source_table)
        
        # Apply column renames first
        for old_col, new_col in column_mappings.items():
            df = df.withColumnRenamed(old_col, new_col)
        
        # Apply transformations (computed columns)
        # These transformations can reference renamed columns
        for new_col, expr_str in transformations.items():
            df = df.withColumn(new_col, F.expr(expr_str))
        
        # Drop original BDATE column if birthday was created
        if "birthday" in transformations and "BDATE" in [c.upper() for c in df.columns]:
            df = df.drop("BDATE")
        
        # Add processing timestamp for CDC sequencing
        df = df.withColumn(sequence_col, F.current_timestamp())
        
        return df
    
    # Create target streaming table
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
    
    # Create Auto CDC flow
    dp.create_auto_cdc_flow(
        target=target_table,
        source=staging_name,
        keys=cdc_keys,
        sequence_by=sequence_col,
        stored_as_scd_type=scd_type,
        except_column_list=[],
    )

# Generate silver layer for all active configurations
for config in silver_configs:
    create_silver_pipeline(config)
