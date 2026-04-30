from pyspark import pipelines as dp
from pyspark.sql import functions as F
import json

# Load active gold layer configurations
gold_configs = spark.table("ctrl_dev.metadata.gold_config").filter("is_active = true").collect()

def create_gold_table(config):
    """
    Dynamically create gold layer datasets based on metadata configuration.
    Supports joins, column selections, filters, and data quality checks.
    """
    # Extracts configuration
    target_table = config["target_gold_table"]
    table_type = config["table_type"]
    description = config["description"]
    
    # Parse JSON configurations
    source_tables = json.loads(config["source_tables"]) if config["source_tables"] else []
    join_config = json.loads(config["join_config"]) if config["join_config"] else {}
    column_selections = json.loads(config["column_selections"]) if config["column_selections"] else {}
    filters = json.loads(config["filters"]) if config["filters"] else []
    data_quality_checks = json.loads(config["data_quality_checks"]) if config["data_quality_checks"] else []
    
    # Build the dataset function
    if table_type == "MATERIALIZED_VIEW":
        @dp.materialized_view(
            name=target_table,
            comment=description
        )
        def gold_table():
            # Read source tables
            if len(source_tables) == 0:
                raise ValueError(f"No source tables defined for {target_table}")
            
            # Read first table (primary)
            primary_table = source_tables[0]
            df = spark.read.table(primary_table)
            
            # Apply filters to primary table
            for filter_expr in filters:
                df = df.filter(filter_expr)
            
            # Perform joins if multiple sources
            if len(source_tables) > 1 and join_config:
                join_type = join_config.get("type", "inner")
                
                # Read secondary table
                secondary_table = source_tables[1]
                df_secondary = spark.read.table(secondary_table)
                
                # Apply filters to secondary table
                for filter_expr in filters:
                    try:
                        df_secondary = df_secondary.filter(filter_expr)
                    except:
                        pass  # Skip if filter doesn't apply to this table
                
                # Drop common metadata columns from right table to avoid duplicates
                metadata_cols = ['_rescued_data', 'ingest_timestamp', 'source_metadata', 'silver_processed_timestamp']
                cols_to_drop = [col for col in metadata_cols if col in df_secondary.columns]
                if cols_to_drop:
                    df_secondary = df_secondary.drop(*cols_to_drop)
                
                # Get join condition
                condition = join_config.get("condition", "")
                if condition:
                    # Create aliases for tables
                    left_alias = join_config.get("left_table", "t1")
                    right_alias = join_config.get("right_table", "t2")
                    
                    df_left = df.alias(left_alias)
                    df_right = df_secondary.alias(right_alias)
                    
                    # Perform join - use SQL-style condition
                    df = df_left.join(df_right, F.expr(condition), join_type)
            
            # Apply column selections and transformations
            if column_selections:
                for output_col, expr_str in column_selections.items():
                    # Check if it's a simple column reference or expression
                    if expr_str in df.columns:
                        df = df.withColumn(output_col, F.col(expr_str))
                    else:
                        # It's an expression
                        df = df.withColumn(output_col, F.expr(expr_str))
            
            # Add gold processing timestamp
            df = df.withColumn("gold_processed_timestamp", F.current_timestamp())
            
            # Select only the output columns
            if column_selections:
                output_columns = list(column_selections.keys()) + ["gold_processed_timestamp"]
                df = df.select(*output_columns)
            
            return df
        
        # Apply data quality expectations
        if data_quality_checks:
            for check in data_quality_checks:
                check_name = check.get("name", "quality_check")
                condition = check.get("condition", "")
                action = check.get("action", "DROP").upper()
                
                if action == "DROP":
                    gold_table = dp.expect_or_drop(check_name, condition)(gold_table)
                elif action == "FAIL":
                    gold_table = dp.expect_or_fail(check_name, condition)(gold_table)
                else:
                    gold_table = dp.expect(check_name, condition)(gold_table)
        
        return gold_table
    else:
        raise ValueError(f"Unsupported table type: {table_type}")

# Create all active gold tables
for config_row in gold_configs:
    create_gold_table(config_row)
