from pyspark import pipelines as dp
from pyspark.sql import functions as F

# 1. METADATA-DRIVEN: Load only active tables
configs = spark.table("ctrl_dev.metadata.pipeline_config").filter("is_active = true").collect()

def create_pipeline(row):
    # Variable names
    T_NAME = row["table_name"]
    S_PATH = row["source_path"]
    S_LOC  = row["schema_location"]
    D_QUALITY =row["data_quality_rules"]
    FORMAT = row["file_format"]
    DW_WAREHOUSE =row["dwh_file"]

    # --- PARTITION LOGIC ---
    # We read the string from SQL. If it's empty/None, we use an empty list [].
    raw_partitions = row.asDict().get("partition_cols")
    PARTITION_LIST = [p.strip() for p in raw_partitions.split(",")] if raw_partitions else []
    
    @dp.table(name=f"{DW_WAREHOUSE}.bronze.{T_NAME}",
              partition_cols=PARTITION_LIST
              )
    # 2. OBSERVABLE: Expectations track data quality automatically
    @dp.expect_or_drop("valid_id",f"{D_QUALITY} IS NOT NULL") 
    def bronze_layer():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", FORMAT)
            .option("cloudFiles.schemaLocation", S_LOC)
            .option("cloudFiles.schemaEvolutionMode", "rescue")  # NEW: Use rescued data column
            .option("rescuedDataColumn", "_rescued_data")        # NEW: Explicit column name
            .load(S_PATH)
            .withColumn("ingest_timestamp", F.current_timestamp())
            .withColumn("source_metadata", F.col("_metadata"))
            .withColumn("_metadata_time", F.col("_metadata.file_modification_time"))
        )

for row in configs:
    create_pipeline(row)
