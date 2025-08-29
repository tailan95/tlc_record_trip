import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, input_file_name, current_timestamp

@dlt.table(
  comment="Raw tripdata with ingestion metadata.",
  table_properties={"delta.feature.timestampNtz": "supported"}
)
def tripdata_raw() -> DataFrame:
  return (
    spark.readStream.format("cloudFiles") 
      .option("cloudFiles.format", "parquet") # ~ Enable Databricks Auto Loader
      .option("cloudFiles.schemaLocation", "/Volumes/tlc/tripdata/checkpoints/tripdata_bronze") # ~ Store schema evolution history
      .load("/Volumes/tlc/tripdata/tripdata_files/") # ~ Source folder containing raw Parquet files
      .withColumn("_ingestion_time", current_timestamp()) # ~ Add ingestion timestamp
      .withColumn("_source_file", col("_metadata.file_path")) # ~ Add source file path for traceability
  )