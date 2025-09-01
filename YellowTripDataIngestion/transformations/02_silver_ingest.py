import dlt
import geopandas as gpd
from utilities.silver_functions import *
from pyspark.sql import DataFrame

# Pre-load valid location IDs outside DLT function
taxi_zones = gpd.read_file("/Volumes/tlc/tripdata/shapefiles/taxi_zones.shp")
valid_ids = taxi_zones['LocationID'].tolist()
location_dict = dict(zip(taxi_zones['LocationID'], taxi_zones['zone']))

@dlt.table(
  comment="Clean and validated trip data.",
  table_properties={"quality": "silver"},
)
def tripdata_silver() -> DataFrame:

  # Read raw data
  dataset = dlt.read("tripdata_raw")
  
  # Apply transformations
  dataset = fill_missing_values(dataset)
  dataset = cast_columns(dataset)
  dataset = add_trip_time(dataset)
  dataset = apply_consistency_filters(dataset, valid_ids)
  dataset = validate_financials(dataset, tolerance_pct=0.03)
  dataset = drop_duplicate_trips(dataset)
  dataset = dataset.withColumn(
    "avg_speed", 
    spark_round(
      when(
        col("trip_time_minutes") != 0, 
        col("trip_distance") / (col("trip_time_minutes")/60)
        ).otherwise(None), 2)
    ) 
  
  # Final column select
  dataset = dataset.select(
    "VendorID", "PUlocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "DOlocationID", "trip_distance", "trip_time_minutes", "avg_speed", "passenger_count",
    "RatecodeID", "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
    "airport_fee", "total_amount", "_ingestion_time", "_source_file"
  )

  # Data description
  dataset = map_ratecode_description(dataset)
  dataset = map_payment_type_description(dataset)
  dataset = map_vendor_id_description(dataset)
  dataset = map_location_to_zone(dataset, "PUlocationID", location_dict)
  dataset = map_location_to_zone(dataset, "DOlocationID", location_dict)
  return dataset
