from itertools import chain
from pyspark.sql import DataFrame
from typing import Optional, List, Dict
from pyspark.sql.functions import col, unix_timestamp, to_date, lit, round as spark_round, abs as spark_abs, when, create_map

def cast_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("VendorID", col("VendorID").cast("int"))\
        .withColumn("PUlocationID", col("PUlocationID").cast("int"))\
        .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))\
        .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))\
        .withColumn("DOlocationID", col("DOlocationID").cast("int"))\
        .withColumn("payment_type", col("payment_type").cast("int"))\
        .withColumn("passenger_count", col("passenger_count").cast("int"))\
        .withColumn("trip_distance", col("trip_distance").cast("double"))\
        .withColumn("RatecodeID", col("RatecodeID").cast("int"))\
        .withColumn("fare_amount", col("fare_amount").cast("double"))\
        .withColumn("extra", col("extra").cast("double"))\
        .withColumn("mta_tax", col("mta_tax").cast("double"))\
        .withColumn("tip_amount", col("tip_amount").cast("double"))\
        .withColumn("tolls_amount", col("tolls_amount").cast("double"))\
        .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double"))\
        .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double"))\
        .withColumn("airport_fee", col("airport_fee").cast("double"))\
        .withColumn("total_amount", col("total_amount").cast("double"))

def fill_missing_values(df: DataFrame) -> DataFrame:
    return df.fillna({
        "RatecodeID": 99,
        "PUlocationID": 999,
        "DOlocationID": 999,
        "payment_type": 5,
        "fare_amount": 0,
        "tolls_amount": 0,
        "tip_amount": 0,
        "extra": 0,
        "mta_tax": 0,
        "airport_fee": 0,
        "improvement_surcharge": 0,
        "congestion_surcharge": 0,
        "total_amount": 0,
    })

def add_trip_time(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "trip_time_minutes",
        spark_round((unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60).cast("int")
    )

def apply_consistency_filters(df: DataFrame, valid_ids: List[int]) -> DataFrame:
    return df.filter(col("tpep_pickup_datetime") >= to_date(lit("2023-01-01")))\
        .filter(col("tpep_dropoff_datetime") >= to_date(lit("2023-01-01")))\
        .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))\
        .filter((col("passenger_count") > 0) & (col("passenger_count") < 7))\
        .filter(col("trip_distance") > 0)\
        .filter(col("RatecodeID").isin([1, 2, 3, 4, 5, 6, 99]))\
        .filter(col("payment_type").isin([0, 1, 2, 3, 4, 5, 6]))\
        .filter(col("VendorID").isin([1, 2, 6, 7]))\
        .filter((col("PUlocationID").isin(valid_ids)) & (col("DOlocationID").isin(valid_ids)))

def validate_financials(df: DataFrame, tolerance_pct: Optional[float] = 0.03) -> DataFrame:
    df = df.withColumn(
        "total_amount_calc",
        col("fare_amount") + col("tolls_amount") + col("tip_amount") + col("extra") +
        col("mta_tax") + col("airport_fee") + col("improvement_surcharge") + col("congestion_surcharge"))
    df = df.withColumn(
        "financial_consistent",
        when(col("total_amount") == 0, lit(0))\
        .when((spark_abs(col("total_amount_calc") - col("total_amount")) / col("total_amount")) <= tolerance_pct, lit(1))\
        .otherwise(lit(0)))
    return df.filter(col("financial_consistent") == 1).drop(*["total_amount_calc", "financial_consistent"])

def drop_duplicate_trips(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance"])

def map_ratecode_description(df: DataFrame) -> DataFrame:   
    return df.withColumn(
        "RatecodeID",
        when(col("RatecodeID") == 1, "Standard rate")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau or Westchester")
        .when(col("RatecodeID") == 5, "Negotiated fare")
        .when(col("RatecodeID") == 6, "Group ride")
        .when(col("RatecodeID") == 99, "Null/Unknown")
    )

def map_payment_type_description(df: DataFrame) -> DataFrame:    
    return df.withColumn(
        "payment_type",
        when(col("payment_type") == 0, "Flex Fare trip")
        .when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 5, "Unknown")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Other")
    )

def map_vendor_id_description(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "VendorID",
        when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
        .when(col("VendorID") == 2, "Curb Mobility, LLC")
        .when(col("VendorID") == 6, "Myle Technologies Inc")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Other")
    )

def map_location_to_zone(df: DataFrame, location_col: str, location_dict:Dict[int, str]) -> DataFrame:
    mapping_expr = create_map([lit(x) for x in chain(*location_dict.items())])
    return df.withColumn(location_col, mapping_expr[col(location_col)])