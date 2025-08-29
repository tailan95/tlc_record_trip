import dlt
from utilities.golden_functions import *
from pyspark.sql import DataFrame


@dlt.table(
    comment="Daily trip metrics",
    table_properties={"quality": "gold"}
)
def gold_aggregated_values() -> DataFrame:
    df_silver = dlt.read("tripdata_silver")
    return aggregated_values(df_silver, time_stamp="day", location_col=["PUlocationID"])


@dlt.table(
    comment="Daily bucket trip metrics by pickup location.",
    table_properties={"quality": "gold"}
)
def gold_bucket_metrics() -> DataFrame:
    df_silver = dlt.read("tripdata_silver")
    return aggregate_values_by_bucket(df_silver, time_stamp="day", location_col=["PUlocationID"])


@dlt.table(
    comment="Daily bucket trip metrics by pickup location.",
    table_properties={"quality": "gold"}
)
def gold_passengers_metrics() -> DataFrame:
    df_silver = dlt.read("tripdata_silver")
    return aggregate_values_by_passengers(df_silver, time_stamp="day", location_col=["PUlocationID"])

