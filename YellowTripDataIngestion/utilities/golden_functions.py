from pyspark.sql import DataFrame
from typing import Optional, List
from pyspark.sql.functions import col, weekofyear, year, month, dayofweek, hour, count, sum as spark_sum, avg, when, round as spark_round

def transform_month(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "pickup_month",
        when(col("pickup_month") == 1, "Jan")
        .when(col("pickup_month") == 2, "Feb")
        .when(col("pickup_month") == 3, "Mar")
        .when(col("pickup_month") == 4, "Apr")
        .when(col("pickup_month") == 5, "May")
        .when(col("pickup_month") == 6, "Jun")
        .when(col("pickup_month") == 7, "Jul")
        .when(col("pickup_month") == 8, "Aug")
        .when(col("pickup_month") == 9, "Sep")
        .when(col("pickup_month") == 10, "Oct")
        .when(col("pickup_month") == 11, "Nov")
        .when(col("pickup_month") == 12, "Dec")
        .otherwise("N/A")
    )

def transform_day(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "pickup_day",
        when(col("pickup_day") == 1, "Sunday")
        .when(col("pickup_day") == 2, "Monday")
        .when(col("pickup_day") == 3, "Tuesday")
        .when(col("pickup_day") == 4, "Wednesday")
        .when(col("pickup_day") == 5, "Thursday")
        .when(col("pickup_day") == 6, "Friday")
        .when(col("pickup_day") == 7, "Saturday")
        .otherwise("N/A")
    )

def aggregate_values_by_stamp(df: DataFrame, time_stamp:Optional[str]="week", location_col:Optional[list]=[]) -> DataFrame:
    from pyspark.sql.functions import dayofweek, weekofyear, month, year, col, count, avg, sum as spark_sum, round as spark_round, when

    # Define timestamp function
    if time_stamp == "day":
        stamp_func = dayofweek
    elif time_stamp == "week":
        stamp_func = weekofyear
    elif time_stamp == "month":
        stamp_func = month
    else:
        raise ValueError(f"Invalid stamp: {time_stamp}")

    # Adiciona colunas de ano e mês
    df = df.withColumn("pickup_year", year(col("tpep_pickup_datetime")))\
           .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

    # Adiciona coluna de período somente se não for month
    if time_stamp != "month":
        df = df.withColumn(f"pickup_{time_stamp}", stamp_func(col("tpep_pickup_datetime")))

    # Define colunas de agrupamento
    group_cols = [*location_col, "pickup_year", "pickup_month"]
    if time_stamp != "month":
        group_cols.append(f"pickup_{time_stamp}")

    # Agregação
    agg_df = df.groupBy(*[col(c) for c in group_cols]).agg(
        count("*").alias("total_trips"),
        spark_sum("total_amount").alias("total_revenue"),
        spark_sum("fare_amount").alias("total_fare"),
        spark_sum("tip_amount").alias("total_tips"),
        spark_sum("tolls_amount").alias("total_tolls"),
        avg("trip_distance").alias("avg_distance"),
        avg("trip_time_minutes").alias("avg_trip_time")
    )

    # Calcula tip percentage
    agg_df = agg_df.withColumn(
        "tip_percentage",
        when(col("total_revenue") != 0, 100 * col("total_tips") / col("total_revenue")).otherwise(0)
    )

    # Arredonda colunas numéricas
    numeric_cols = ["total_revenue","total_fare","total_tips","total_tolls","avg_distance","avg_trip_time","tip_percentage"]
    for c in numeric_cols:
        agg_df = agg_df.withColumn(c, spark_round(col(c),2))
    agg_df = transform_month(agg_df)
    if time_stamp == "day":
        agg_df = transform_day(agg_df)
    return agg_df

def aggregate_values_by_bucket(df: DataFrame, time_stamp:Optional[str]="week", location_col:Optional[list]=[]) -> DataFrame:
    from pyspark.sql.functions import dayofweek, weekofyear, month, year, col, count, avg, sum as spark_sum, round as spark_round, when

    # Cria buckets de distância
    df = df.withColumn(
        "distance_bucket", 
        when(col("trip_distance") <= 2, "0-2 miles")
        .when(col("trip_distance") <= 5, "2-5 miles")
        .when(col("trip_distance") <= 10, "5-10 miles")
        .when(col("trip_distance") <= 25, "10-25 miles")
        .when(col("trip_distance") <= 50, "25-50 miles")
        .when(col("trip_distance") <= 100, "50-100 miles")
        .otherwise(">100 miles")
    )

    # Define função de timestamp
    if time_stamp == "day":
        stamp_func = dayofweek
    elif time_stamp == "week":
        stamp_func = weekofyear
    elif time_stamp == "month":
        stamp_func = month
    else:
        raise ValueError(f"Invalid stamp: {time_stamp}")

    # Adiciona colunas de ano e mês
    df = df.withColumn("pickup_year", year(col("tpep_pickup_datetime")))\
           .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

    # Adiciona coluna de período somente se não for month
    if time_stamp != "month":
        df = df.withColumn(f"pickup_{time_stamp}", stamp_func(col("tpep_pickup_datetime")))

    # Colunas de agrupamento
    group_cols = [*location_col, "distance_bucket", "pickup_year", "pickup_month"]
    if time_stamp != "month":
        group_cols.append(f"pickup_{time_stamp}")

    # Agregação
    agg_df = df.groupBy(*[col(c) for c in group_cols]).agg(
        count("*").alias("total_trips"),
        spark_sum("total_amount").alias("total_revenue"),
        avg("trip_time_minutes").alias("avg_trip_time"),
        avg("tip_amount").alias("avg_tips")
    )

    # Arredonda colunas numéricas
    numeric_cols = ["total_revenue", "avg_trip_time", "avg_tips"]
    for c in numeric_cols:
        agg_df = agg_df.withColumn(c, spark_round(col(c), 2))
    agg_df = transform_month(agg_df)
    if time_stamp == "day":
        agg_df = transform_day(agg_df)
    return agg_df