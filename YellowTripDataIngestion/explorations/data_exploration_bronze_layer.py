# Databricks notebook source
# MAGIC %md
# MAGIC # Exploração de Dados – Camada Bronze
# MAGIC
# MAGIC Este notebook explora os **registros brutos de viagens (Camada Bronze)** antes de avançar para a **Camada Silver**.  
# MAGIC O objetivo é avaliar a **completude, consistência, plausibilidade e duplicação** no conjunto de dados.  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 1 – Carregar Dados da Camada Bronze
# MAGIC A tabela Bronze é carregada em um DataFrame do Spark e os primeiros registros são visualizados.

# COMMAND ----------

from pyspark.sql.functions import col, to_date, lit, month, to_date
raw_data = spark.sql("SELECT * FROM tlc.tripdata.tripdata_raw")
columns = [
      "VendorID", 
      "PUlocationID", 
      "tpep_pickup_datetime", 
      "tpep_dropoff_datetime", 
      "DOlocationID", 
      "passenger_count", 
      "trip_distance", 
      "RatecodeID", 
      "payment_type", 
      "fare_amount", 
      "extra", 
      "mta_tax", 
      "tip_amount", 
      "tolls_amount", 
      "improvement_surcharge", 
      "total_amount", 
      "congestion_surcharge", 
      "airport_fee", 
]
raw_data = raw_data.filter(col("tpep_pickup_datetime") > to_date(lit("2024-01-01")))
raw_data = raw_data.select(*columns)
raw_data.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 2 – Validação do Esquema
# MAGIC Verificar se o esquema corresponde ao **dicionário de dados oficial da TLC**.  
# MAGIC Verificar se os campos de timestamp são corretamente reconhecidos como `timestamp` e se as colunas numéricas possuem os tipos corretos.  

# COMMAND ----------

raw_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 3 – Valores Ausentes
# MAGIC Nesta etapa é contabilizado o número de **valores nulos por coluna**. 

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, to_date, lit
missing_summary = raw_data.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in raw_data.columns])
missing_summary.display()
print(f"The dataset contains {raw_data.count()} values.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 4 – Distribuição Temporal
# MAGIC - Inspecionar os timestamps de embarque para detectar irregularidades.  
# MAGIC
# MAGIC A ideia aqui é verificar as datas de início e fim, pois no problema foi solicitado o uso de dados a partir de janeiro de 2023.  
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import min as spark_min, max as spark_max
raw_data.agg(
    spark_min(col("tpep_pickup_datetime")).alias("initial_pickup_datetime"),
    spark_max(col("tpep_pickup_datetime")).alias("final_pickup_datetime")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 5 – Verificações de Consistência
# MAGIC - O horário de desembarque deve ser **maior** que o horário de embarque.  
# MAGIC - A contagem de passageiros deve estar entre **1 e 7**.  
# MAGIC - A distância da viagem deve ser **positiva**.  
# MAGIC - Campos financeiros como `fare_amount`, `total_amount` e `tips` devem ser **não negativos**.  
# MAGIC - Os IDs de embarque e desembarque (`PUlocationID` e `DOlocationID`) devem existir no shapefile das Zonas de Táxi de NYC.

# COMMAND ----------

lower_values_in = raw_data.filter(col("tpep_pickup_datetime") < to_date(lit("2023-01-01")))
lower_values_off = raw_data.filter(col("tpep_dropoff_datetime") < to_date(lit("2023-01-01")))
dropin_ = raw_data.filter(col("tpep_dropoff_datetime") < col("tpep_pickup_datetime")) 
passenger_ = raw_data.filter((col("passenger_count") <= 0) | (col("passenger_count") > 7)) 
tistances_ = raw_data.filter(col("trip_distance") <= 0) 
fate_ = raw_data.filter(col("fare_amount") < 0)
extra_ = raw_data.filter(col("extra") < 0)
mta_tax_ = raw_data.filter(col("mta_tax") < 0)
mta_tax_ = raw_data.filter(col("mta_tax") < 0)
tips_ = raw_data.filter(col("tip_amount") < 0)
tolls_amount_ = raw_data.filter(col("tolls_amount") < 0)
improvement_surcharge_ = raw_data.filter(col("improvement_surcharge") < 0)
congestion_surcharge_ = raw_data.filter(col("congestion_surcharge") < 0)
airport_fee_ = raw_data.filter(col("airport_fee") < 0)
total_amount_ = raw_data.filter(col("total_amount") < 0)
print(f"Invalid pickup/dropoff before jan-2023: {lower_values_in.count()}/{lower_values_off.count()}")
print(f"Invalid pickup/dropoff times: {dropin_.count()}")
print(f"Invalid passenger count: {passenger_.count()}")
print(f"Invalid trip distances: {tistances_.count()}")
print(f"Invalid fares: {fate_.count()}")
print(f"Invalid extras: {extra_.count()}")
print(f"Invalid mta_tax: {mta_tax_.count()}")
print(f"Invalid tips: {tips_.count()}")
print(f"Invalid tolls_amount: {tolls_amount_.count()}")
print(f"Invalid improvement_surcharge: {improvement_surcharge_.count()}")
print(f"Invalid congestion_surcharge: {congestion_surcharge_.count()}")
print(f"Invalid airport_fee: {airport_fee_.count()}")
print(f"Invalid total_amount: {total_amount_.count()}")


# COMMAND ----------

try:
    import geopandas as gpd
except ImportError:
    %pip install geopandas shapely
    import geopandas as gpd

# COMMAND ----------

# Load NYC Taxi Zones Shapefiles
taxi_zones = gpd.read_file("/Volumes/tlc/tripdata/shapefiles/taxi_zones.shp")
display(taxi_zones)

# COMMAND ----------

# Filter invalid zones
valid_ids = taxi_zones['LocationID'].tolist() 
location_dict = dict(zip(taxi_zones['LocationID'], taxi_zones['zone']))
df_invalid_zones = raw_data.filter(
    (~col("PUlocationID").isin(valid_ids)) | 
    (~col("DOlocationID").isin(valid_ids)) |
    col("PUlocationID").isNull() | 
    col("DOlocationID").isNull()
)
print(f"Invalid zones: {df_invalid_zones.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 6 – Consistência Entre Campos
# MAGIC
# MAGIC - Verificar se o `total_amount` corresponde à soma de seus componentes: tarifa, pedágios, gorjetas, encargos extras, impostos e taxas.  
# MAGIC
# MAGIC É considerada uma tolerância de **3% de erro** nessa verificaçã devido a coreções possibilidade de correções manuais e erros. 

# COMMAND ----------

from pyspark.sql.functions import abs as spark_abs, lit, when 
columns = ["fare_amount", "tolls_amount", "tip_amount", "extra", "mta_tax", "airport_fee", "improvement_surcharge", "total_amount"]
df_financial = raw_data.select(*columns).fillna(0)
df_financial = raw_data.withColumn("total_amount_calc", col("fare_amount")+col("tolls_amount")+col("tip_amount")+col("extra")+col("mta_tax")+col("airport_fee")+col("improvement_surcharge")+col("congestion_surcharge")+col("airport_fee"))
tolerance_pct = 0.03
df_financial = df_financial.withColumn(
    "financial_consistent",
    when(col("total_amount") == 0, lit(0))
    .when(
        (spark_abs(col("total_amount_calc") - col("total_amount")) / col("total_amount")) <= tolerance_pct,
        lit(1)
    ).otherwise(lit(0))
)
financial_inconsistencies = df_financial.filter(col("financial_consistent") == 1)
financial_consistencies = df_financial.filter(col("financial_consistent") == 0)
print(f"Total financial inconsistencies: {financial_inconsistencies.count()} (tolerance: {100*tolerance_pct}%)")
print(f"Total financial valid values: {financial_consistencies.count()} (tolerance: {100*tolerance_pct}%)")
print(f"Total values: {financial_inconsistencies.count()+financial_consistencies.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 7 – Detecção de Outliers
# MAGIC As distribuições de `trip_distance` e `fare_amount` são analisadas para identificar picos anormais ou outliers.  

# COMMAND ----------

columns = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"]
display(raw_data.select(*columns)) # ~ Histogram distribution
raw_data.select(*columns).describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 8 – Registros Duplicados
# MAGIC Os registros duplicados devem ser tratados antes de promover os dados para a Camada Silver.

# COMMAND ----------

from pyspark.sql.functions import count
duplicates = (
    raw_data.groupBy("tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance")
      .agg(count("*").alias("dup_count"))
      .filter(col("dup_count") > 1)
)
duplicates.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Planejamento – Camada Silver
# MAGIC
# MAGIC A Camada Silver realizará a padronização, enriquecimento e validação dos dados provenientes da Camada Raw.  
# MAGIC O objetivo é produzir conjuntos de dados consistentes, limpos e contextualizados, servindo como base confiáve
# MAGIC

# COMMAND ----------

# import silver_functions
import os, sys
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
if os.path.join(parent_dir, 'utilities') not in sys.path:
    sys.path.append(os.path.join(parent_dir, 'utilities'))
if 'silver_functions' in sys.modules:
    del sys.modules['silver_functions']
from silver_functions import *

# COMMAND ----------

# Read raw data
dataset = spark.sql("SELECT * FROM tlc.tripdata.tripdata_raw")
  
# # Apply transformations
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
dataset = dataset.fillna({"PUlocationID": "Unknown", "DOlocationID": "Unknown"})
dataset.withColumn("month", month(col("tpep_pickup_datetime"))).select(col("month")).orderBy(col("month")).distinct().show()

# dataset.display()
# dataset.count()
