# Databricks notebook source
# 1. Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col
import re

# 2. Start Spark Session (skip if running in Databricks notebook)
spark = SparkSession.builder \
    .appName("AppleJobs_BronzeLayer") \
    .getOrCreate()

# 3. Define File Paths
source_path = "/FileStore/tables/apple_jobs-1.csv"  # Databricks CSV path
bronze_path = "/FileStore/medallion/bronze/apple_jobs/"  # Bronze save path in DBFS

# 4. Read Raw CSV with Schema Inference
df_raw = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(source_path)

# 5. Print Schema (for reference)
print("=== RAW SCHEMA ===")
df_raw.printSchema()

# 6. Preview Raw Data
df_raw.show(5, truncate=False)

# 7. Add Metadata Columns
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name())

# 8. Clean Column Names to snake_case
def to_snake_case(name):
    name = name.strip()
    name = re.sub(r'[^0-9a-zA-Z]+', '_', name)
    return name.lower().strip('_')

for col_name in df_bronze.columns:
    df_bronze = df_bronze.withColumnRenamed(col_name, to_snake_case(col_name))

# 9. Preview Final Bronze Data
print("=== BRONZE LAYER DATA SAMPLE ===")
df_bronze.show(5, truncate=False)

# 10. Save as Parquet
df_bronze.write \
    .mode("overwrite") \
    .parquet(bronze_path)

# 11. Optional: Register Temp View for Querying
df_bronze.createOrReplaceTempView("bronze_apple_jobs")

print(f"✅ Bronze layer data saved at: {bronze_path}")


# COMMAND ----------

