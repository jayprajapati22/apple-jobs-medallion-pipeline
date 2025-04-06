# Databricks notebook source
# 1. Import Required Libraries
from pyspark.sql.functions import col, trim, lower, split, when

# 2. Define Paths
bronze_path = "/FileStore/medallion/bronze/apple_jobs/"
silver_path = "/FileStore/medallion/silver/apple_jobs/"

# 3. Load Bronze Data
df_bronze = spark.read.parquet(bronze_path)

# 4. Data Cleaning & Transformation

# 4.1 Drop rows with null in critical columns (e.g., title or location)
df_cleaned = df_bronze.dropna(subset=["title", "location"])

# 4.2 Standardize text columns (trim + lowercase for title, location, etc.)
text_columns = ["title", "location", "source_file"]
for col_name in text_columns:
    df_cleaned = df_cleaned.withColumn(col_name, lower(trim(col(col_name))))

# 4.3 Extract city and country from location
# Example: "cupertino, united states" → city = cupertino, country = united states
df_cleaned = df_cleaned.withColumn("city", split(col("location"), ",").getItem(0)) \
                       .withColumn("country", trim(split(col("location"), ",").getItem(1)))

# 4.4 Optional: Add a job_type column based on keywords in title
df_cleaned = df_cleaned.withColumn(
    "job_type",
    when(col("title").like("%intern%"), "Intern")
    .when(col("title").like("%manager%"), "Manager")
    .otherwise("Full-Time")
)

# 5. Preview Silver Data
print("=== SILVER LAYER DATA SAMPLE ===")
df_cleaned.show(5, truncate=False)

# 6. Save to Parquet
df_cleaned.write.mode("overwrite").parquet(silver_path)

# 7. Register a Temp View
df_cleaned.createOrReplaceTempView("silver_apple_jobs")

print(f"✅ Silver layer data saved to: {silver_path}")


# COMMAND ----------

