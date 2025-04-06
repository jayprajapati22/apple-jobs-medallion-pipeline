# Databricks notebook source
# 1. Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, count, when, isnan, desc, to_date, year, month


# COMMAND ----------

spark = SparkSession.builder.appName("AppleJobsEDA").getOrCreate()

# COMMAND ----------

#Load CSV file
df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/apple_jobs-1.csv")

# COMMAND ----------

#Show first few rows
df.show(5)

# COMMAND ----------

# Print schema
df.printSchema()

# COMMAND ----------

#Count rows and columns
print(f"Rows: {df.count()}, Columns: {len(df.columns)}")

# COMMAND ----------

#List all column names
print(df.columns)


# COMMAND ----------

#Summary statistics for all columns
df.describe().show()


# COMMAND ----------

#Check for nulls in each column
df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).show()


# COMMAND ----------

#Count unique values in each column
for c in df.columns:
    df.select(c).distinct().count()

# COMMAND ----------

#Value counts of job titles
df.groupBy("title").count().orderBy(desc("count")).show(10)

# COMMAND ----------

#Top 10 locations
df.groupBy("location").count().orderBy(desc("count")).show(10)

# COMMAND ----------

#Most common team (if exists)
if "team" in df.columns:
    df.groupBy("team").count().orderBy(desc("count")).show(10)

# COMMAND ----------

#Drop duplicate rows
df = df.dropDuplicates()

# COMMAND ----------

#Add 'country' column from 'location'
df = df.withColumn("country", split(col("location"), ",").getItem(-1))

# COMMAND ----------

#Count by country
df.groupBy("country").count().orderBy(desc("count")).show(10)

# COMMAND ----------

#Add 'region' as USA/Other
df = df.withColumn("region", when(col("country") == "United States", "USA").otherwise("Other"))
df.groupBy("region").count().show()

# COMMAND ----------

#Count how many job titles contain 'Engineer'
df.filter(col("title").contains("Engineer")).count()

# COMMAND ----------

#Check if any location has more than 100 jobs
df.groupBy("location").count().filter("count > 100").show()

# COMMAND ----------

#Use SQL to get top 5 departments (if exists)
if "department" in df.columns:
    spark.sql("SELECT department, COUNT(*) as total FROM apple_jobs GROUP BY department ORDER BY total DESC LIMIT 5").show()


# COMMAND ----------

