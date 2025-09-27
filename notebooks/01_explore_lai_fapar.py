# Databricks notebook source
# MAGIC %md
# MAGIC # LAI & FAPAR Yearly Trends
# MAGIC This notebook queries the parquet files stored in ADLS and computes yearly averages.

# COMMAND ----------
from pyspark.sql import functions as F

# Load from ADLS (replace with your account + container)
df = spark.read.parquet(
    "abfss://climate-data-analysis@datalakeq7aj6k0.dfs.core.windows.net/climate_data/*/*.parquet"
)

df.createOrReplaceTempView("climate")

# COMMAND ----------
# Yearly average LAI & FAPAR
yearly = (
    df.groupBy(F.substring("date", 1, 4).alias("year"))
      .agg(F.avg("lai").alias("avg_lai"), F.avg("fapar").alias("avg_fapar"))
      .orderBy("year")
)

display(yearly)  # Databricks will render a nice table/graph
