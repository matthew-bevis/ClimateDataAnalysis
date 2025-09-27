# Databricks notebook source
# MAGIC %md
# MAGIC # LAI & FAPAR Yearly Trends
# MAGIC This notebook queries the parquet files stored in ADLS and computes yearly averages.

# COMMAND ----------
import os
from pyspark.sql import functions as F

# Pull environment variables
storage_account = dbutils.secrets.get("climate-scope", "AZURE_STORAGE_ACCOUNT")
container = dbutils.secrets.get("climate-scope", "AZURE_BLOB_CONTAINER")
storage_key = dbutils.secrets.get("climate-scope", "AZURE_STORAGE_KEY")

if not (storage_account and container and storage_key):
    raise ValueError("Missing storage configuration. Ensure env vars or secrets are set.")

# Configure Spark to authenticate with ADLS Gen2
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Build the path dynamically
path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/climate_data/*/*.parquet"

# Load from ADLS
df = spark.read.parquet(path)
df.createOrReplaceTempView("climate")

# COMMAND ----------
# Yearly average LAI & FAPAR
yearly = (
    df.groupBy(F.substring("date", 1, 4).alias("year"))
      .agg(F.avg("lai").alias("avg_lai"), F.avg("fapar").alias("avg_fapar"))
      .orderBy("year")
)

display(yearly)
