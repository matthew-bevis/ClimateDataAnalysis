# Databricks notebook source
# MAGIC %md
# MAGIC # Climate Pipeline Runner (Databricks)
# MAGIC Use this notebook to run your existing pipeline code on Databricks **without modifying your repo**.
# MAGIC - Pulls secrets from a Databricks Secret Scope and exports them as env vars your code already expects.
# MAGIC - Optionally sets Spark config for ABFSS reads.
# MAGIC - Imports and runs `ClimatePipeline` from your repo.

# COMMAND ----------

# ===== 1) Parameters & Secrets -> Environment Variables =====

dbutils.widgets.text("PROJECT_ROOT", "/Workspace/Users/matthew-bevis@comcast.net/ClimateDataAnalysis")
dbutils.widgets.text("SECRET_SCOPE", "climate-scope")
dbutils.widgets.text("AZURE_ACCOUNT_KEY_NAME", "AZURE_STORAGE_KEY")
dbutils.widgets.text("AZURE_ACCOUNT_NAME_NAME", "AZURE_STORAGE_ACCOUNT")
dbutils.widgets.text("PER_YEAR_DAYS", "10")
dbutils.widgets.text("MAX_TOTAL_GB", "3.0")
dbutils.widgets.text("SET_ABFSS_CONF", "true")

PROJECT_ROOT = dbutils.widgets.get("PROJECT_ROOT")
SCOPE        = dbutils.widgets.get("SECRET_SCOPE")
KEY_NAME     = dbutils.widgets.get("AZURE_ACCOUNT_KEY_NAME")
ACC_NAME     = dbutils.widgets.get("AZURE_ACCOUNT_NAME_NAME")
PER_YEAR_DAYS = int(dbutils.widgets.get("PER_YEAR_DAYS"))
MAX_TOTAL_GB  = float(dbutils.widgets.get("MAX_TOTAL_GB"))
SET_ABFSS_CONF = dbutils.widgets.get("SET_ABFSS_CONF").lower() == "true"

# Pull secrets from scope
account = dbutils.secrets.get(SCOPE, ACC_NAME)
key     = dbutils.secrets.get(SCOPE, KEY_NAME)

# Export as env vars expected by your existing DataStorage class
import os
os.environ["AZURE_STORAGE_ACCOUNT"] = account
os.environ["AZURE_STORAGE_KEY"]     = key

if SET_ABFSS_CONF:
    spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)

print(f"- PROJECT_ROOT: {PROJECT_ROOT}")
print(f"- AZURE_STORAGE_ACCOUNT: {account}")
print(f"- ABFSS Spark conf set: {SET_ABFSS_CONF}")

# COMMAND ----------

# ===== 2) Import your repo code =====
import sys
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import os
print("Repo contents (top-level):", os.listdir(PROJECT_ROOT))

from pipeline.climate_pipeline import ClimatePipeline

print("Imported ClimatePipeline")

# COMMAND ----------

print(f"Running ClimatePipeline().run(per_year_days={PER_YEAR_DAYS}, max_total_gb={MAX_TOTAL_GB}) ...")
ClimatePipeline().run(per_year_days=PER_YEAR_DAYS, max_total_gb=MAX_TOTAL_GB)
print("Pipeline run complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Optional) Quick validation: read a sample of uploaded Parquet from ADLS
# MAGIC If your pipeline writes to `abfss://<container>@<account>.dfs.core.windows.net/climate_data/<year>/*.parquet`,
# MAGIC set the variables below and preview a few rows.

# COMMAND ----------

dbutils.widgets.text("CONTAINER", "climate-data-analysis")
dbutils.widgets.text("SAMPLE_GLOB", "climate_data/*/*.parquet")  # adjust if needed

CONTAINER = dbutils.widgets.get("CONTAINER")
SAMPLE_GLOB = dbutils.widgets.get("SAMPLE_GLOB")

abfss_path = f"abfss://{CONTAINER}@{account}.dfs.core.windows.net/{SAMPLE_GLOB}"
print("ABFSS path:", abfss_path)

try:
    df = spark.read.parquet(abfss_path).limit(10)
    display(df)
except Exception as e:
    print("Could not preview data:", e)
