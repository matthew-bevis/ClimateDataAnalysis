# Databricks notebook source
# MAGIC %md
# MAGIC # Climate Data Pipeline Runner (Databricks)
# MAGIC This notebook runs your updated **parallelized Climate Data Analysis Pipeline** on Databricks.
# MAGIC 
# MAGIC Features:
# MAGIC - Pulls **Azure credentials** from a Databricks secret scope  
# MAGIC - Exports environment variables expected by your repo  
# MAGIC - Supports **parallel downloads & transformations** via `ThreadPoolExecutor`  
# MAGIC - Integrates with **Azure Data Lake (ABFSS)** for output validation  
# MAGIC - Provides optional runtime parameterization and lightweight Spark validation

# COMMAND ----------

# ===== Parameters & Secret Setup =====

dbutils.widgets.text("PROJECT_ROOT", "/Workspace/Repos/matthew-bevis@comcast.net/ClimateDataAnalysis")
dbutils.widgets.text("SECRET_SCOPE", "climate-scope")
dbutils.widgets.text("AZURE_ACCOUNT_KEY_NAME", "AZURE_STORAGE_KEY")
dbutils.widgets.text("AZURE_ACCOUNT_NAME_NAME", "AZURE_STORAGE_ACCOUNT")
dbutils.widgets.text("CONTAINER", "climate-data-analysis")
dbutils.widgets.text("PER_YEAR_DAYS", "10")
dbutils.widgets.text("MAX_TOTAL_GB", "3.0")
dbutils.widgets.text("MAX_WORKERS", "4")
dbutils.widgets.text("SET_ABFSS_CONF", "true")
dbutils.widgets.text("APPINSIGHTS_SECRET_NAME", "APPLICATIONINSIGHTS_CONNECTION_STRING")

AI_NAME      = dbutils.widgets.get("APPINSIGHTS_SECRET_NAME")
PROJECT_ROOT = dbutils.widgets.get("PROJECT_ROOT")
SCOPE        = dbutils.widgets.get("SECRET_SCOPE")
KEY_NAME     = dbutils.widgets.get("AZURE_ACCOUNT_KEY_NAME")
ACC_NAME     = dbutils.widgets.get("AZURE_ACCOUNT_NAME_NAME")
CONTAINER    = dbutils.widgets.get("CONTAINER")

PER_YEAR_DAYS = int(dbutils.widgets.get("PER_YEAR_DAYS"))
MAX_TOTAL_GB  = float(dbutils.widgets.get("MAX_TOTAL_GB"))
MAX_WORKERS   = int(dbutils.widgets.get("MAX_WORKERS"))
SET_ABFSS_CONF = dbutils.widgets.get("SET_ABFSS_CONF").lower() == "true"

# Retrieve Azure credentials from Databricks Secret Scope
account_name = dbutils.secrets.get(SCOPE, ACC_NAME)
account_key  = dbutils.secrets.get(SCOPE, KEY_NAME)
ai_conn = dbutils.secrets.get(SCOPE, AI_NAME)

import os
os.environ["AZURE_STORAGE_ACCOUNT"] = account_name
os.environ["AZURE_STORAGE_KEY"] = account_key
os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"] = ai_conn

if SET_ABFSS_CONF:
    spark.conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)

print(f"Project root: {PROJECT_ROOT}")
print(f"Storage account: {account_name}")
print(f"Container: {CONTAINER}")
print(f"Parallel workers: {MAX_WORKERS}")
print(f"ABFSS config set: {SET_ABFSS_CONF}")

# COMMAND ----------

# ===== Import Repo Code =====
import sys, os
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

print("Repo contents:", os.listdir(PROJECT_ROOT))

from pipeline.climate_pipeline import ClimatePipeline
print("Imported ClimatePipeline successfully")

# COMMAND ----------

# ===== Run the Pipeline =====
print(f"Running ClimatePipeline with per_year_days={PER_YEAR_DAYS}, max_total_gb={MAX_TOTAL_GB}, workers={MAX_WORKERS}")

try:
    ClimatePipeline().run(per_year_days=PER_YEAR_DAYS, max_total_gb=MAX_TOTAL_GB, max_workers=MAX_WORKERS)
    print("Pipeline execution complete.")
except Exception as e:
    print("Pipeline failed:", e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional Validation: Inspect Parquet in ADLS
# MAGIC Quickly preview the output of your pipeline to verify schema and records.

# COMMAND ----------

dbutils.widgets.text("SAMPLE_GLOB", "climate_data/*/*.parquet")
SAMPLE_GLOB = dbutils.widgets.get("SAMPLE_GLOB")

abfss_path = f"abfss://{CONTAINER}@{account_name}.dfs.core.windows.net/{SAMPLE_GLOB}"
print("Sample ABFSS path:", abfss_path)

try:
    df = spark.read.parquet(abfss_path).limit(20)
    display(df)
except Exception as e:
    print("Unable to read Parquet data:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Test & Coverage Run
# MAGIC You can run your pytest suite inside Databricks (if repo tests are present) for regression validation.

# COMMAND ----------

# Optional: only run if pytest is installed
try:
    import pytest
    print("Running pytest coverage...")
    !pytest -q --cov=pipeline --cov=transformation --cov=acquisition --cov=storage --cov=utils --cov-report=term-missing
except Exception as e:
    print("Pytest not available or failed:", e)
