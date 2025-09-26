from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add project root to PYTHONPATH
PROJECT_ROOT = "/mnt/c/Users/Matt/VSCodeProjects/Springboard/Springboard/ClimateDataAnalysis"
sys.path.append(PROJECT_ROOT)

from acquisition.data_acquisition import DataAcquisition
from transformation.data_transformer import DataTransformer
from storage.data_storage import DataStorage
from utils.logger import get_logger
from utils.checkpoint import load_processed, mark_processed

BASE_URL = "https://noaa-cdr-leaf-area-index-fapar-pds.s3.amazonaws.com"
LOCAL_DIR = "ClimateRecords"
JSON_OUTPUT_DIR = os.path.join(LOCAL_DIR, "json_output")
HDFS_URL = "http://localhost:9870"
HDFS_DIR = "/climate_data"

LAT_MIN, LAT_MAX = 25.0, 26.5
LON_MIN, LON_MAX = -81.5, -80.5


def run_daily_pipeline(**context):
    """Run pipeline for the given Airflow execution_date (YYYYMMDD)."""
    logger = get_logger("DailyPipeline")

    exec_date = context["ds"]  # e.g. "2025-09-27"
    date_str = exec_date.replace("-", "")  # e.g. "20250927"
    year = date_str[:4]

    logger.info(f"Running daily pipeline for {date_str}")

    # Initialize components
    acquirer = DataAcquisition(BASE_URL, LOCAL_DIR)
    transformer = DataTransformer(LAT_MIN, LAT_MAX, LON_MIN, LON_MAX, JSON_OUTPUT_DIR)
    storage = DataStorage(HDFS_URL, HDFS_DIR)

    # List all S3 keys
    keys = acquirer._list_objects()
    todays = [k for k in keys if date_str in k["key"]]

    if not todays:
        logger.warning(f"No files found for {date_str}")
        return

    processed_keys = load_processed()

    for rec in todays:
        key = rec["key"]
        if key in processed_keys:
            logger.info(f"Skipping already processed: {key}")
            continue

        try:
            nc_file = acquirer.download(key)
            json_file = transformer.process(nc_file)

            if json_file and storage.upload(json_file):
                mark_processed(key)
                logger.info(f"Uploaded {json_file} to {HDFS_DIR}/{year}/")
            else:
                logger.warning(f"No data written for {key}")

            os.remove(nc_file)

        except Exception as e:
            logger.error(f"Failed {key}: {e}")


# -------------------------
# Airflow DAG Definition
# -------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["you@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="climate_pipeline_daily",
    default_args=default_args,
    description="Ingest NOAA climate data for a single execution_date",
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 27),
    catchup=True,   # enables backfill one day at a time
    tags=["climate", "noaa", "daily"],
) as dag:

    daily_task = PythonOperator(
        task_id="run_daily_pipeline",
        python_callable=run_daily_pipeline,
        provide_context=True,
    )
