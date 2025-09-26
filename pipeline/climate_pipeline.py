import os
import time
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

class ClimatePipeline:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.acquirer = DataAcquisition(BASE_URL, LOCAL_DIR)
        self.transformer = DataTransformer(
            LAT_MIN, LAT_MAX, LON_MIN, LON_MAX, JSON_OUTPUT_DIR
        )
        self.storage = DataStorage(HDFS_URL, HDFS_DIR)

    def run(self, per_year_days=3, max_total_gb=3.0):
        """Main pipeline runner with skip-checkpoint support"""
        self.logger.info("Pipeline started")

        # Build a plan of files to process
        max_bytes = int(max_total_gb * (1024**3))
        plan = self.acquirer.get_first_n_days_per_year_capped(
            per_year_days=per_year_days,
            years=range(1981, 2026),
            max_total_bytes=max_bytes
        )

        processed_keys = load_processed()
        self.logger.info(f"Loaded {len(processed_keys)} previously processed keys")

        downloaded = processed = uploaded = failed = 0
        file_times, start_time = [], time.time()

        for rec in plan:
            key, size = rec["key"], rec["size"]

            if key in processed_keys:
                self.logger.info(f"Skipping already processed: {key}")
                continue

            try:
                t0 = time.time()
                self.logger.info(f"Processing new file: {key}")

                nc_file = self.acquirer.download(key)
                downloaded += 1

                json_file = self.transformer.process(nc_file)
                if json_file and self.storage.upload(json_file):
                    processed += 1
                    uploaded += 1
                    mark_processed(key)  # record success
                else:
                    failed += 1

                os.remove(nc_file)
                dt = time.time() - t0
                file_times.append(dt)
                self.logger.info(f"Completed {os.path.basename(key)} in {dt:.2f}s")

            except Exception as e:
                failed += 1
                self.logger.error(f"Failed {key}: {e}")

        total_time = time.time() - start_time
        avg_file_time = (sum(file_times) / len(file_times)) if file_times else 0

        self.logger.info(
            f"Pipeline summary: {downloaded} downloaded, {processed} processed, "
            f"{uploaded} uploaded, {failed} failed. "
            f"Total runtime {total_time:.2f}s (avg/file {avg_file_time:.2f}s)"
        )
        self.logger.info("Pipeline completed")

if __name__ == "__main__":
    # Grab ~10 days per year, but cap total at ~3 GB
    ClimatePipeline().run(per_year_days=10, max_total_gb=3.0)

