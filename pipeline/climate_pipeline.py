import os
import time
from acquisition.data_acquisition import DataAcquisition
from transformation.data_transformer import DataTransformer
from storage.data_storage import DataStorage
from utils.logger import get_logger
from utils.checkpoint import load_processed, mark_processed

BASE_URL = "https://noaa-cdr-leaf-area-index-fapar-pds.s3.amazonaws.com"
LOCAL_DIR = "ClimateRecords"
PARQUET_OUTPUT_DIR = os.path.join(LOCAL_DIR, "parquet_output")

LAT_MIN, LAT_MAX = 25.0, 26.5
LON_MIN, LON_MAX = -81.5, -80.5


class ClimatePipeline:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.acquirer = DataAcquisition(BASE_URL, LOCAL_DIR)
        self.transformer = DataTransformer(
            LAT_MIN, LAT_MAX, LON_MIN, LON_MAX, PARQUET_OUTPUT_DIR
        )
        self.storage = DataStorage(container_name="climate", base_dir="climate_data")

    def run(self, per_year_days=3, max_total_gb=3.0):
        """Main pipeline runner with skip-checkpoint + upload to ADLS"""
        self.logger.info("Pipeline started")

        # Plan files to acquire
        max_bytes = int(max_total_gb * (1024**3))
        plan = self.acquirer.get_first_n_days_per_year_capped(
            per_year_days=per_year_days,
            years=range(1981, 2026),
            max_total_bytes=max_bytes
        )

        processed_keys = load_processed()
        self.logger.info(f"Loaded {len(processed_keys)} previously processed keys")

        downloaded = transformed = uploaded = failed = 0
        cloud_paths = []  # track ADLS upload locations
        file_times, start_time = [], time.time()

        for rec in plan:
            key = rec["key"]

            if key in processed_keys:
                self.logger.info(f"Skipping already processed: {key}")
                continue

            try:
                t0 = time.time()
                self.logger.info(f"Processing new file: {key}")

                # Step 1: Download raw .nc file
                nc_file = self.acquirer.download(key)
                downloaded += 1

                # Step 2: Transform into local parquet
                local_parquet = self.transformer.process(nc_file)
                os.remove(nc_file)  # cleanup .nc after transform

                if not local_parquet:
                    failed += 1
                    continue
                transformed += 1

                # Step 3: Upload parquet into ADLS
                cloud_path = self.storage.upload(local_parquet)
                if cloud_path:
                    uploaded += 1
                    cloud_paths.append(cloud_path)
                    mark_processed(key)
                else:
                    failed += 1

                dt = time.time() - t0
                file_times.append(dt)
                self.logger.info(f"Completed {os.path.basename(key)} in {dt:.2f}s")

            except Exception as e:
                failed += 1
                self.logger.error(f"Failed {key}: {e}")

        total_time = time.time() - start_time
        avg_file_time = (sum(file_times) / len(file_times)) if file_times else 0

        # Final summary
        self.logger.info(
            f"Pipeline summary: {downloaded} downloaded, {transformed} transformed, "
            f"{uploaded} uploaded, {failed} failed. "
            f"Total runtime {total_time:.2f}s (avg/file {avg_file_time:.2f}s)"
        )

        if cloud_paths:
            self.logger.info("Uploaded files stored in ADLS at:")
            for path in cloud_paths:
                self.logger.info(f"  {path}")

        self.logger.info("Pipeline completed")


if __name__ == "__main__":
    # Example run: first 10 days of each year, capped at 3 GB
    ClimatePipeline().run(per_year_days=20, max_total_gb=10.0)
