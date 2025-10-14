import os
import time
import concurrent.futures, signal, threading, glob, os
from acquisition.data_acquisition import DataAcquisition
from transformation.data_transformer import DataTransformer
from storage.data_storage import DataStorage
from utils.logger import get_logger
from utils.checkpoint import load_processed, mark_processed
from dotenv import load_dotenv
from queue import Queue


load_dotenv()

BASE_URL = "https://noaa-cdr-leaf-area-index-fapar-pds.s3.amazonaws.com"
LOCAL_DIR = "ClimateRecords"
PARQUET_OUTPUT_DIR = os.path.join(LOCAL_DIR, "parquet_output")

LAT_MIN, LAT_MAX = 25.0, 26.5
LON_MIN, LON_MAX = -81.5, -80.5

STOP = threading.Event()

def _install_signal_handlers(logger):
    def _handle(sig, frame):
        logger.warning("Signal %s received — requesting graceful shutdown...", sig)
        STOP.set()
    signal.signal(signal.SIGINT,  _handle)
    signal.signal(signal.SIGTERM, _handle)

def _cleanup_stale_part_files(root_dir, logger):
    stale = glob.glob(os.path.join(root_dir, "**", "*.part"), recursive=True)
    for p in stale:
        try: os.remove(p)
        except: pass
    if stale:
        logger.info("Removed %d stale .part files from prior runs", len(stale))

def _transform_one(nc_path, lat_min, lat_max, lon_min, lon_max, outdir, logger):
    """Thread-safe single-file transform; returns (ok, parquet_path|None)."""
    from transformation.data_transformer import DataTransformer
    import os
    try:
        transformer = DataTransformer(lat_min, lat_max, lon_min, lon_max, outdir)
        parquet = transformer.process(nc_path)
        try:
            if os.path.exists(nc_path):
                os.remove(nc_path)
        except Exception:
            pass
        return True, parquet
    except Exception as e:
        logger.error(f"Transform error for {nc_path}: {e}")
        try:
            if os.path.exists(nc_path):
                os.remove(nc_path)
        except Exception:
            pass
        return False, None

class ClimatePipeline:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.acquirer = DataAcquisition(BASE_URL, LOCAL_DIR)
        self.transformer = DataTransformer(
            LAT_MIN, LAT_MAX, LON_MIN, LON_MAX, PARQUET_OUTPUT_DIR
        )
        self.storage = DataStorage(container_name="climate-data-analysis", base_dir="climate_data")

    def run(self, per_year_days=3, max_total_gb=3.0, max_workers=4):
        self.logger.info("Pipeline started")
        _install_signal_handlers(self.logger)
        _cleanup_stale_part_files(LOCAL_DIR, self.logger)

        plan = self.acquirer.get_first_n_days_per_year_capped(
            per_year_days=per_year_days, years=range(1981, 2026),
            max_total_bytes=int(max_total_gb * (1024**3))
        )

        processed_keys = load_processed()
        todo = [rec for rec in plan if rec["key"] not in processed_keys]

        # stage 1: parallel downloads
        downloads = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            try:
                futures = {
                    pool.submit(self.acquirer.download, rec["key"]): rec
                    for rec in todo
                }
                for fut in concurrent.futures.as_completed(futures):
                    if STOP.is_set(): break
                    rec = futures[fut]
                    path = fut.result()
                    if path: downloads[rec["key"]] = path
            except KeyboardInterrupt:
                STOP.set()
                self.logger.warning("Interrupted during downloads; cancelling pending tasks...")
                pool.shutdown(wait=False, cancel_futures=True)
                raise

        if STOP.is_set():
            self.logger.info("Shutdown requested. Exiting early after downloads.")
            return

        # stage 2: serial or light-parallel transform+upload
        for key, nc_file in downloads.items():
            if STOP.is_set(): break
            try:
                parquet = self.transformer.process(nc_file)
                os.remove(nc_file)  # clean raw after transform
                if not parquet: 
                    self.logger.error("Transform failed for %s", key); continue
                cloud_path = self.storage.upload(parquet)
                if cloud_path:
                    mark_processed(key)
                else:
                    self.logger.error("Upload failed for %s", key)
            except KeyboardInterrupt:
                STOP.set(); self.logger.warning("Interrupted during transform/upload.")
                break

        self.logger.info("Pipeline completed")

if __name__ == "__main__":
    ClimatePipeline().run(per_year_days=30, max_total_gb=20.0)
