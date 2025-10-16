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

# --- Azure logging (optional) ---
try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler
except Exception:
    AzureLogHandler = None

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

        # ---- Attach Azure Log Analytics handler if configured ----
        try:
            conn = os.getenv("APPINSIGHTS_CONNECTION_STRING")
            if AzureLogHandler and conn:
                az_handler = AzureLogHandler(connection_string=conn)
                # Optional: keep same verbosity as file logger
                az_handler.setLevel("INFO")
                self.logger.addHandler(az_handler)
                # One-time startup breadcrumb
                self.logger.info(
                    "Azure Log Analytics handler initialized.",
                    extra={"custom_dimensions": {
                        "component": "pipeline",
                        "module": "ClimatePipeline",
                        "event": "startup"
                    }}
                )
            elif not conn:
                self.logger.warning("APPINSIGHTS_CONNECTION_STRING not set — Azure telemetry disabled.")
        except Exception as e:
            # Never fail the pipeline due to telemetry wiring
            self.logger.error(f"Failed to attach AzureLogHandler: {e}")

        self.acquirer = DataAcquisition(BASE_URL, LOCAL_DIR)
        self.transformer = DataTransformer(
            LAT_MIN, LAT_MAX, LON_MIN, LON_MAX, PARQUET_OUTPUT_DIR
        )
        self.storage = DataStorage(container_name="climate-data-analysis", base_dir="climate_data")

    def run(self, per_year_days=3, max_total_gb=3.0, max_workers=4):
        self.logger.info("Pipeline started", extra={"custom_dimensions": {
            "per_year_days": per_year_days,
            "max_total_gb": max_total_gb,
            "max_workers": max_workers
        }})
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
                    if path:
                        downloads[rec["key"]] = path
                    else:
                        self.logger.error("Download failed", extra={"custom_dimensions": {"key": rec["key"]}})
            except KeyboardInterrupt:
                STOP.set()
                self.logger.warning("Interrupted during downloads; cancelling pending tasks...")
                pool.shutdown(wait=False, cancel_futures=True)
                raise

        if STOP.is_set():
            self.logger.info("Shutdown requested. Exiting early after downloads.")
            return

        # stage 2: serial or light-parallel transform+upload
        success_uploads = 0
        for key, nc_file in downloads.items():
            if STOP.is_set(): break
            try:
                parquet = self.transformer.process(nc_file)
                os.remove(nc_file)  # clean raw after transform
                if not parquet:
                    self.logger.error("Transform failed", extra={"custom_dimensions": {"key": key}})
                    continue
                cloud_path = self.storage.upload(parquet)
                if cloud_path:
                    success_uploads += 1
                    mark_processed(key)
                    self.logger.info("Uploaded", extra={"custom_dimensions": {
                        "key": key, "cloud_path": cloud_path
                    }})
                else:
                    self.logger.error("Upload failed", extra={"custom_dimensions": {"key": key}})
            except KeyboardInterrupt:
                STOP.set(); self.logger.warning("Interrupted during transform/upload.")
                break
            except Exception as e:
                self.logger.error(f"Unhandled error for {key}: {e}")

        self.logger.info("Pipeline completed", extra={"custom_dimensions": {
            "planned": len(plan),
            "skipped": len(plan) - len(todo),
            "downloaded": len(downloads),
            "uploaded": success_uploads
        }})

if __name__ == "__main__":
    ClimatePipeline().run(per_year_days=30, max_total_gb=20.0)