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

# --- OpenTelemetry (optional) for Application Map ---
# If these are unavailable, the pipeline still runs.
try:
    from azure.monitor.opentelemetry import configure_azure_monitor
    from opentelemetry import trace
    from opentelemetry.trace import SpanKind
    _OTEL_AVAILABLE = True
except Exception:
    configure_azure_monitor = None
    trace = None
    SpanKind = None
    _OTEL_AVAILABLE = False

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

def _init_ai_telemetry(logger):
    """
    Best-effort OpenTelemetry setup for Application Insights.
    Returns a tracer or None. Never raises.
    """
    try:
        conn = (
            os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
            or os.getenv("APPINSIGHTS_CONNECTION_STRING")
            or os.getenv("APPINSIGHTS_CONNECTIONSTR")  # some env names vary
        )

        if not (_OTEL_AVAILABLE and conn):
            # No OpenTelemetry or no connection string: skip gracefully
            if not conn:
                logger.info("APPLICATIONINSIGHTS_CONNECTION_STRING not set — AI tracing disabled.")
            else:
                logger.info("OpenTelemetry not available — AI tracing disabled.")
            return None

        # Make sure the connection string is visible to the exporter
        os.environ.setdefault("APPLICATIONINSIGHTS_CONNECTION_STRING", conn)

        # Tell Azure SDK to emit OpenTelemetry dependency spans
        os.environ.setdefault("AZURE_SDK_TRACING_IMPLEMENTATION", "opentelemetry")

        # Give the node a readable name on the Application Map
        os.environ.setdefault("OTEL_SERVICE_NAME", "databricks-climate-pipeline")

        # Configure tracing/metrics/logs export to App Insights
        configure_azure_monitor()

        tracer = trace.get_tracer("climate-pipeline")
        logger.info("OpenTelemetry Application Insights exporter initialized.")
        return tracer

    except Exception as e:
        logger.error(f"Failed to initialize AI telemetry: {e}")
        return None

class ClimatePipeline:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

        # ---- Attach Azure Log Analytics handler if configured ----
        try:
            # Prefer full connection string; keep compat with old var
            conn = (
                os.getenv("APPINSIGHTS_CONNECTION_STRING")
                or os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
            )
            if AzureLogHandler and conn:
                az_handler = AzureLogHandler(connection_string=conn)
                az_handler.setLevel("INFO")
                self.logger.addHandler(az_handler)
                self.logger.info(
                    "Azure Log Analytics handler initialized.",
                    extra={"custom_dimensions": {
                        "component": "pipeline",
                        "module": "ClimatePipeline",
                        "event": "startup"
                    }}
                )
            elif not conn:
                self.logger.info("APPINSIGHTS_CONNECTION_STRING not set — Azure trace logging disabled.")
        except Exception as e:
            # Never fail the pipeline due to telemetry wiring
            self.logger.error(f"Failed to attach AzureLogHandler: {e}")

        # ---- OpenTelemetry tracer for Application Map (optional) ----
        self.tracer = _init_ai_telemetry(self.logger)

        self.acquirer = DataAcquisition(BASE_URL, LOCAL_DIR)
        self.transformer = DataTransformer(
            LAT_MIN, LAT_MAX, LON_MIN, LON_MAX, PARQUET_OUTPUT_DIR
        )
        self.storage = DataStorage(container_name="climate-data-analysis", base_dir="climate_data")

    def _span(self, name, kind=None, attrs=None):
        """
        Utility: start a no-op context manager if tracer isn't available.
        """
        if self.tracer and SpanKind:
            cm = self.tracer.start_as_current_span(name, kind=kind)
            span = cm.__enter__()
            try:
                if attrs:
                    for k, v in attrs.items():
                        span.set_attribute(k, v)
            finally:
                # Return a context manager that will close the span on exit
                class _Closer:
                    def __init__(self, cm): self._cm = cm
                    def __enter__(self): return span
                    def __exit__(self, exc_type, exc, tb): self._cm.__exit__(exc_type, exc, tb)
                return _Closer(cm)
        # Fallback no-op
        class _Noop:
            def __enter__(self): return None
            def __exit__(self, *a): return False
        return _Noop()

    def run(self, per_year_days=3, max_total_gb=3.0, max_workers=4):
        with self._span(
            "ClimatePipeline.run",
            kind=(SpanKind.SERVER if SpanKind else None),
            attrs={"pipeline.per_year_days": per_year_days,
                   "pipeline.max_total_gb": max_total_gb,
                   "pipeline.max_workers": max_workers}
        ):
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
                        with self._span(
                            "download",
                            kind=(SpanKind.CLIENT if SpanKind else None),
                            attrs={"s3.key": rec["key"], "component": "http"}
                        ):
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

            # stage 2: serial transform+upload
            success_uploads = 0
            for key, nc_file in downloads.items():
                if STOP.is_set(): break
                try:
                    with self._span("transform", attrs={"file": os.path.basename(nc_file)}):
                        parquet = self.transformer.process(nc_file)

                    os.remove(nc_file)  # clean raw after transform
                    if not parquet:
                        self.logger.error("Transform failed", extra={"custom_dimensions": {"key": key}})
                        continue

                    with self._span(
                        "upload",
                        kind=(SpanKind.CLIENT if SpanKind else None),
                        attrs={"component": "azure.storage", "blob.file": os.path.basename(parquet)}
                    ):
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
