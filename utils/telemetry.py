import os
import logging

# Try OpenTelemetry first (for Application Map & metrics)
try:
    from azure.monitor.opentelemetry import configure_azure_monitor
    from opentelemetry import trace
    _OTEL_AVAILABLE = True
except Exception:
    configure_azure_monitor = None
    trace = None
    _OTEL_AVAILABLE = False

# OpenCensus fallback for classic Log Analytics logs
try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler
except Exception:
    AzureLogHandler = None


def init_telemetry(logger_name: str = "telemetry"):
    """
    Initialize unified telemetry for Application Insights.
    Uses only APPLICATIONINSIGHTS_CONNECTION_STRING.
    Returns (logger, tracer) — safe even if telemetry isn't configured.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    conn = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    tracer = None

    if not conn:
        logger.warning("APPLICATIONINSIGHTS_CONNECTION_STRING not set — telemetry disabled.")
        return logger, tracer

    # --- Configure OpenTelemetry (Application Map + traces) ---
    if _OTEL_AVAILABLE:
        try:
            os.environ.setdefault("APPLICATIONINSIGHTS_CONNECTION_STRING", conn)
            os.environ.setdefault("OTEL_SERVICE_NAME", "climate-pipeline")
            os.environ.setdefault("AZURE_SDK_TRACING_IMPLEMENTATION", "opentelemetry")

            configure_azure_monitor()
            tracer = trace.get_tracer("climate-pipeline")
            logger.info("OpenTelemetry Application Insights initialized.")
        except Exception as e:
            logger.warning(f"OpenTelemetry init failed: {e}")

    # --- Configure OpenCensus (Log Analytics logs) ---
    if AzureLogHandler:
        try:
            handler = AzureLogHandler(connection_string=conn)
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)
            logger.info("Azure Log Analytics handler attached.")
        except Exception as e:
            logger.warning(f"AzureLogHandler init failed: {e}")

    return logger, tracer


def start_span(tracer, name: str, attributes: dict | None = None):
    """
    Helper to start a tracing span safely (no-op if tracer unavailable).
    Example:
        with start_span(tracer, "download", {"key": file_key}):
            ...
    """
    if tracer and _OTEL_AVAILABLE:
        cm = tracer.start_as_current_span(name)
        span = cm.__enter__()
        try:
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(k, v)
        finally:
            class _Closer:
                def __init__(self, cm): self.cm = cm
                def __enter__(self): return span
                def __exit__(self, et, e, tb): self.cm.__exit__(et, e, tb)
            return _Closer(cm)

    # no-op context manager if tracing disabled
    class _Noop:
        def __enter__(self): return None
        def __exit__(self, *a): return False
    return _Noop()
