# utils/telemetry.py
from __future__ import annotations

import os
import logging
from typing import Optional

# --- Load .env early (safe if file is absent) ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- OpenTelemetry (Azure Monitor distro) ---
_OTEL_AVAILABLE = True
try:
    from azure.monitor.opentelemetry import configure_azure_monitor
    from opentelemetry import trace
    # Optional: auto-instrument HTTP if installed
    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
    except Exception:
        RequestsInstrumentor = None
except Exception:
    configure_azure_monitor = None
    trace = None
    RequestsInstrumentor = None
    _OTEL_AVAILABLE = False

# --- OpenCensus fallback (classic traces table logs) ---
try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler
except Exception:
    AzureLogHandler = None


APPINSIGHTS_ENV = "APPLICATIONINSIGHTS_CONNECTION_STRING"
LEGACY_ENV = "APPINSIGHTS_CONNECTION_STRING"  # legacy safety

DEFAULT_SCOPE = os.getenv("DBX_APPINSIGHTS_SCOPE", "climate-scope")
DEFAULT_KEY   = os.getenv("DBX_APPINSIGHTS_KEY_NAME", APPINSIGHTS_ENV)


def _find_dbutils() -> Optional[object]:
    """Best-effort discovery of Databricks dbutils; returns the object or None."""
    if "dbutils" in globals():
        return globals()["dbutils"]
    try:
        ip = get_ipython()  # type: ignore[name-defined]
        if ip and "dbutils" in getattr(ip, "user_ns", {}):
            return ip.user_ns["dbutils"]
    except Exception:
        pass
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return None
    return None


def _resolve_connection_string(logger: logging.Logger) -> Optional[str]:
    """
    Priority:
    1) APPLICATIONINSIGHTS_CONNECTION_STRING (env/.env)
    2) APPINSIGHTS_CONNECTION_STRING (legacy)
    3) Databricks secret scope (DBX_APPINSIGHTS_SCOPE / DBX_APPINSIGHTS_KEY_NAME), unless disabled
    """
    conn = os.getenv(APPINSIGHTS_ENV)
    if conn:
        logger.debug(f"{APPINSIGHTS_ENV} found in env/.env")
        return conn

    legacy = os.getenv(LEGACY_ENV)
    if legacy:
        logger.debug(f"{LEGACY_ENV} found in env (legacy)")
        return legacy

    if os.getenv("TELEMETRY_DISABLE_DBUTILS", "").strip() == "1":
        logger.debug("DBUtils lookup disabled via TELEMETRY_DISABLE_DBUTILS=1")
        return None

    dbu = _find_dbutils()
    if dbu is None:
        logger.debug("dbutils not available; skipping secret lookup")
        return None

    scope = os.getenv("DBX_APPINSIGHTS_SCOPE", DEFAULT_SCOPE)
    key   = os.getenv("DBX_APPINSIGHTS_KEY_NAME", DEFAULT_KEY)
    try:
        secret_val = dbu.secrets.get(scope, key)  # type: ignore[attr-defined]
        if secret_val:
            logger.info(f"Loaded App Insights connection from Databricks secret scope '{scope}' key '{key}'")
            return secret_val
    except Exception as e:
        logger.warning(f"Failed to load App Insights connection from dbutils secrets [{scope}/{key}]: {e}")

    return None


def init_telemetry(
    logger_name: str = "telemetry",
    service_name: str | None = None,
    service_version: str | None = None,
) -> tuple[logging.Logger, Optional[object]]:
    """
    Initialize Azure Monitor OpenTelemetry and (optionally) OpenCensus log export.
    Returns (logger, tracer). Safe no-op if connection string cannot be resolved.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    conn = _resolve_connection_string(logger)
    tracer = None

    if not conn:
        logger.warning(f"{APPINSIGHTS_ENV} not set and no secret found — telemetry disabled.")
        return logger, tracer

    # Make connection string available to any libs that still read from env
    os.environ[APPINSIGHTS_ENV] = conn
    os.environ.setdefault(LEGACY_ENV, conn)

    # Reasonable defaults (overridable via env before calling this)
    if service_name:
        os.environ.setdefault("OTEL_SERVICE_NAME", service_name)
    else:
        os.environ.setdefault("OTEL_SERVICE_NAME", "climate-pipeline")
    if service_version:
        os.environ.setdefault("OTEL_RESOURCE_ATTRIBUTES", f"service.version={service_version}")

    # --- Configure OpenTelemetry (preferred) ---
    if _OTEL_AVAILABLE and configure_azure_monitor:
        try:
            configure_azure_monitor(connection_string=conn)

            # Optional: auto-instrument HTTP dependencies if available
            if RequestsInstrumentor:
                RequestsInstrumentor().instrument()

            tracer = trace.get_tracer(os.environ.get("OTEL_SERVICE_NAME", "climate-pipeline"))
            logger.info("OpenTelemetry → Azure Monitor initialized.")
            print("✅ Telemetry initialized — logs and traces will be sent to Azure Application Insights.")
        except Exception as e:
            logger.warning(f"OpenTelemetry init failed: {e}")

    # --- OpenCensus fallback for logs to traces table ---
    if AzureLogHandler:
        try:
            handler = AzureLogHandler(connection_string=conn)
            handler.setLevel(logging.INFO)
            if not any(isinstance(h, AzureLogHandler) for h in logger.handlers):
                logger.addHandler(handler)
            logger.info("AzureLogHandler attached (OpenCensus fallback).")
        except Exception as e:
            logger.warning(f"AzureLogHandler init failed: {e}")

    return logger, tracer


def start_span(tracer, name: str, attributes: dict | None = None):
    """Safe span helper: returns a context manager; no-op if tracer unavailable."""
    if tracer and _OTEL_AVAILABLE:
        cm = tracer.start_as_current_span(name)
        span = cm.__enter__()
        try:
            if attributes:
                for k, v in attributes.items():
                    try:
                        span.set_attribute(k, v)
                    except Exception:
                        pass
        finally:
            class _Closer:
                def __init__(self, ctx_mgr): self._cm = ctx_mgr
                def __enter__(self): return span
                def __exit__(self, et, ex, tb): self._cm.__exit__(et, ex, tb)
            return _Closer(cm)

    class _Noop:
        def __enter__(self): return None
        def __exit__(self, *a): return False
    return _Noop()
