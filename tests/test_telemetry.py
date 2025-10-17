"""
Smoke-test for utils/telemetry.py

What it does:
- Reads APPLICATIONINSIGHTS_CONNECTION_STRING from env
- Initializes your telemetry (OpenTelemetry distro + AzureLogHandler fallback)
- Emits a few logs
- Creates nested spans: test.run -> http.example -> compute.fake -> error.demo
- Makes a small HTTP GET to https://www.example.com (shows as dependency if requests is instrumented)
- Force-flushes the OTEL provider and waits briefly to ensure export
"""

import os
import time
import logging
import traceback
from dotenv import load_dotenv
from utils.telemetry import init_telemetry, start_span

# --- Make sure your repo is importable if you run this from anywhere ---
# Adjust the PROJECT_ROOT resolution if needed.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in os.sys.path:
    os.sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

# Helpful defaults to make the app easy to find in App Insights
os.environ.setdefault("OTEL_SERVICE_NAME", "climate-pipeline-smoketest")
# Add tags you'll filter on in KQL (optional)
os.environ.setdefault("OTEL_RESOURCE_ATTRIBUTES", "env=dev,component=smoketest")

# IMPORTANT: your connection string must be present
CONN = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")

def main():
    # Sanity print (redacted)
    print("Has APPLICATIONINSIGHTS_CONNECTION_STRING?", bool(CONN))
    if CONN:
        print("Conn prefix:", CONN[:32] + "…")

    # Initialize telemetry using your helper (attaches AzureLogHandler + OTEL exporter)
    logger = logging.getLogger("telemetry-smoke")
    logger, tracer = init_telemetry(logger_name=logger.name)

    logger.info("✅ Telemetry smoke test starting")
    logger.warning("This is a WARNING test log")
    logger.error("This is a harmless ERROR test log to verify severity mapping")

    # Root span
    with start_span(tracer, "test.run", {"runner": "tests/test_telemetry.py"}):

        # Nested HTTP span (you’ll also see an HTTP dependency if requests is auto-instrumented on the cluster)
        try:
            import requests
            with start_span(tracer, "http.example", {"url": "https://www.example.com"}):
                r = requests.get("https://www.example.com", timeout=5)
                logger.info("HTTP status (example.com) = %s", getattr(r, "status_code", None))
        except Exception as http_exc:
            logger.exception("HTTP test failed: %s", http_exc)

        # A fake compute span
        with start_span(tracer, "compute.fake", {"work_units": 3}):
            total = 0
            for i in range(3):
                total += i * i
                time.sleep(0.25)
            logger.info("Fake compute result = %s", total)

        # An exception inside a span (to confirm exceptions surface in AI)
        try:
            with start_span(tracer, "error.demo", {"will_raise": True}):
                raise ValueError("Demonstration error from telemetry smoke test")
        except Exception as e:
            # Log the exception (OpenTelemetry will also annotate the active span)
            logger.exception("Expected demo exception: %s", e)

    # Try to force-flush OpenTelemetry exporters (if available)
    try:
        from opentelemetry import trace as _otel_trace
        provider = _otel_trace.get_tracer_provider()
        if hasattr(provider, "force_flush"):
            provider.force_flush()
    except Exception:
        pass

    # Give exporters a moment to ship (especially on ephemeral runs)
    time.sleep(3.0)

    logger.info("✅ Telemetry smoke test finished")

if __name__ == "__main__":
    main()
