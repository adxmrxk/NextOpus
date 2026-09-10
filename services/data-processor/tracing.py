"""OpenTelemetry setup for the Data Processor.

Tracing stays off unless OTEL_EXPORTER_OTLP_ENDPOINT is set, so local runs and
the test suite need no collector and every span becomes a no-op.
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Probes and scrapes would otherwise bury the interesting traces.
EXCLUDED_URLS = "health,ready,metrics"


def tracing_enabled() -> bool:
    return bool(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))


def setup_tracing(service_name: Optional[str] = None) -> None:
    """Configure the global tracer provider and OTLP/HTTP exporter."""
    if not tracing_enabled():
        logger.info("Tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set)")
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    endpoint = os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"].rstrip("/")
    name = service_name or os.getenv("OTEL_SERVICE_NAME", "data-processor")

    resource = Resource.create(
        {
            "service.name": name,
            "service.version": "1.0.0",
            "deployment.environment": os.getenv("ENVIRONMENT", "local"),
            "service.instance.id": os.getenv("HOSTNAME", "unknown"),
        }
    )

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
    )
    trace.set_tracer_provider(provider)

    logger.info("Tracing enabled: exporting to %s as %r", endpoint, name)


def instrument_app(app) -> None:
    """Attach FastAPI instrumentation.

    This is what reads the W3C traceparent header the generator sends, so the
    server span becomes a child of the generator's batch span rather than the
    root of a separate trace.
    """
    if not tracing_enabled():
        return

    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app, excluded_urls=EXCLUDED_URLS)
    logger.info("FastAPI instrumented (excluding: %s)", EXCLUDED_URLS)


def get_tracer(name: str = "nextopus.data-processor"):
    """Return a tracer. Safe to call whether or not tracing is configured."""
    from opentelemetry import trace

    return trace.get_tracer(name)
