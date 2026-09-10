package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// tracerName identifies the instrumentation scope in Jaeger.
const tracerName = "github.com/nextopus/data-generator"

// initTracing wires up OTLP/HTTP export to a collector such as Jaeger.
//
// When OTEL_EXPORTER_OTLP_ENDPOINT is unset, tracing stays disabled and every
// span becomes a no-op, so local runs and tests need no collector.
func initTracing(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }

	if cfg.OTLPEndpoint == "" {
		log.Println("Tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set)")
		return noop, nil
	}

	// The exporter wants a host:port, not a scheme.
	endpoint := strings.TrimPrefix(strings.TrimPrefix(cfg.OTLPEndpoint, "http://"), "https://")
	endpoint = strings.TrimSuffix(endpoint, "/")

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return noop, fmt.Errorf("creating OTLP exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion("1.0.0"),
			semconv.DeploymentEnvironment(envOrDefault("ENVIRONMENT", "local")),
			attribute.String("service.instance.id", envOrDefault("HOSTNAME", "unknown")),
		),
	)
	if err != nil {
		return noop, fmt.Errorf("building resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(provider)

	// W3C tracecontext is what lets the processor join this trace rather than
	// starting its own.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("Tracing enabled: exporting to %s as %q", endpoint, cfg.ServiceName)
	return provider.Shutdown, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
