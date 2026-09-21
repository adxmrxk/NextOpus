// NextOpus Data Generator Service
// High-throughput data generation microservice written in Go
// Generates metrics, events, and test data for the platform

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Configuration from environment variables
type Config struct {
	Port              string
	DataRate          int // Events per second
	MetricsPort       string
	ProcessorEndpoint string
	BatchSize         int
	FlushInterval     time.Duration
	OTLPEndpoint      string // empty disables tracing
	ServiceName       string
}

// DataEvent represents a generated data event
type DataEvent struct {
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Type      string                 `json:"type"`
	Source    string                 `json:"source"`
	Data      map[string]interface{} `json:"data"`
	Metadata  map[string]string      `json:"metadata"`
}

// Generator handles data generation
type Generator struct {
	config       Config
	eventCounter uint64
	buffer       []DataEvent
	bufferMu     sync.Mutex
	httpClient   *http.Client
	eventTypes   []string
	sources      []string
	// Capacity-1 channel used as a try-lock, so at most one flush runs at a
	// time. Previously every full batch spawned a goroutine, which under load
	// meant unbounded goroutines all contending for the same buffer.
	flushing chan struct{}
}

// Prometheus metrics
var (
	eventsGenerated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nextopus_events_generated_total",
			Help: "Total number of events generated",
		},
		[]string{"type", "source"},
	)

	eventsSent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "nextopus_events_sent_total",
			Help: "Total number of events sent to processor",
		},
	)

	sendErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "nextopus_send_errors_total",
			Help: "Total number of send errors",
		},
	)

	batchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "nextopus_batch_size",
			Help:    "Size of event batches sent",
			Buckets: prometheus.LinearBuckets(10, 10, 10),
		},
	)

	generationLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "nextopus_generation_latency_seconds",
			Help:    "Time to generate events",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10),
		},
	)

	bufferUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "nextopus_buffer_utilization",
			Help: "Current buffer utilization",
		},
	)

	healthStatus = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "nextopus_generator_health",
			Help: "Health status (1 = healthy, 0 = unhealthy)",
		},
	)
)

func loadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9090"
	}

	dataRate, _ := strconv.Atoi(os.Getenv("DATA_RATE"))
	if dataRate == 0 {
		dataRate = 100 // Default 100 events per second
	}

	batchSize, _ := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if batchSize == 0 {
		batchSize = 50
	}

	flushIntervalMs, _ := strconv.Atoi(os.Getenv("FLUSH_INTERVAL_MS"))
	if flushIntervalMs == 0 {
		flushIntervalMs = 1000
	}

	processorEndpoint := os.Getenv("PROCESSOR_ENDPOINT")
	if processorEndpoint == "" {
		processorEndpoint = "http://data-processor:8080/ingest"
	}

	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "data-generator"
	}

	return Config{
		Port:              port,
		DataRate:          dataRate,
		MetricsPort:       metricsPort,
		ProcessorEndpoint: processorEndpoint,
		BatchSize:         batchSize,
		FlushInterval:     time.Duration(flushIntervalMs) * time.Millisecond,
		OTLPEndpoint:      os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		ServiceName:       serviceName,
	}
}

func NewGenerator(config Config) *Generator {
	return &Generator{
		config:   config,
		buffer:   make([]DataEvent, 0, config.BatchSize*2),
		flushing: make(chan struct{}, 1),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			// otelhttp injects W3C tracecontext headers on every request, which
			// is what lets the processor continue this trace instead of
			// starting a disconnected one.
			Transport: otelhttp.NewTransport(&http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			}),
		},
		eventTypes: []string{
			"metric", "log", "trace", "alert", "audit", "heartbeat",
		},
		sources: []string{
			"api-gateway", "auth-service", "payment-service",
			"inventory-service", "notification-service", "analytics-engine",
		},
	}
}

func (g *Generator) generateEvent() DataEvent {
	eventType := g.eventTypes[rand.Intn(len(g.eventTypes))]
	source := g.sources[rand.Intn(len(g.sources))]
	eventID := atomic.AddUint64(&g.eventCounter, 1)

	data := make(map[string]interface{})

	switch eventType {
	case "metric":
		data["cpu_usage"] = rand.Float64() * 100
		data["memory_usage"] = rand.Float64() * 100
		data["disk_io"] = rand.Float64() * 1000
		data["network_rx"] = rand.Intn(1000000)
		data["network_tx"] = rand.Intn(1000000)
	case "log":
		levels := []string{"DEBUG", "INFO", "WARN", "ERROR"}
		data["level"] = levels[rand.Intn(len(levels))]
		data["message"] = fmt.Sprintf("Log message from %s at %d", source, time.Now().Unix())
		data["correlation_id"] = fmt.Sprintf("corr-%d", rand.Intn(10000))
	case "trace":
		data["trace_id"] = fmt.Sprintf("trace-%d", rand.Intn(100000))
		data["span_id"] = fmt.Sprintf("span-%d", rand.Intn(1000))
		data["duration_ms"] = rand.Float64() * 500
		data["status_code"] = []int{200, 201, 400, 404, 500}[rand.Intn(5)]
	case "alert":
		severities := []string{"low", "medium", "high", "critical"}
		data["severity"] = severities[rand.Intn(len(severities))]
		data["title"] = fmt.Sprintf("Alert from %s", source)
		data["resolved"] = rand.Float32() > 0.7
	case "audit":
		actions := []string{"create", "read", "update", "delete", "login", "logout"}
		data["action"] = actions[rand.Intn(len(actions))]
		data["user_id"] = fmt.Sprintf("user-%d", rand.Intn(1000))
		data["resource"] = fmt.Sprintf("/api/v1/resource/%d", rand.Intn(10000))
	case "heartbeat":
		data["status"] = "alive"
		data["uptime_seconds"] = rand.Intn(86400)
		data["version"] = "1.0.0"
	}

	return DataEvent{
		ID:        fmt.Sprintf("evt-%d-%d", time.Now().UnixNano(), eventID),
		Timestamp: time.Now().UTC(),
		Type:      eventType,
		Source:    source,
		Data:      data,
		Metadata: map[string]string{
			"generator_id": os.Getenv("HOSTNAME"),
			"region":       os.Getenv("REGION"),
			"environment":  os.Getenv("ENVIRONMENT"),
		},
	}
}

// addToBuffer appends an event and returns the buffer length observed under
// the lock. Callers must use this return value rather than reading
// len(g.buffer) themselves, which races with flushBuffer truncating it.
func (g *Generator) addToBuffer(event DataEvent) int {
	g.bufferMu.Lock()
	defer g.bufferMu.Unlock()
	g.buffer = append(g.buffer, event)
	n := len(g.buffer)
	bufferUtilization.Set(float64(n))
	return n
}

// triggerFlush starts a flush unless one is already running. A dropped trigger
// is harmless: the in-flight flush drains whatever is in the buffer, and the
// periodic ticker catches anything that arrives after it.
func (g *Generator) triggerFlush() {
	select {
	case g.flushing <- struct{}{}:
		go func() {
			defer func() { <-g.flushing }()
			g.flushBuffer()
		}()
	default:
	}
}

func (g *Generator) flushBuffer() {
	g.bufferMu.Lock()
	if len(g.buffer) == 0 {
		g.bufferMu.Unlock()
		return
	}

	batch := make([]DataEvent, len(g.buffer))
	copy(batch, g.buffer)
	g.buffer = g.buffer[:0]
	g.bufferMu.Unlock()

	batchSize.Observe(float64(len(batch)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Root span for this batch. Everything the processor does lands underneath
	// it, so one trace shows the whole journey across both services.
	ctx, span := otel.Tracer(tracerName).Start(ctx, "flush_batch",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.Int("batch.size", len(batch)),
			attribute.String("processor.endpoint", g.config.ProcessorEndpoint),
		),
	)
	defer span.End()

	jsonData, err := json.Marshal(batch)
	if err != nil {
		log.Printf("Error marshaling batch: %v", err)
		sendErrors.Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal failed")
		return
	}
	span.SetAttributes(attribute.Int("batch.bytes", len(jsonData)))

	req, err := http.NewRequestWithContext(ctx, "POST", g.config.ProcessorEndpoint,
		bytes.NewReader(jsonData))
	if err != nil {
		log.Printf("Error creating request: %v", err)
		sendErrors.Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, "request build failed")
		return
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		log.Printf("Error sending batch: %v", err)
		sendErrors.Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, "send failed")
		return
	}
	defer resp.Body.Close()

	span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		eventsSent.Add(float64(len(batch)))
		span.SetStatus(codes.Ok, "")
	} else {
		log.Printf("Processor returned status %d", resp.StatusCode)
		sendErrors.Inc()
		span.SetStatus(codes.Error, fmt.Sprintf("processor returned %d", resp.StatusCode))
	}
}

func (g *Generator) Run(ctx context.Context) {
	// Calculate interval between events
	interval := time.Second / time.Duration(g.config.DataRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	flushTicker := time.NewTicker(g.config.FlushInterval)
	defer flushTicker.Stop()

	log.Printf("Starting generator: rate=%d/s, batch=%d, flush=%v",
		g.config.DataRate, g.config.BatchSize, g.config.FlushInterval)

	healthStatus.Set(1)

	for {
		select {
		case <-ctx.Done():
			log.Println("Generator shutting down...")
			g.flushBuffer() // Final flush
			return

		case <-ticker.C:
			start := time.Now()
			event := g.generateEvent()
			generationLatency.Observe(time.Since(start).Seconds())

			eventsGenerated.WithLabelValues(event.Type, event.Source).Inc()

			// Use the length returned under the lock; reading len(g.buffer)
			// here raced with flushBuffer truncating it.
			if n := g.addToBuffer(event); n >= g.config.BatchSize {
				g.triggerFlush()
			}

		case <-flushTicker.C:
			g.triggerFlush()
		}
	}
}

// HTTP Handlers

func (g *Generator) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC(),
		"events":    atomic.LoadUint64(&g.eventCounter),
	})
}

func (g *Generator) handleReady(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ready"))
}

func (g *Generator) handleStats(w http.ResponseWriter, r *http.Request) {
	g.bufferMu.Lock()
	bufferLen := len(g.buffer)
	g.bufferMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_events":  atomic.LoadUint64(&g.eventCounter),
		"buffer_size":   bufferLen,
		"data_rate":     g.config.DataRate,
		"batch_size":    g.config.BatchSize,
		"processor_url": g.config.ProcessorEndpoint,
	})
}

func (g *Generator) handleGenerate(w http.ResponseWriter, r *http.Request) {
	countStr := r.URL.Query().Get("count")
	count, _ := strconv.Atoi(countStr)
	if count <= 0 {
		count = 1
	}
	if count > 1000 {
		count = 1000
	}

	events := make([]DataEvent, count)
	for i := 0; i < count; i++ {
		events[i] = g.generateEvent()
		eventsGenerated.WithLabelValues(events[i].Type, events[i].Source).Inc()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

func main() {
	// Go 1.20+ seeds the global rand source automatically; rand.Seed is
	// deprecated (staticcheck SA1019).
	config := loadConfig()

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownTracing, err := initTracing(ctx, config)
	if err != nil {
		log.Printf("Tracing setup failed, continuing without it: %v", err)
	}

	generator := NewGenerator(config)

	// HTTP server for API
	mux := http.NewServeMux()
	mux.HandleFunc("/health", generator.handleHealth)
	mux.HandleFunc("/ready", generator.handleReady)
	mux.HandleFunc("/stats", generator.handleStats)
	mux.HandleFunc("/generate", generator.handleGenerate)

	apiServer := &http.Server{
		Addr:    ":" + config.Port,
		Handler: mux,
	}

	// Metrics server
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	metricsServer := &http.Server{
		Addr:    ":" + config.MetricsPort,
		Handler: metricsMux,
	}

	// Start servers
	go func() {
		log.Printf("API server starting on port %s", config.Port)
		if err := apiServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("API server error: %v", err)
		}
	}()

	go func() {
		log.Printf("Metrics server starting on port %s", config.MetricsPort)
		if err := metricsServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Metrics server error: %v", err)
		}
	}()

	// Start generator
	go generator.Run(ctx)

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Received shutdown signal")
	healthStatus.Set(0)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	apiServer.Shutdown(shutdownCtx)
	metricsServer.Shutdown(shutdownCtx)

	// Flush any spans still sitting in the batch processor.
	if err := shutdownTracing(shutdownCtx); err != nil {
		log.Printf("Tracing shutdown error: %v", err)
	}

	log.Println("Shutdown complete")
}
