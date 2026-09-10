package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func testConfig(endpoint string) Config {
	return Config{
		Port:              "8080",
		MetricsPort:       "9090",
		DataRate:          50,
		BatchSize:         25,
		FlushInterval:     time.Second,
		ProcessorEndpoint: endpoint,
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	for _, k := range []string{"PORT", "METRICS_PORT", "DATA_RATE", "BATCH_SIZE", "FLUSH_INTERVAL_MS", "PROCESSOR_ENDPOINT"} {
		t.Setenv(k, "")
	}

	c := loadConfig()

	if c.Port != "8080" || c.MetricsPort != "9090" {
		t.Errorf("ports = %s/%s, want 8080/9090", c.Port, c.MetricsPort)
	}
	if c.DataRate != 100 || c.BatchSize != 50 {
		t.Errorf("rate/batch = %d/%d, want 100/50", c.DataRate, c.BatchSize)
	}
	if c.FlushInterval != time.Second {
		t.Errorf("flush interval = %v, want 1s", c.FlushInterval)
	}
	if c.ProcessorEndpoint != "http://data-processor:8080/ingest" {
		t.Errorf("endpoint = %q", c.ProcessorEndpoint)
	}
}

func TestLoadConfigReadsEnvironment(t *testing.T) {
	t.Setenv("PORT", "9999")
	t.Setenv("DATA_RATE", "7")
	t.Setenv("BATCH_SIZE", "3")
	t.Setenv("FLUSH_INTERVAL_MS", "250")
	t.Setenv("PROCESSOR_ENDPOINT", "http://example.invalid/ingest")

	c := loadConfig()

	if c.Port != "9999" || c.DataRate != 7 || c.BatchSize != 3 {
		t.Errorf("config not read from env: %+v", c)
	}
	if c.FlushInterval != 250*time.Millisecond {
		t.Errorf("flush interval = %v, want 250ms", c.FlushInterval)
	}
}

func TestGenerateEventPopulatesRequiredFields(t *testing.T) {
	g := NewGenerator(testConfig("http://example.invalid/ingest"))

	seenTypes := map[string]bool{}
	for i := 0; i < 200; i++ {
		e := g.generateEvent()

		if e.ID == "" {
			t.Fatal("event ID is empty")
		}
		if e.Timestamp.IsZero() {
			t.Fatal("event timestamp is zero")
		}
		if e.Type == "" || e.Source == "" {
			t.Fatalf("event missing type/source: %+v", e)
		}
		if len(e.Data) == 0 {
			t.Fatalf("event %s has no data payload", e.Type)
		}
		seenTypes[e.Type] = true
	}

	for _, want := range []string{"metric", "log", "trace", "alert", "audit", "heartbeat"} {
		if !seenTypes[want] {
			t.Errorf("never generated event type %q in 200 draws", want)
		}
	}
}

func TestGenerateEventIDsAreUnique(t *testing.T) {
	g := NewGenerator(testConfig("http://example.invalid/ingest"))

	seen := make(map[string]bool)
	for i := 0; i < 500; i++ {
		id := g.generateEvent().ID
		if seen[id] {
			t.Fatalf("duplicate event ID: %s", id)
		}
		seen[id] = true
	}
}

// The batch body used to be passed as a json.RawMessage, which is not an
// io.Reader, so this path did not compile and no batch was ever sent.
func TestFlushBufferPostsBatchAsJSON(t *testing.T) {
	type received struct {
		contentType string
		events      []DataEvent
	}
	got := make(chan received, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading body: %v", err)
		}
		var events []DataEvent
		if err := json.Unmarshal(body, &events); err != nil {
			t.Errorf("body is not a JSON array of events: %v", err)
		}
		got <- received{contentType: r.Header.Get("Content-Type"), events: events}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	g := NewGenerator(testConfig(srv.URL + "/ingest"))
	for i := 0; i < 3; i++ {
		g.addToBuffer(g.generateEvent())
	}

	g.flushBuffer()

	select {
	case r := <-got:
		if r.contentType != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", r.contentType)
		}
		if len(r.events) != 3 {
			t.Errorf("received %d events, want 3", len(r.events))
		}
		if r.events[0].ID == "" || r.events[0].Type == "" {
			t.Errorf("event did not survive the round trip: %+v", r.events[0])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("processor never received the batch")
	}

	if n := len(g.buffer); n != 0 {
		t.Errorf("buffer holds %d events after flush, want 0", n)
	}
}

func TestFlushBufferOnEmptyBufferDoesNotCallProcessor(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	g := NewGenerator(testConfig(srv.URL + "/ingest"))
	g.flushBuffer()

	if called {
		t.Error("flushBuffer called the processor with an empty buffer")
	}
}

func TestFlushBufferSurvivesProcessorError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	g := NewGenerator(testConfig(srv.URL + "/ingest"))
	g.addToBuffer(g.generateEvent())

	g.flushBuffer() // must not panic

	if n := len(g.buffer); n != 0 {
		t.Errorf("buffer holds %d events after a failed flush, want 0", n)
	}
}

func TestHandleHealthReturnsJSON(t *testing.T) {
	g := NewGenerator(testConfig("http://example.invalid/ingest"))
	rec := httptest.NewRecorder()

	g.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if body["status"] != "healthy" {
		t.Errorf("status = %v, want healthy", body["status"])
	}
}

func TestHandleReady(t *testing.T) {
	g := NewGenerator(testConfig("http://example.invalid/ingest"))
	rec := httptest.NewRecorder()

	g.handleReady(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "ready" {
		t.Errorf("body = %q, want ready", rec.Body.String())
	}
}

func TestHandleStatsReportsConfig(t *testing.T) {
	g := NewGenerator(testConfig("http://example.invalid/ingest"))
	g.addToBuffer(g.generateEvent())
	rec := httptest.NewRecorder()

	g.handleStats(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if body["buffer_size"].(float64) != 1 {
		t.Errorf("buffer_size = %v, want 1", body["buffer_size"])
	}
	if body["data_rate"].(float64) != 50 {
		t.Errorf("data_rate = %v, want 50", body["data_rate"])
	}
}

func TestHandleGenerateRespectsCount(t *testing.T) {
	g := NewGenerator(testConfig("http://example.invalid/ingest"))

	cases := map[string]int{
		"?count=5":    5,
		"?count=0":    1,    // non-positive falls back to 1
		"":            1,    // missing falls back to 1
		"?count=5000": 1000, // capped
	}

	for query, want := range cases {
		rec := httptest.NewRecorder()
		g.handleGenerate(rec, httptest.NewRequest(http.MethodGet, "/generate"+query, nil))

		var events []DataEvent
		if err := json.Unmarshal(rec.Body.Bytes(), &events); err != nil {
			t.Fatalf("%q: response is not JSON: %v", query, err)
		}
		if len(events) != want {
			t.Errorf("%q: got %d events, want %d", query, len(events), want)
		}
	}
}

func TestGenerateEventCarriesEnvironmentMetadata(t *testing.T) {
	t.Setenv("REGION", "oci-ashburn")
	t.Setenv("ENVIRONMENT", "production")
	os.Setenv("HOSTNAME", "data-generator-abc")
	defer os.Unsetenv("HOSTNAME")

	e := NewGenerator(testConfig("http://example.invalid/ingest")).generateEvent()

	if e.Metadata["region"] != "oci-ashburn" {
		t.Errorf("region = %q", e.Metadata["region"])
	}
	if e.Metadata["environment"] != "production" {
		t.Errorf("environment = %q", e.Metadata["environment"])
	}
	if e.Metadata["generator_id"] != "data-generator-abc" {
		t.Errorf("generator_id = %q", e.Metadata["generator_id"])
	}
}

func TestLoadConfigTracingDefaults(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SERVICE_NAME", "")

	c := loadConfig()

	if c.OTLPEndpoint != "" {
		t.Errorf("OTLPEndpoint = %q, want empty so tracing stays off", c.OTLPEndpoint)
	}
	if c.ServiceName != "data-generator" {
		t.Errorf("ServiceName = %q, want data-generator", c.ServiceName)
	}
}

func TestLoadConfigReadsTracingEnvironment(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger-collector:4318")
	t.Setenv("OTEL_SERVICE_NAME", "custom-name")

	c := loadConfig()

	if c.OTLPEndpoint != "http://jaeger-collector:4318" {
		t.Errorf("OTLPEndpoint = %q", c.OTLPEndpoint)
	}
	if c.ServiceName != "custom-name" {
		t.Errorf("ServiceName = %q, want custom-name", c.ServiceName)
	}
}

// Without a collector configured the service must still start, with spans
// becoming no-ops rather than erroring or blocking on export.
func TestInitTracingDisabledWithoutEndpoint(t *testing.T) {
	cfg := testConfig("http://example.invalid/ingest")
	cfg.OTLPEndpoint = ""

	shutdown, err := initTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("initTracing returned error when disabled: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown func is nil")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown returned error: %v", err)
	}
}

func TestFlushBufferWorksWithTracingDisabled(t *testing.T) {
	got := make(chan int, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var events []DataEvent
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &events)
		got <- len(events)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	g := NewGenerator(testConfig(srv.URL + "/ingest"))
	g.addToBuffer(g.generateEvent())
	g.addToBuffer(g.generateEvent())

	g.flushBuffer()

	select {
	case n := <-got:
		if n != 2 {
			t.Errorf("received %d events, want 2", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("processor never received the batch")
	}
}
