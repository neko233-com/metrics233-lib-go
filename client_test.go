package metrics233_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metrics "github.com/neko233-com/metrics233-lib-go"
)

type packet struct {
	Samples []metrics.Sample `json:"samples"`
	body    []byte
}

func readPacket(t *testing.T, r *http.Request) packet {
	t.Helper()
	data, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read request: %v", err)
	}
	var result packet
	if err := json.Unmarshal(data, &result); err != nil {
		t.Errorf("decode request: %v", err)
	}
	result.body = data
	return result
}

func ack(w http.ResponseWriter, count int) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"accepted": count})
}

func mustClient(t *testing.T, endpoint string, options ...metrics.Option) *metrics.Client {
	t.Helper()
	client, err := metrics.New(endpoint, options...)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func token(t *testing.T) string {
	t.Helper()
	var data [24]byte
	if _, err := rand.Read(data[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(data[:])
}

func TestFluentMetricsScopesBatchAndCumulativeSnapshots(t *testing.T) {
	requests := make(chan packet, 8)
	secret := token(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/monitoring/api/v1/ingest" || r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Authorization") != "Bearer "+secret {
			t.Error("incorrect native endpoint/method/auth/content type")
		}
		p := readPacket(t, r)
		requests <- p
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL+"/monitoring/", metrics.WithToken(secret))
	labels := metrics.Labels{"service": "shop", "environment": "test"}
	scope := client.WithLabels(labels)
	labels["service"] = "mutated"
	orders := scope.Counter("business_orders_total")
	must(t, orders.Add(2))
	must(t, scope.Gauge("business_active_users").Set(42))
	batch := scope.Batch().Counter("business_orders_total", 3).Gauge("business_active_users", 43)
	must(t, batch.Commit())
	if !errors.Is(batch.Commit(), metrics.ErrBatchCommitted) {
		t.Fatal("batch commit must not apply a second time")
	}
	select {
	case <-requests:
		t.Fatal("construction/recording performed unexpected I/O")
	default:
	}
	for iteration := 0; iteration < 2; iteration++ {
		must(t, client.Push(context.Background()))
		p := <-requests
		if len(p.Samples) != 2 {
			t.Fatalf("expected 2 managed series, got %d", len(p.Samples))
		}
		for _, point := range p.Samples {
			if point.Labels["service"] != "shop" || point.Timestamp <= 0 {
				t.Fatal("labels were mutated or timestamp was not fixed")
			}
			want := float64(43)
			if strings.HasSuffix(point.Name, "_total") {
				want = 5
			}
			if point.Value != want {
				t.Fatalf("cumulative snapshot %s = %v, want %v", point.Name, point.Value, want)
			}
		}
	}
	if client.Stats().Series != 2 {
		t.Fatal("shared scopes did not share registry")
	}
}

func TestFailedDeliveryRetainsBodyAndCounterUpdates(t *testing.T) {
	requests := make(chan packet, 8)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := readPacket(t, r)
		requests <- p
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL, metrics.WithRetry(0, 0), metrics.WithLimits(metrics.Limits{MaxPending: 1}))
	counter := client.Counter("orders_total")
	must(t, counter.Add(10))
	raw := metrics.Sample{Name: "queue_depth", Labels: metrics.Labels{"region": "test"}, Value: 3}
	must(t, client.Record(raw))
	raw.Labels["region"] = "mutated"
	if err := client.Push(context.Background()); err == nil {
		t.Fatal("expected retryable failure")
	}
	first := <-requests
	if client.Stats().PendingSamples != 1 || client.Stats().InFlightSamples != 2 {
		t.Fatal("failed delivery was discarded")
	}
	if err := client.Record(metrics.Sample{Name: "other", Value: 1}); !errors.Is(err, metrics.ErrQueueFull) {
		t.Fatal("in-flight raw samples must reserve queue capacity")
	}
	must(t, counter.Add(5))
	must(t, client.Push(context.Background()))
	retry, current := <-requests, <-requests
	if !bytes.Equal(first.body, retry.body) {
		t.Fatal("retry changed the original body or timestamps")
	}
	for _, point := range retry.Samples {
		if point.Name == "queue_depth" && point.Labels["region"] != "test" {
			t.Fatal("queued labels were not owned")
		}
	}
	if len(current.Samples) != 1 || current.Samples[0].Value != 15 || client.Stats().PendingSamples != 0 {
		t.Fatal("counter increments during failure were lost")
	}
}

func TestRetryAfterAndFiniteRetryBudget(t *testing.T) {
	requests := make(chan packet, 8)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := readPacket(t, r)
		requests <- p
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL, metrics.WithRetry(1, 0), metrics.WithTimeout(3*time.Second))
	must(t, client.Counter("requests_total").Inc())
	start := time.Now()
	must(t, client.Push(context.Background()))
	first, second := <-requests, <-requests
	if time.Since(start) < 950*time.Millisecond || !bytes.Equal(first.body, second.body) || calls.Load() != 2 {
		t.Fatal("Retry-After/body stability/retry count not respected")
	}
	var failures atomic.Int32
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failures.Add(1)
		w.WriteHeader(503)
	}))
	defer failing.Close()
	client = mustClient(t, failing.URL, metrics.WithRetry(2, 0))
	must(t, client.Gauge("active").Set(1))
	err := client.Push(context.Background())
	var status *metrics.HTTPError
	if !errors.As(err, &status) || status.StatusCode != 503 || failures.Load() != 3 {
		t.Fatal("retry budget is not finite and exact")
	}
}

func TestRetryWaitAndNetworkRequestsRespectCancellation(t *testing.T) {
	arrived := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		w.Header().Set("Retry-After", "9999999999999999999")
		w.WriteHeader(429)
	}))
	defer server.Close()
	client := mustClient(t, server.URL, metrics.WithTimeout(50*time.Millisecond))
	must(t, client.Gauge("active").Set(1))
	start := time.Now()
	if err := client.Push(context.Background()); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("bounded batch timeout failed: %v", err)
	}
	<-arrived
	if client.Stats().InFlightSamples != 1 {
		t.Fatal("timeout discarded pending delivery")
	}
	entered, cancelled := make(chan struct{}), make(chan struct{})
	blocking := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer blocking.Close()
	client = mustClient(t, blocking.URL)
	must(t, client.Gauge("active").Set(1))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Push(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("request cancellation: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("HTTP request context was not canceled")
	}
}

func TestBatchCapacityTypesAndOverflowAreAtomic(t *testing.T) {
	requests := make(chan packet, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := readPacket(t, r)
		requests <- p
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL, metrics.WithLimits(metrics.Limits{MaxSeries: 2, MaxPending: 1}))
	must(t, client.Counter("orders_total").Add(5))
	err := client.Batch().Counter("orders_total", 3).Gauge("new_gauge", 1).Gauge("third_series", 1).Commit()
	if !errors.Is(err, metrics.ErrSeriesLimit) || client.Stats().Series != 1 {
		t.Fatal("series capacity rejection mutated a prefix")
	}
	err = client.Batch().Counter("orders_total", math.MaxFloat64).Counter("orders_total", math.MaxFloat64).Commit()
	if !errors.Is(err, metrics.ErrInvalidSample) {
		t.Fatal("counter overflow was not rejected")
	}
	err = client.WithLabels(metrics.Labels{"other": "series"}).Gauge("orders_total").Set(3)
	if !errors.Is(err, metrics.ErrTypeConflict) {
		t.Fatal("one metric name must retain its recording type")
	}
	must(t, client.Push(context.Background()))
	if p := <-requests; len(p.Samples) != 1 || p.Samples[0].Value != 5 {
		t.Fatal("failed atomic batch changed cumulative counter")
	}
}

func TestHTTPBatchCountAndEncodedBodyBudgets(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(fmt.Sprint(large), func(t *testing.T) {
			var calls, accepted atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				p := readPacket(t, r)
				if len(p.body) > 4*1024*1024 || (!large && len(p.Samples) > 2) {
					t.Error("HTTP count/encoded byte budget exceeded")
				}
				calls.Add(1)
				accepted.Add(int32(len(p.Samples)))
				ack(w, len(p.Samples))
			}))
			defer server.Close()
			batchSize, count := 2, 5
			if large {
				batchSize, count = 10000, 60
			}
			client := mustClient(t, server.URL, metrics.WithLimits(metrics.Limits{BatchSize: batchSize}))
			points := make([]metrics.Sample, count)
			for index := range points {
				labels := metrics.Labels{"index": fmt.Sprint(index)}
				if large {
					for key := 0; key < 31; key++ {
						labels[fmt.Sprintf("label_%d", key)] = strings.Repeat("<", 512)
					}
				}
				points[index] = metrics.Sample{Name: "raw_queue_depth", Labels: labels, Value: float64(index)}
			}
			must(t, client.Record(points...))
			must(t, client.Push(context.Background()))
			if accepted.Load() != int32(count) || calls.Load() < 2 || (!large && calls.Load() != 3) {
				t.Fatal("batch splitting lost samples")
			}
		})
	}
}

func TestRedirectPermanentErrorAndAcknowledgementSafety(t *testing.T) {
	var leaked atomic.Int32
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer sink.Close()
	secret := token(t)
	for _, status := range []int{307, 400, 401, 200} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", sink.URL)
				w.WriteHeader(status)
				if status == 200 {
					ack(w, 0)
				} else {
					_, _ = io.WriteString(w, secret)
				}
			}))
			defer server.Close()
			client := mustClient(t, server.URL, metrics.WithToken(secret), metrics.WithRetry(2, 0))
			must(t, client.Counter("orders_total").Inc())
			err := client.Push(context.Background())
			if err == nil || strings.Contains(err.Error(), secret) || calls.Load() != 1 || client.Stats().InFlightSamples != 1 {
				t.Fatal("permanent error/acknowledgement handling leaked, retried or dropped data")
			}
		})
	}
	if leaked.Load() != 0 {
		t.Fatal("redirect forwarded a request/credential to another endpoint")
	}
}

func TestValidationAndQueueRejectWithoutPartialMutation(t *testing.T) {
	client := mustClient(t, "http://127.0.0.1:7333", metrics.WithLimits(metrics.Limits{MaxPending: 1}))
	for _, bad := range []metrics.Sample{
		{Name: "invalid-name", Value: 1},
		{Name: "valid", Labels: metrics.Labels{"__name__": "reserved"}, Value: 1},
		{Name: "valid", Labels: metrics.Labels{"service": strings.Repeat("x", 513)}, Value: 1},
		{Name: "valid", Labels: metrics.Labels{"service": string([]byte{0xff})}, Value: 1},
		{Name: "valid", Value: math.Inf(1)},
		{Name: "valid", Value: 1, Timestamp: time.Now().Add(6 * time.Minute).UnixMilli()},
	} {
		if !errors.Is(client.Record(bad), metrics.ErrInvalidSample) || client.Stats().Series != 0 {
			t.Fatal("invalid sample mutated registry")
		}
	}
	if !errors.Is(client.Counter("orders_total").Add(-1), metrics.ErrInvalidSample) {
		t.Fatal("negative cumulative counter accepted")
	}
	must(t, client.Record(metrics.Sample{Name: "raw", Value: 1}))
	if !errors.Is(client.Record(metrics.Sample{Name: "other", Value: 1}), metrics.ErrQueueFull) || client.Stats().Series != 1 {
		t.Fatal("queue capacity rejection registered another series")
	}
	labels := make(metrics.Labels)
	for index := 0; index < 33; index++ {
		labels[fmt.Sprintf("key_%d", index)] = "value"
	}
	if !errors.Is(client.WithLabels(labels).Gauge("bad_scope").Set(1), metrics.ErrInvalidSample) {
		t.Fatal("label count limit was not enforced")
	}
	for _, endpoint := range []string{"", "file:///tmp/metrics", "https://user:pass@metrics.example.com", "https://metrics.example.com?token=private", "https://metrics.example.com/#fragment", "https://:7333", "https://metrics.example.com/?"} {
		if _, err := metrics.New(endpoint); err == nil {
			t.Fatal("unsafe endpoint configuration accepted")
		}
	}
	for _, option := range []metrics.Option{metrics.WithRetry(6, 0), metrics.WithTimeout(0), metrics.WithLimits(metrics.Limits{BatchSize: 10001}), metrics.WithToken("invalid\nheader"), metrics.WithToken("invalid\x00header"), metrics.WithToken("invalid\x7fheader")} {
		if _, err := metrics.New("https://metrics.example.com", option); err == nil {
			t.Fatal("invalid client option accepted")
		}
	}
}

func TestConcurrentRecordingAndPushRetainEveryCounterIncrement(t *testing.T) {
	var final atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := readPacket(t, r)
		for _, point := range p.Samples {
			if point.Name == "orders_total" {
				final.Store(int64(point.Value))
			}
		}
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL)
	counter := client.WithLabels(metrics.Labels{"service": "shop"}).Counter("orders_total")
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := 0; index < 200; index++ {
				if err := counter.Inc(); err != nil {
					t.Errorf("increment: %v", err)
				}
				if index%50 == 0 {
					if err := client.Push(context.Background()); err != nil {
						t.Errorf("push: %v", err)
					}
				}
			}
		}()
	}
	workers.Wait()
	must(t, client.Push(context.Background()))
	if final.Load() != 3200 || client.Stats().Series != 1 {
		t.Fatalf("counter lost increments: %d", final.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func TestCustomTLSClientAndNetworkRetryDoNotExposeTransportErrors(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := readPacket(t, r)
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL, metrics.WithHTTPClient(server.Client()))
	must(t, client.Counter("orders_total").Inc())
	must(t, client.Push(context.Background()))
	secret := token(t)
	var calls atomic.Int32
	custom := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New(secret)
	})}
	client = mustClient(t, "https://metrics.example.com", metrics.WithHTTPClient(custom), metrics.WithRetry(1, 0))
	must(t, client.Gauge("active").Set(1))
	err := client.Push(context.Background())
	if err == nil || strings.Contains(err.Error(), secret) || calls.Load() != 2 || custom.CheckRedirect != nil {
		t.Fatal("custom client was mutated or transport diagnostics leaked")
	}
}
