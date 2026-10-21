package metrics233_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	metrics "github.com/neko233-com/metrics233-lib-go"
)

func TestExplicitRunCancelAndRetryableClose(t *testing.T) {
	seen := make(chan struct{}, 32)
	var failing atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := readPacket(t, r)
		seen <- struct{}{}
		if failing.Load() {
			w.WriteHeader(503)
			return
		}
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL, metrics.WithRetry(0, 0))
	must(t, client.Counter("orders_total").Inc())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx, 10*time.Millisecond, nil) }()
	<-seen
	if err := client.Run(ctx, time.Second, nil); !errors.Is(err, metrics.ErrAlreadyRunning) {
		t.Fatal("duplicate periodic loop was allowed")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || client.Stats().Running {
		t.Fatal("Run did not release its explicit lifecycle")
	}
	must(t, client.Counter("orders_total").Inc())
	failing.Store(true)
	if err := client.Close(context.Background()); err == nil || !client.Stats().Closing || client.Stats().Closed {
		t.Fatal("failed Close did not preserve a retryable state")
	}
	if !errors.Is(client.Counter("orders_total").Inc(), metrics.ErrClosed) {
		t.Fatal("Close allowed new recordings")
	}
	failing.Store(false)
	must(t, client.Close(context.Background()))
	must(t, client.Close(context.Background()))
	if !client.Stats().Closed || client.Stats().InFlightSamples != 0 {
		t.Fatal("Close did not finish retained data")
	}
}

func TestRunReportsFailureAndContinuesNextInterval(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := readPacket(t, r)
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL, metrics.WithRetry(0, 0))
	must(t, client.Counter("orders_total").Inc())
	errorsSeen := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx, 10*time.Millisecond, func(err error) { errorsSeen <- err }) }()
	select {
	case <-errorsSeen:
	case <-time.After(time.Second):
		t.Fatal("Run did not report its failed push")
	}
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || calls.Load() < 3 {
		t.Fatal("Run did not recover retained and current cumulative snapshots")
	}
}

func TestCloseStopsRun(t *testing.T) {
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := readPacket(t, r)
		if r.Header.Get("X-Unused") != "" {
			t.Error("unexpected header")
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL)
	must(t, client.Gauge("active").Set(2))
	runResult := make(chan error, 1)
	go func() { runResult <- client.Run(context.Background(), time.Hour, nil) }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, client.Close(ctx))
	if err := <-runResult; !errors.Is(err, context.Canceled) {
		t.Fatal("Close did not cancel Run")
	}
	if client.Stats().Running || !client.Stats().Closed {
		t.Fatal("Close returned with periodic work still running")
	}
}

func TestCancellationInterruptsConcurrentPushGateWait(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := readPacket(t, r)
		close(entered)
		<-release
		ack(w, len(p.Samples))
	}))
	defer server.Close()
	client := mustClient(t, server.URL)
	must(t, client.Gauge("active").Set(2))
	first := make(chan error, 1)
	go func() { first <- client.Push(context.Background()) }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := client.Push(ctx)
	close(release)
	must(t, <-first)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiting Push did not honor its own cancellation")
	}
}

func TestRetryAfterSurvivesSeparatePushCalls(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
	}))
	defer server.Close()
	client := mustClient(t, server.URL, metrics.WithRetry(0, 0), metrics.WithTimeout(20*time.Millisecond))
	must(t, client.Gauge("active").Set(2))
	if err := client.Push(context.Background()); err == nil {
		t.Fatal("429 was not reported")
	}
	if err := client.Push(context.Background()); !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatal("separate Push retried before the server Retry-After window")
	}
}
