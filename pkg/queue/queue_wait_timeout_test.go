/*
Copyright 2026 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace/noop"

	netstats "knative.dev/networking/pkg/http/stats"
	"knative.dev/pkg/observability/metrics/metricstest"
)

const (
	// shortQueueWait is a queue wait timeout that is meant to expire.
	shortQueueWait = 20 * time.Millisecond
	// longQueueWait is a queue wait timeout that is not meant to expire
	// before the test hands over the slot.
	longQueueWait = 500 * time.Millisecond
)

func TestBreakerInvalidQueueWaitTimeout(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected NewBreaker to panic on a negative QueueWaitTimeout")
		}
	}()
	NewBreaker(BreakerParams{QueueDepth: 1, MaxConcurrency: 1, InitialCapacity: 1, QueueWaitTimeout: -time.Second})
}

// holdSlot occupies one concurrency slot of b until the returned func is called.
func holdSlot(t *testing.T, b *Breaker) (release func()) {
	t.Helper()
	held, done, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- b.Maybe(context.Background(), func() {
			close(held)
			<-done
		})
	}()
	<-held
	return func() {
		t.Helper()
		close(done)
		if err := <-finished; err != nil {
			t.Error("Slot holder failed:", err)
		}
	}
}

// waitForInFlight waits until the breaker tracks want requests (queued + executing).
func waitForInFlight(t *testing.T, b *Breaker, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for b.InFlight() != want {
		if time.Now().After(deadline) {
			t.Fatalf("InFlight() = %d, want: %d", b.InFlight(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBreakerQueueWaitTimeout(t *testing.T) {
	b := NewBreaker(BreakerParams{
		QueueDepth: 1, MaxConcurrency: 1, InitialCapacity: 1, QueueWaitTimeout: shortQueueWait,
	})
	release := holdSlot(t, b)

	// The slot is taken, so this request queues and has to time out.
	start := time.Now()
	err := b.Maybe(context.Background(), func() {
		t.Error("The thunk of a request that timed out in the queue was executed")
	})
	if !errors.Is(err, ErrRequestQueueWaitTimeout) {
		t.Fatalf("Maybe() = %v, want: %v", err, ErrRequestQueueWaitTimeout)
	}
	if waited := time.Since(start); waited < shortQueueWait {
		t.Errorf("Maybe() returned after %v, before the %v timeout", waited, shortQueueWait)
	}

	// The timed out request gave back its place in the queue: only the slot
	// holder is tracked.
	if got, want := b.InFlight(), 1; got != want {
		t.Fatalf("InFlight() = %d, want: %d", got, want)
	}

	// With a queue depth of 1 the next request is only admitted to the queue
	// (rather than failing with ErrRequestQueueFull) if that place was freed,
	// and it must get the slot once it is released.
	queued := make(chan error, 1)
	go func() {
		queued <- b.Maybe(context.Background(), func() {})
	}()
	waitForInFlight(t, b, 2)
	release()
	if err := <-queued; err != nil {
		t.Errorf("Maybe() of the request queued after a timeout = %v, want no error", err)
	}

	// Nothing leaked: neither a queue place nor a concurrency slot.
	if got, want := b.InFlight(), 0; got != want {
		t.Errorf("InFlight() = %d, want: %d", got, want)
	}
	if !b.sem.tryAcquire() {
		t.Error("The concurrency slot was not released")
	}
}

func TestBreakerQueueWaitTimeoutDoesNotLimitExecution(t *testing.T) {
	b := NewBreaker(BreakerParams{
		QueueDepth: 1, MaxConcurrency: 1, InitialCapacity: 1, QueueWaitTimeout: longQueueWait,
	})
	release := holdSlot(t, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executed time.Duration
	finished := make(chan error, 1)
	go func() {
		finished <- b.Maybe(ctx, func() {
			// Run for much longer than the queue wait timeout.
			start := time.Now()
			select {
			case <-ctx.Done():
			case <-time.After(3 * longQueueWait):
			}
			executed = time.Since(start)
		})
	}()

	// Hand over the slot while the request is still waiting within the limit.
	waitForInFlight(t, b, 2)
	release()

	if err := <-finished; err != nil {
		t.Fatalf("Maybe() = %v, want no error", err)
	}
	if executed < 3*longQueueWait {
		t.Errorf("The thunk was interrupted after %v, want it to run for %v", executed, 3*longQueueWait)
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("The request context was affected by the queue wait timeout: %v", err)
	}
}

func TestBreakerQueueWaitTimeoutNoWait(t *testing.T) {
	b := NewBreaker(BreakerParams{
		QueueDepth: 1, MaxConcurrency: 1, InitialCapacity: 1, QueueWaitTimeout: shortQueueWait,
	})

	// A request that gets a slot right away isn't bound by the timeout either.
	if err := b.Maybe(context.Background(), func() {
		time.Sleep(3 * shortQueueWait)
	}); err != nil {
		t.Errorf("Maybe() = %v, want no error", err)
	}
}

func TestBreakerQueueWaitTimeoutKeepsContextErrors(t *testing.T) {
	b := NewBreaker(BreakerParams{
		QueueDepth: 2, MaxConcurrency: 1, InitialCapacity: 1, QueueWaitTimeout: time.Minute,
	})
	defer holdSlot(t, b)()

	// The caller going away while queued is still reported as such.
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		finished <- b.Maybe(ctx, func() {})
	}()
	waitForInFlight(t, b, 2)
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Errorf("Maybe() = %v, want: %v", err, context.Canceled)
	}

	// As is a deadline of the caller that is shorter than the queue wait timeout.
	ctx, cancel = context.WithTimeout(context.Background(), shortQueueWait)
	defer cancel()
	if err := b.Maybe(ctx, func() {}); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrRequestQueueWaitTimeout) {
		t.Errorf("Maybe() = %v, want: %v", err, context.DeadlineExceeded)
	}

	if got, want := b.InFlight(), 1; got != want {
		t.Errorf("InFlight() = %d, want: %d", got, want)
	}
}

// queueWaitTimeoutHandler returns a ProxyHandler that counts queue wait
// timeouts, along with the reader to assert the counter with.
func queueWaitTimeoutHandler(t *testing.T, b *Breaker, next http.Handler) (http.HandlerFunc, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	counter, err := NewQueueWaitTimeoutCounter(metric.NewMeterProvider(metric.WithReader(reader)))
	if err != nil {
		t.Fatal("NewQueueWaitTimeoutCounter() =", err)
	}
	h := ProxyHandler(noop.NewTracerProvider().Tracer("test"), b, netstats.NewRequestStats(time.Now()), next,
		WithQueueWaitTimeoutCounter(counter))
	return h, reader
}

func assertQueueWaitTimeouts(t *testing.T, reader *metric.ManualReader, want int64) {
	t.Helper()
	if want == 0 {
		// A counter that was never incremented isn't exported at all.
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatal("Collect() =", err)
		}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				t.Errorf("Unexpected metric %q: %v", m.Name, m.Data)
			}
		}
		return
	}
	metricstest.AssertMetrics(t, reader, metricstest.MetricsEqual(scopeName, metricdata.Metrics{
		Name:        "kn.serving.queue.wait.timeouts",
		Unit:        "{request}",
		Description: "Number of requests rejected because they waited too long in the queue for a concurrency slot",
		Data: metricdata.Sum[int64]{
			Temporality: metricdata.CumulativeTemporality,
			IsMonotonic: true,
			DataPoints:  []metricdata.DataPoint[int64]{{Value: want}},
		},
	}))
}

func TestHandlerQueueWaitTimeout(t *testing.T) {
	var served atomic.Int32
	seen := make(chan struct{})
	resp := make(chan struct{})
	blockHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		seen <- struct{}{}
		<-resp
	})
	breaker := NewBreaker(BreakerParams{
		QueueDepth: 1, MaxConcurrency: 1, InitialCapacity: 1, QueueWaitTimeout: shortQueueWait,
	})
	h, reader := queueWaitTimeoutHandler(t, breaker, blockHandler)

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "http://localhost:8081/time", nil))
		first <- rec
	}()
	// Wait until the first request holds the only slot.
	<-seen

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "http://localhost:8081/time", nil))
	if got, want := rec.Code, http.StatusServiceUnavailable; got != want {
		t.Fatalf("Code = %d, want: %d", got, want)
	}
	if got, want := rec.Body.String(), ErrRequestQueueWaitTimeout.Error(); !strings.Contains(got, want) {
		t.Errorf("Body = %q wanted to contain %q", got, want)
	}
	if got, notWant := rec.Body.String(), ErrRequestQueueFull.Error(); strings.Contains(got, notWant) {
		t.Errorf("Body = %q is not distinguishable from %q", got, notWant)
	}
	// The request never reached the user container and freed its queue place.
	if got, want := served.Load(), int32(1); got != want {
		t.Errorf("Requests that reached the next handler = %d, want: %d", got, want)
	}
	if got, want := breaker.InFlight(), 1; got != want {
		t.Errorf("InFlight() = %d, want: %d", got, want)
	}
	assertQueueWaitTimeouts(t, reader, 1)

	// The request holding the slot is unaffected.
	close(resp)
	if got, want := (<-first).Code, http.StatusOK; got != want {
		t.Errorf("Code of the request holding the slot = %d, want: %d", got, want)
	}
	if got, want := breaker.InFlight(), 0; got != want {
		t.Errorf("InFlight() = %d, want: %d", got, want)
	}
}

func TestHandlerQueueWaitTimeoutDoesNotLimitExecution(t *testing.T) {
	seen := make(chan struct{}, 2)
	resp := make(chan struct{})
	var secondCtxErr error
	var requests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- struct{}{}
		if requests.Add(1) == 1 {
			<-resp
			return
		}
		// The second request takes much longer than the queue wait timeout.
		select {
		case <-r.Context().Done():
		case <-time.After(3 * longQueueWait):
		}
		secondCtxErr = r.Context().Err()
	})
	breaker := NewBreaker(BreakerParams{
		QueueDepth: 1, MaxConcurrency: 1, InitialCapacity: 1, QueueWaitTimeout: longQueueWait,
	})
	h, reader := queueWaitTimeoutHandler(t, breaker, handler)

	go h(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "http://localhost:8081/time", nil))
	<-seen

	second := make(chan *httptest.ResponseRecorder, 1)
	var took time.Duration
	go func() {
		rec := httptest.NewRecorder()
		start := time.Now()
		h(rec, httptest.NewRequest(http.MethodGet, "http://localhost:8081/time", nil))
		took = time.Since(start)
		second <- rec
	}()

	// Free the slot while the second request is queued, within the limit.
	waitForInFlight(t, breaker, 2)
	close(resp)

	rec := <-second
	if got, want := rec.Code, http.StatusOK; got != want {
		t.Errorf("Code = %d, want: %d, body: %q", got, want, rec.Body.String())
	}
	if secondCtxErr != nil {
		t.Errorf("The request context was affected by the queue wait timeout: %v", secondCtxErr)
	}
	if took < 3*longQueueWait {
		t.Errorf("The request finished after %v, want it to run for at least %v", took, 3*longQueueWait)
	}
	assertQueueWaitTimeouts(t, reader, 0)
}

func TestHandlerQueueWaitTimeoutKeepsContextDeadline(t *testing.T) {
	seen := make(chan struct{})
	resp := make(chan struct{})
	defer close(resp)
	blockHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- struct{}{}
		<-resp
	})
	breaker := NewBreaker(BreakerParams{
		QueueDepth: 1, MaxConcurrency: 1, InitialCapacity: 1, QueueWaitTimeout: time.Minute,
	})
	h, reader := queueWaitTimeoutHandler(t, breaker, blockHandler)

	go h(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "http://localhost:8081/time", nil))
	<-seen

	// A deadline on the request itself is reported as before.
	ctx, cancel := context.WithTimeout(context.Background(), shortQueueWait)
	defer cancel()
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "http://localhost:8081/time", nil).WithContext(ctx))
	if got, want := rec.Code, http.StatusServiceUnavailable; got != want {
		t.Fatalf("Code = %d, want: %d", got, want)
	}
	if got, want := rec.Body.String(), context.DeadlineExceeded.Error(); !strings.Contains(got, want) {
		t.Errorf("Body = %q wanted to contain %q", got, want)
	}
	assertQueueWaitTimeouts(t, reader, 0)
}
