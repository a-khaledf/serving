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

package sharedmain

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"

	"knative.dev/serving/pkg/queue"
)

// processQueueEnv parses the queue-proxy environment with the required
// variables set, plus the given overrides.
func processQueueEnv(t *testing.T, overrides map[string]string) (config, error) {
	t.Helper()
	for k, v := range map[string]string{
		"CONTAINER_CONCURRENCY":    "1",
		"QUEUE_SERVING_PORT":       "8080",
		"QUEUE_SERVING_TLS_PORT":   "443",
		"REVISION_TIMEOUT_SECONDS": "300",
		"USER_PORT":                "8081",
		"SERVING_LOGGING_CONFIG":   "",
		"SERVING_LOGGING_LEVEL":    "info",
		"SERVING_NAMESPACE":        "default",
		"SERVING_CONFIGURATION":    "",
		"SERVING_REVISION":         "rev",
		"SERVING_POD":              "pod",
		"SERVING_POD_IP":           "1.1.1.1",
	} {
		t.Setenv(k, v)
	}
	for k, v := range overrides {
		t.Setenv(k, v)
	}
	var env config
	err := envconfig.Process("", &env)
	return env, err
}

// queueCapacity returns how many requests the breaker admits (executing plus
// queued) before it starts rejecting them with ErrRequestQueueFull.
func queueCapacity(t *testing.T, b *queue.Breaker) int {
	t.Helper()
	done := make(chan struct{})
	finished := make(chan error)
	admitted := 0
	defer func() {
		close(done)
		for range admitted {
			if err := <-finished; err != nil {
				t.Error("Admitted request failed:", err)
			}
		}
	}()

	for {
		// InFlight is incremented synchronously on admission, so it tells
		// whether the request sent below was admitted or rejected.
		go func() {
			finished <- b.Maybe(context.Background(), func() { <-done })
		}()
		deadline := time.After(5 * time.Second)
		for b.InFlight() == admitted {
			select {
			case err := <-finished:
				if !errors.Is(err, queue.ErrRequestQueueFull) {
					t.Fatalf("Maybe() = %v, want: %v", err, queue.ErrRequestQueueFull)
				}
				return admitted
			case <-deadline:
				t.Fatal("Timed out waiting for the request to be admitted or rejected")
			default:
				time.Sleep(100 * time.Microsecond)
			}
		}
		admitted++
	}
}

func TestQueueConfigDefaults(t *testing.T) {
	env, err := processQueueEnv(t, nil)
	if err != nil {
		t.Fatal("Got unexpected error processing env:", err)
	}
	if got, want := env.QueueDepthMultiplier, 10; got != want {
		t.Errorf("QueueDepthMultiplier = %d, want: %d", got, want)
	}
	if got, want := env.QueueWaitTimeout, time.Duration(0); got != want {
		t.Errorf("QueueWaitTimeout = %v, want: %v", got, want)
	}
	if err := validateQueueConfig(env); err != nil {
		t.Error("validateQueueConfig() =", err)
	}
}

func TestQueueConfigFromEnv(t *testing.T) {
	env, err := processQueueEnv(t, map[string]string{
		"QUEUE_DEPTH_MULTIPLIER": "3",
		"QUEUE_WAIT_TIMEOUT":     "1500ms",
	})
	if err != nil {
		t.Fatal("Got unexpected error processing env:", err)
	}
	if got, want := env.QueueDepthMultiplier, 3; got != want {
		t.Errorf("QueueDepthMultiplier = %d, want: %d", got, want)
	}
	if got, want := env.QueueWaitTimeout, 1500*time.Millisecond; got != want {
		t.Errorf("QueueWaitTimeout = %v, want: %v", got, want)
	}
	if err := validateQueueConfig(env); err != nil {
		t.Error("validateQueueConfig() =", err)
	}
}

func TestQueueConfigInvalid(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{{
		name:    "zero multiplier",
		env:     map[string]string{"QUEUE_DEPTH_MULTIPLIER": "0"},
		wantErr: "QUEUE_DEPTH_MULTIPLIER must be at least 1, got 0",
	}, {
		name:    "negative multiplier",
		env:     map[string]string{"QUEUE_DEPTH_MULTIPLIER": "-2"},
		wantErr: "QUEUE_DEPTH_MULTIPLIER must be at least 1, got -2",
	}, {
		name:    "multiplier is not a number",
		env:     map[string]string{"QUEUE_DEPTH_MULTIPLIER": "many"},
		wantErr: "QUEUE_DEPTH_MULTIPLIER",
	}, {
		name:    "negative timeout",
		env:     map[string]string{"QUEUE_WAIT_TIMEOUT": "-1s"},
		wantErr: "QUEUE_WAIT_TIMEOUT must not be negative, got -1s",
	}, {
		name:    "timeout is not a duration",
		env:     map[string]string{"QUEUE_WAIT_TIMEOUT": "1"},
		wantErr: "QUEUE_WAIT_TIMEOUT",
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env, err := processQueueEnv(t, test.env)
			if err == nil {
				err = validateQueueConfig(env)
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("Error = %v, want it to contain %q", err, test.wantErr)
			}
		})
	}
}

func TestBuildBreakerQueueDepth(t *testing.T) {
	logger := zap.NewNop().Sugar()

	tests := []struct {
		name string
		env  map[string]string
		// wantCapacity is containerConcurrency + queue depth, 0 for no breaker.
		wantCapacity int
	}{{
		name:         "default multiplier reproduces the hardcoded 10x",
		env:          map[string]string{"CONTAINER_CONCURRENCY": "2"},
		wantCapacity: 2 + 10*2,
	}, {
		name:         "smaller multiplier",
		env:          map[string]string{"CONTAINER_CONCURRENCY": "2", "QUEUE_DEPTH_MULTIPLIER": "1"},
		wantCapacity: 2 + 1*2,
	}, {
		name:         "larger multiplier",
		env:          map[string]string{"CONTAINER_CONCURRENCY": "2", "QUEUE_DEPTH_MULTIPLIER": "15"},
		wantCapacity: 2 + 15*2,
	}, {
		name: "no breaker without container concurrency",
		env: map[string]string{
			"CONTAINER_CONCURRENCY":  "0",
			"QUEUE_DEPTH_MULTIPLIER": "3",
			"QUEUE_WAIT_TIMEOUT":     "1s",
		},
		wantCapacity: 0,
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env, err := processQueueEnv(t, test.env)
			if err != nil {
				t.Fatal("Got unexpected error processing env:", err)
			}
			b := buildBreaker(logger, env)
			if test.wantCapacity == 0 {
				if b != nil {
					t.Fatal("buildBreaker() returned a breaker, want none")
				}
				return
			}
			if got := queueCapacity(t, b); got != test.wantCapacity {
				t.Errorf("Admitted requests = %d, want: %d", got, test.wantCapacity)
			}
		})
	}
}

func TestBuildBreakerQueueWaitTimeout(t *testing.T) {
	env, err := processQueueEnv(t, map[string]string{"QUEUE_WAIT_TIMEOUT": "10ms"})
	if err != nil {
		t.Fatal("Got unexpected error processing env:", err)
	}
	b := buildBreaker(zap.NewNop().Sugar(), env)

	done := make(chan struct{})
	held := make(chan struct{})
	go b.Maybe(context.Background(), func() {
		close(held)
		<-done
	})
	<-held
	defer close(done)

	if err := b.Maybe(context.Background(), func() {}); !errors.Is(err, queue.ErrRequestQueueWaitTimeout) {
		t.Errorf("Maybe() = %v, want: %v", err, queue.ErrRequestQueueWaitTimeout)
	}
}
