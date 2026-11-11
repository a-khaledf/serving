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

package deployment

import (
	"strings"
	"testing"
	"time"
)

func TestQueueSidecarQueueSettings(t *testing.T) {
	tests := []struct {
		name           string
		data           map[string]string
		wantMultiplier int
		wantTimeout    time.Duration
		wantErr        string
	}{{
		name: "unset",
	}, {
		name: "both set",
		data: map[string]string{
			queueSidecarQueueDepthMultiplierKey: "3",
			queueSidecarQueueWaitTimeoutKey:     "1s",
		},
		wantMultiplier: 3,
		wantTimeout:    time.Second,
	}, {
		name:        "explicit zero timeout means no limit",
		data:        map[string]string{queueSidecarQueueWaitTimeoutKey: "0s"},
		wantTimeout: 0,
	}, {
		name:    "zero multiplier",
		data:    map[string]string{queueSidecarQueueDepthMultiplierKey: "0"},
		wantErr: "queue-sidecar-queue-depth-multiplier must be at least 1, was 0",
	}, {
		name:    "negative multiplier",
		data:    map[string]string{queueSidecarQueueDepthMultiplierKey: "-1"},
		wantErr: "queue-sidecar-queue-depth-multiplier must be at least 1, was -1",
	}, {
		name:    "multiplier is not a number",
		data:    map[string]string{queueSidecarQueueDepthMultiplierKey: "ten"},
		wantErr: queueSidecarQueueDepthMultiplierKey,
	}, {
		name:    "negative timeout",
		data:    map[string]string{queueSidecarQueueWaitTimeoutKey: "-1s"},
		wantErr: "queue-sidecar-queue-wait-timeout cannot be a negative duration, was -1s",
	}, {
		name:    "timeout is not a duration",
		data:    map[string]string{queueSidecarQueueWaitTimeoutKey: "1"},
		wantErr: queueSidecarQueueWaitTimeoutKey,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := map[string]string{QueueSidecarImageKey: "busybox"}
			for k, v := range test.data {
				data[k] = v
			}
			got, err := NewConfigFromMap(data)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("NewConfigFromMap() = %v, want an error containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal("NewConfigFromMap() =", err)
			}
			if got.QueueSidecarQueueDepthMultiplier != test.wantMultiplier {
				t.Errorf("QueueSidecarQueueDepthMultiplier = %d, want: %d", got.QueueSidecarQueueDepthMultiplier, test.wantMultiplier)
			}
			if got.QueueSidecarQueueWaitTimeout != test.wantTimeout {
				t.Errorf("QueueSidecarQueueWaitTimeout = %v, want: %v", got.QueueSidecarQueueWaitTimeout, test.wantTimeout)
			}
		})
	}
}
