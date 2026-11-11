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

package resources

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"

	"knative.dev/serving/pkg/apis/serving"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	"knative.dev/serving/pkg/deployment"
)

func TestMakeQueueContainerQueueSettings(t *testing.T) {
	const (
		multiplierEnv = "QUEUE_DEPTH_MULTIPLIER"
		timeoutEnv    = "QUEUE_WAIT_TIMEOUT"
	)

	tests := []struct {
		name        string
		annotations map[string]string
		dc          deployment.Config
		// want holds the expected values of the two env vars, a missing key
		// means the env var must not be set at all.
		want map[string]string
	}{{
		name: "neither set",
		want: map[string]string{},
	}, {
		name: "annotations set",
		annotations: map[string]string{
			serving.QueueSidecarQueueDepthMultiplierAnnotationKey: "2",
			serving.QueueSidecarQueueWaitTimeoutAnnotationKey:     "1s",
		},
		want: map[string]string{multiplierEnv: "2", timeoutEnv: "1s"},
	}, {
		name: "config-deployment defaults set",
		dc: deployment.Config{
			QueueSidecarQueueDepthMultiplier: 5,
			QueueSidecarQueueWaitTimeout:     1500 * time.Millisecond,
		},
		want: map[string]string{multiplierEnv: "5", timeoutEnv: "1.5s"},
	}, {
		name: "both set, annotations win",
		annotations: map[string]string{
			serving.QueueSidecarQueueDepthMultiplierAnnotationKey: "2",
			serving.QueueSidecarQueueWaitTimeoutAnnotationKey:     "250ms",
		},
		dc: deployment.Config{
			QueueSidecarQueueDepthMultiplier: 5,
			QueueSidecarQueueWaitTimeout:     3 * time.Second,
		},
		want: map[string]string{multiplierEnv: "2", timeoutEnv: "250ms"},
	}, {
		name: "only the multiplier annotation set",
		annotations: map[string]string{
			serving.QueueSidecarQueueDepthMultiplierAnnotationKey: "1",
		},
		want: map[string]string{multiplierEnv: "1"},
	}, {
		name: "only the timeout default set",
		dc: deployment.Config{
			QueueSidecarQueueWaitTimeout: time.Second,
		},
		want: map[string]string{timeoutEnv: "1s"},
	}, {
		name: "zero timeout annotation disables the config-deployment default",
		annotations: map[string]string{
			serving.QueueSidecarQueueWaitTimeoutAnnotationKey: "0s",
		},
		dc: deployment.Config{
			QueueSidecarQueueWaitTimeout: time.Second,
		},
		want: map[string]string{},
	}, {
		name: "invalid annotations fall back to the config-deployment defaults",
		annotations: map[string]string{
			serving.QueueSidecarQueueDepthMultiplierAnnotationKey: "0",
			serving.QueueSidecarQueueWaitTimeoutAnnotationKey:     "soon",
		},
		dc: deployment.Config{
			QueueSidecarQueueDepthMultiplier: 5,
			QueueSidecarQueueWaitTimeout:     time.Second,
		},
		want: map[string]string{multiplierEnv: "5", timeoutEnv: "1s"},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rev := revision("bar", "foo",
				withContainers(containers),
				withContainerConcurrency(1),
				func(revision *v1.Revision) {
					revision.Annotations = test.annotations
				})
			cfg := revConfig()
			cfg.Deployment = &test.dc

			c, err := makeQueueContainer(rev, cfg)
			if err != nil {
				t.Fatal("makeQueueContainer returned error:", err)
			}

			got := map[string]string{}
			for _, e := range c.Env {
				if e.Name == multiplierEnv || e.Name == timeoutEnv {
					if _, dup := got[e.Name]; dup {
						t.Errorf("Env var %s is set more than once", e.Name)
					}
					got[e.Name] = e.Value
				}
			}
			if !cmp.Equal(got, test.want) {
				t.Errorf("Queue settings env (-want, +got) =\n%s", cmp.Diff(test.want, got))
			}

			// Apart from those two variables the container is the same as
			// one built without any of the settings.
			base, err := makeQueueContainer(revision("bar", "foo",
				withContainers(containers), withContainerConcurrency(1)), revConfig())
			if err != nil {
				t.Fatal("makeQueueContainer returned error:", err)
			}
			var rest []corev1.EnvVar
			for _, e := range c.Env {
				if e.Name != multiplierEnv && e.Name != timeoutEnv {
					rest = append(rest, e)
				}
			}
			c.Env = rest
			if !cmp.Equal(base, c, quantityComparer) {
				t.Errorf("makeQueueContainer besides the queue settings (-want, +got) =\n%s", cmp.Diff(base, c, quantityComparer))
			}
		})
	}
}
