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

package v1

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"

	"knative.dev/serving/pkg/apis/serving"
)

func TestValidateQueueSidecarQueueAnnotations(t *testing.T) {
	const (
		multiplierKey = serving.QueueSidecarQueueDepthMultiplierAnnotationKey
		timeoutKey    = serving.QueueSidecarQueueWaitTimeoutAnnotationKey
	)
	path := func(key string) []string {
		return []string{fmt.Sprintf("[%s]", key)}
	}

	cases := []struct {
		name        string
		annotations map[string]string
		expectErr   *apis.FieldError
	}{{
		name: "no annotations",
	}, {
		name:        "unrelated annotation",
		annotations: map[string]string{serving.CreatorAnnotation: "umph"},
	}, {
		name:        "valid values",
		annotations: map[string]string{multiplierKey: "3", timeoutKey: "1s"},
	}, {
		name:        "smallest multiplier and no wait limit",
		annotations: map[string]string{multiplierKey: "1", timeoutKey: "0s"},
	}, {
		name:        "sub-second timeout",
		annotations: map[string]string{timeoutKey: "250ms"},
	}, {
		name:        "zero multiplier",
		annotations: map[string]string{multiplierKey: "0"},
		expectErr: &apis.FieldError{
			Message: "expected 1 <= 0 <= 2147483647",
			Paths:   path(multiplierKey),
		},
	}, {
		name:        "negative multiplier",
		annotations: map[string]string{multiplierKey: "-3"},
		expectErr: &apis.FieldError{
			Message: "expected 1 <= -3 <= 2147483647",
			Paths:   path(multiplierKey),
		},
	}, {
		name:        "fractional multiplier",
		annotations: map[string]string{multiplierKey: "1.5"},
		expectErr: &apis.FieldError{
			Message: "invalid value: 1.5",
			Paths:   path(multiplierKey),
		},
	}, {
		name:        "empty multiplier",
		annotations: map[string]string{multiplierKey: ""},
		expectErr: &apis.FieldError{
			Message: "invalid value: ",
			Paths:   path(multiplierKey),
		},
	}, {
		name:        "timeout is not a duration",
		annotations: map[string]string{timeoutKey: "1"},
		expectErr: &apis.FieldError{
			Message: "invalid value: 1",
			Paths:   path(timeoutKey),
		},
	}, {
		name:        "negative timeout",
		annotations: map[string]string{timeoutKey: "-1s"},
		expectErr: &apis.FieldError{
			Message: "queue-wait-timeout=-1s must not be negative",
			Paths:   path(timeoutKey),
		},
	}, {
		name:        "both invalid",
		annotations: map[string]string{multiplierKey: "x", timeoutKey: "y"},
		expectErr: (&apis.FieldError{
			Message: "invalid value: x",
			Paths:   path(multiplierKey),
		}).Also(&apis.FieldError{
			Message: "invalid value: y",
			Paths:   path(timeoutKey),
		}),
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateQueueSidecarQueueAnnotations(c.annotations)
			if got, want := err.Error(), c.expectErr.Error(); got != want {
				t.Errorf("Got: %q want: %q", got, want)
			}
		})
	}
}

// The annotations are validated as part of the revision template, which is
// what the webhook validates for Services and Configurations.
func TestRevisionTemplateSpecQueueAnnotationValidation(t *testing.T) {
	rts := func(annotations map[string]string) *RevisionTemplateSpec {
		return &RevisionTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Annotations: annotations},
			Spec: RevisionSpec{
				PodSpec: corev1.PodSpec{
					Containers: []corev1.Container{{Image: "helloworld"}},
				},
			},
		}
	}

	if err := rts(map[string]string{
		serving.QueueSidecarQueueDepthMultiplierAnnotationKey: "2",
		serving.QueueSidecarQueueWaitTimeoutAnnotationKey:     "1s",
	}).Validate(context.Background()).Filter(apis.ErrorLevel); err != nil {
		t.Error("Validate() =", err)
	}

	err := rts(map[string]string{
		serving.QueueSidecarQueueDepthMultiplierAnnotationKey: "0",
		serving.QueueSidecarQueueWaitTimeoutAnnotationKey:     "-1s",
	}).Validate(context.Background()).Filter(apis.ErrorLevel)
	want := (&apis.FieldError{
		Message: "expected 1 <= 0 <= 2147483647",
		Paths:   []string{fmt.Sprintf("[%s]", serving.QueueSidecarQueueDepthMultiplierAnnotationKey)},
	}).Also(&apis.FieldError{
		Message: "queue-wait-timeout=-1s must not be negative",
		Paths:   []string{fmt.Sprintf("[%s]", serving.QueueSidecarQueueWaitTimeoutAnnotationKey)},
	}).ViaField("metadata.annotations")
	if got, want := err.Error(), want.Error(); got != want {
		t.Errorf("Validate() = %q, want: %q", got, want)
	}
}
