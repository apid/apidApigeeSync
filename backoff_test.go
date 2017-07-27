// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("backoff", func() {

	Context("Backoff timeout calculations", func() {

		It("Should properly apply defaults", func() {
			log.Info("Starting backoff tests...")
			b := NewExponentialBackoff(0, 0, 0, true)
			Expect(defaultInitial).To(Equal(b.initial))
			Expect(defaultMax).To(Equal(b.max))
			Expect(defaultFactor).To(Equal(b.factor))

			b = NewExponentialBackoff(-1, -1, -1, true)
			Expect(defaultInitial).To(Equal(b.initial))
			Expect(defaultMax).To(Equal(b.max))
			Expect(defaultFactor).To(Equal(b.factor))
		})

		It("should properly apply exponential backoff strategy", func() {
			b := NewExponentialBackoff(200*time.Millisecond, 2*time.Second, 2, false)
			Expect(200 * time.Millisecond).To(Equal(b.Duration()))
			Expect(1).To(Equal(b.Attempt()))
			Expect(400 * time.Millisecond).To(Equal(b.Duration()))
			Expect(2).To(Equal(b.Attempt()))
			Expect(800 * time.Millisecond).To(Equal(b.Duration()))
			Expect(3).To(Equal(b.Attempt()))
			Expect(1600 * time.Millisecond).To(Equal(b.Duration()))
			Expect(4).To(Equal(b.Attempt()))
		})

		It("should reset properly", func() {
			b := NewExponentialBackoff(200*time.Millisecond, 2*time.Second, 2, false)
			Expect(200 * time.Millisecond).To(Equal(b.Duration()))
			Expect(1).To(Equal(b.Attempt()))
			Expect(400 * time.Millisecond).To(Equal(b.Duration()))
			Expect(2).To(Equal(b.Attempt()))
			Expect(800 * time.Millisecond).To(Equal(b.Duration()))
			Expect(3).To(Equal(b.Attempt()))
			b.Reset()
			Expect(200 * time.Millisecond).To(Equal(b.Duration()))
			Expect(1).To(Equal(b.Attempt()))
			Expect(400 * time.Millisecond).To(Equal(b.Duration()))
			Expect(2).To(Equal(b.Attempt()))
			Expect(800 * time.Millisecond).To(Equal(b.Duration()))
			Expect(3).To(Equal(b.Attempt()))
		})
	})

})
