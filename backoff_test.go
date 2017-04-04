package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = FDescribe("backoff", func() {

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
			b := NewExponentialBackoff(200 * time.Millisecond, 2 * time.Second, 2, false)
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
			b := NewExponentialBackoff(200 * time.Millisecond, 2 * time.Second, 2, false)
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
