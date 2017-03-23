package apidApigeeSync

import (
	"math"
	"math/rand"
	"time"
)

const defaultInitial time.Duration  = 200 * time.Millisecond
const defaultMax time.Duration      = 10 * time.Second
const defaultFactor float64         = 2

type Backoff struct {
	attempt         int
	initial, max    time.Duration
	backoffStrategy func() time.Duration
}

type ExponentialBackoff struct {
	Backoff
	factor float64

}

func NewExponentialBackoff(initial, max time.Duration, factor float64) *Backoff {
	backoff := &ExponentialBackoff{}

	if initial <= 0 {
		initial = defaultInitial
	}
	if max <= 0 {
		max = defaultMax
	}

	if factor <= 0 {
		factor = defaultFactor
	}

	backoff.initial = initial
	backoff.max = max
	backoff.attempt = 0
	backoff.factor = factor
	backoff.backoffStrategy = backoff.exponentialBackoffStrategy

	return &backoff.Backoff
}


func (b *Backoff) Duration() time.Duration {
	d := b.backoffStrategy()
	b.attempt++
	return d
}

func (b *ExponentialBackoff) exponentialBackoffStrategy() time.Duration {

	initial := float64(b.Backoff.initial)
	attempt := float64(b.Backoff.attempt)
	duration := initial * math.Pow(b.factor, attempt)

	//introduce some jitter
	duration = (rand.Float64()*(duration-initial) + initial)

	if duration > math.MaxInt64 {
		return b.max
	}
	dur := time.Duration(duration)

	if dur > b.max {
		return b.max
	}

	return dur
}

func (b *Backoff) Reset() {
	b.attempt = 0
}

func (b *Backoff) Attempt() int {
	return b.attempt
}