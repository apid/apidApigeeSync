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
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"
)

const (
	httpTimeout                       = time.Minute
	pluginTimeout                     = time.Minute
	maxIdleConnsPerHost               = 10
	defaultInitial      time.Duration = 200 * time.Millisecond
	defaultMax          time.Duration = 10 * time.Second
	defaultFactor       float64       = 2
)

var (
	initialBackoffInterval = defaultInitial
)

var (
	expected200Error = fmt.Errorf("did not recieve OK response")
	quitSignalError  = fmt.Errorf("signal to quit encountered")
	authFailError    = fmt.Errorf("authorization failed")
)

type Backoff struct {
	attempt         int
	initial, max    time.Duration
	jitter          bool
	backoffStrategy func() time.Duration
}

type ExponentialBackoff struct {
	Backoff
	factor float64
}

func NewExponentialBackoff(initial, max time.Duration, factor float64, jitter bool) *ExponentialBackoff {
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
	backoff.jitter = jitter
	backoff.backoffStrategy = backoff.exponentialBackoffStrategy

	return backoff
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

	if duration > math.MaxInt64 {
		return b.max
	}
	dur := time.Duration(duration)

	if b.jitter {
		duration = rand.Float64()*(duration-initial) + initial
	}

	if dur > b.max {
		return b.max
	}

	log.Debugf("Backing off for %d ms", int64(dur/time.Millisecond))
	return dur
}

func (b *Backoff) Reset() {
	b.attempt = 0
}

func (b *Backoff) Attempt() int {
	return b.attempt
}

/*
 * Call toExecute repeatedly until it does not return an error, with an exponential backoff policy
 * for retrying on errors
 */
func pollWithBackoff(quit chan bool, toExecute func(chan bool) error, handleError func(error)) {

	backoff := NewExponentialBackoff(initialBackoffInterval, config.GetDuration(configPollInterval), 2, true)

	//initialize the retry channel to start first attempt immediately
	retry := time.After(0 * time.Millisecond)

	for {
		select {
		case <-quit:
			log.Info("Quit signal recieved.  Returning")
			return
		case <-retry:
			start := time.Now()

			err := toExecute(quit)
			if err == nil || err == quitSignalError {
				return
			}

			end := time.Now()
			//error encountered, since we would have returned above otherwise
			handleError(err)

			/* TODO keep this around? Imagine an immediately erroring service,
			 *  causing many sequential requests which could pollute logs
			 */
			//only backoff if the request took less than one second
			if end.After(start.Add(time.Second)) {
				backoff.Reset()
				retry = time.After(0 * time.Millisecond)
			} else {
				retry = time.After(backoff.Duration())
			}
		}
	}
}

func addHeaders(req *http.Request, token string) {
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("apid_instance_id", apidInfo.InstanceID)
	req.Header.Set("apid_cluster_Id", apidInfo.ClusterID)
	req.Header.Set("updated_at_apid", time.Now().Format(time.RFC3339))
}

type changeServerError struct {
	Code string `json:"code"`
}

func (a changeServerError) Error() string {
	return a.Code
}
