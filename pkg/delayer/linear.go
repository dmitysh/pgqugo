package delayer

import (
	"math/rand/v2"
	"time"
)

// Linear returns the pgqugo.AttemptDelayer generating time.Duration which is linearly dependent of attempt number.
// Deviation determines the random spread around the received time.Duration in the range [-d;d]
func Linear(base time.Duration, deviation float64) func(attempt int16) time.Duration {
	if deviation < 0 || deviation > 1 {
		panic("deviation must be in [0;1]")
	}

	return func(attempt int16) time.Duration {
		return calculateDeviationPeriod(time.Duration(attempt)*base, deviation)
	}
}

func calculateDeviationPeriod(mean time.Duration, deviation float64) time.Duration {
	return time.Duration(float64(mean) + deviation*(rand.Float64()*float64(2)-float64(1))*float64(mean))
}
