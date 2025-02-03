package delayer

import (
	"math/rand/v2"
	"time"

	"github.com/DmitySH/pgqugo"
)

func Linear(base time.Duration, deviation float64) pgqugo.AttemptDelayer {
	return func(attempt int16) time.Duration {
		return calculateDeviationPeriod(time.Duration(attempt)*base, deviation)
	}
}

func calculateDeviationPeriod(mean time.Duration, deviation float64) time.Duration {
	return time.Duration(float64(mean) + deviation*(rand.Float64()*float64(2)-float64(1))*float64(mean))
}
