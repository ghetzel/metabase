package stats

import (
	"time"
)

type Timing struct {
	Name      string
	StartedAt time.Time
}

func NewTiming(times ...time.Time) *Timing {
	if len(times) == 0 {
		times = []time.Time{time.Now()}
	}

	return &Timing{
		StartedAt: times[0],
	}
}

func (self *Timing) Send(name string, tags ...map[string]interface{}) {
	Elapsed(name, time.Since(self.StartedAt))
}

func Elapsed(name string, duration time.Duration, tags ...map[string]interface{}) {
	m := metric(name, tags)

	if StatsDB != nil {
		StatsDB.Write(m.Push(time.Now(), float64(duration)/float64(time.Millisecond)))
	}

	statsdclient.Timing(m.GetUniqueName(), duration)
}
