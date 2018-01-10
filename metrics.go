package quorum

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	splitBrain        prometheus.Gauge
	raftMembers       prometheus.Gauge
	memberlistMembers prometheus.Gauge
}

// NewMetrics returns a populated, and registered *Metrics, or an error.
func NewMetrics() (*Metrics, error) {
	a := &Metrics{
		splitBrain: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quorum",
			Name:      "split_brain",
			Help:      "whether this node is in split brain mode or not",
		}),
		raftMembers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quorum",
			Name:      "raft_members",
			Help:      "how many nodes are in the raft group",
		}),
		memberlistMembers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "quorum",
			Name:      "memberlist_members",
			Help:      "how many nodes are in the memberlist group",
		}),
	}
	return a, a.Register()
}

func (a *Metrics) SplitBrain(val int) {
	a.splitBrain.Set(float64(val))
}
func (a *Metrics) RaftMembers(val int64) {
	a.raftMembers.Set(float64(val))
}
func (a *Metrics) MemberlistMembers(val int64) {
	a.memberlistMembers.Set(float64(val))
}

func (a *Metrics) Register() error {
	if err := prometheus.Register(a.splitBrain); err != nil {
		return errors.Wrap(err, "registering splitBrain")
	}
	if err := prometheus.Register(a.raftMembers); err != nil {
		return errors.Wrap(err, "registering splitBrain")
	}
	if err := prometheus.Register(a.memberlistMembers); err != nil {
		return errors.Wrap(err, "registering splitBrain")
	}
	return nil
}
