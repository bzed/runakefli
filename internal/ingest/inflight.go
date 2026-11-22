// Copied from grafana/loki@acaa752c17c1560eead5a33d04d63ed877d4a6de pkg/distributor/http.go (the max-inflight-bytes gate and accounting of pushHandler) and pkg/distributor/metrics.go (maxInflightBytes, inflightBytesHighWatermark); modifications: the gate and accounting are methods on Handler, the received Content-Length is reserved at admission so a concurrent push is refused while this one is still decompressing (Loki reserves only the parsed size, after parsing), and the collectors are package-level because runakefli does not construct Loki's Distributor.

package ingest

import (
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// errServiceUnavailableMaxLoad is Loki's error for a push refused by the
// in-flight byte budget.
var errServiceUnavailableMaxLoad = errors.New("the server cannot accept more requests at this time")

// Package-level Prometheus collectors, which AGENTS.md allows. They carry
// Loki's distributor metric names so dashboards keep working; Loki keeps them
// in its Distributor, which runakefli does not construct.
var (
	maxInflightBytesGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "loki_distributor_max_inflight_bytes",
		Help: "The max permitted inflight bytes. 0 if disabled.",
	})
	inflightBytesHighWatermark = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "loki_distributor_inflight_bytes_high_watermark",
		Help:       "The most observed inflight bytes in the last 1 minute.",
		Objectives: map[float64]float64{1.0: 0.1},
		MaxAge:     time.Minute,
	})
)

// receivedInflightBytes is what a push reserves before it is parsed: its
// received body size, bounded by recvSize, or 0 when the client sent no
// Content-Length.
func receivedInflightBytes(r *http.Request, recvSize int) int64 {
	if r.ContentLength <= 0 {
		return 0
	}
	return min(r.ContentLength, int64(recvSize))
}

// observeInflight records the current in-flight total.
func (h *Handler) observeInflight() {
	inflightBytesHighWatermark.Observe(float64(h.inflightBytes.Load()))
}
