// Package ingest handles Loki push requests, validating and writing them to
// ClickHouse via the chstore.Writer.
package ingest

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/limiter"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/loki/v3/pkg/loghttp/push"
	"github.com/grafana/loki/v3/pkg/validation"

	"github.com/bzed/runakefli/internal/chstore"
	"github.com/bzed/runakefli/internal/ingest/distcopy"
)

// reasonTooManyPartitions labels pushes rejected for spanning more than
// chstore.MaxPushPartitions partitions in Loki's discarded-data metrics.
const reasonTooManyPartitions = "too_many_partitions"

// maxRecvMsgSize is Loki's distributor default (-distributor.max-recv-msg-size).
const maxRecvMsgSize = 100 << 20

// Handler handles Loki push requests and writes them to ClickHouse.
// RateLimiter is nil when the rate limit is disabled.
type Handler struct {
	Writer      *chstore.Writer
	Limits      *validation.Overrides
	Logger      log.Logger
	RateLimiter *limiter.RateLimiter

	// MaxRecvMsgSize and MaxDecompressedSize bound a push body; zero selects
	// the package default (100 MiB) and limits_config.max_push_size (Loki's
	// 2GB) respectively.
	MaxRecvMsgSize      int
	MaxDecompressedSize int64

	// MaxInflightBytes bounds the bytes of push bodies held in flight; 0
	// disables the bound (Loki's distributor.max_inflight_bytes).
	MaxInflightBytes int64

	// inflightBytes is the current reservation, updated atomically. It is
	// maintained even when MaxInflightBytes is 0, so the high-watermark
	// metric reports what Loki's would.
	inflightBytes atomic.Int64
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	parser, format := push.RequestParser(push.ParseLokiRequest), "loki"
	if strings.HasPrefix(r.URL.Path, "/otlp/") {
		parser, format = push.NewOTLPRequestParser(false), "otlp"
	}
	recvSize, decompSize := h.MaxRecvMsgSize, h.MaxDecompressedSize
	if recvSize == 0 {
		recvSize = maxRecvMsgSize
	}
	if decompSize == 0 {
		decompSize = int64(h.Limits.MaxPushSize(tenantID))
	}
	// Set on every request rather than once, because a Handler has no
	// constructor; the gauge describes the handler that is serving.
	maxInflightBytesGauge.Set(float64(h.MaxInflightBytes))
	// Refuse before reading the body, as in Loki's distributor.
	if h.MaxInflightBytes > 0 && h.inflightBytes.Load() >= h.MaxInflightBytes {
		http.Error(w, errServiceUnavailableMaxLoad.Error(), http.StatusServiceUnavailable)
		return
	}
	// Reserve what is already known about the body, so a concurrent push is
	// refused while this one is still decompressing. Loki reserves only the
	// parsed size, after parsing (see docs/divergences.md).
	reserved := receivedInflightBytes(r, recvSize)
	h.inflightBytes.Add(reserved)
	h.observeInflight()
	defer func() { h.inflightBytes.Add(-reserved) }()

	// One resolver per request, so every stream of the push sees the same
	// retention and policy snapshot, as in Loki's distributor.
	resolver := newStreamResolver(tenantID, h.Limits, h.Logger)
	req, err := parsePush(r, h.Logger, tenantID, h.Limits, parser, format, recvSize, decompSize, resolver)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Replace the received-size reservation with the parsed size, as Loki
	// charges its inflight budget.
	if size := int64(req.Size()); size != reserved {
		h.inflightBytes.Add(size - reserved)
		reserved = size
	}
	h.observeInflight()
	res, verr := distcopy.Prepare(r.Context(), h.Limits, tenantID, req, time.Now())
	if h.RateLimiter != nil && len(res.Streams) > 0 && !h.RateLimiter.AllowN(time.Now(), tenantID, res.Bytes) {
		// Rejected before the write, as in Loki's distributor: nothing is
		// stored, and the push consumes no tokens.
		distcopy.ReportDiscarded(r.Context(), h.Limits, tenantID, validation.RateLimited, res.Streams, time.Now())
		http.Error(w, fmt.Sprintf(validation.RateLimitedErrorMsg, tenantID, int(h.RateLimiter.Limit(time.Now(), tenantID)), res.Lines, res.Bytes), http.StatusTooManyRequests)
		return
	}
	if len(res.Streams) > 0 {
		if err := h.Writer.Write(r.Context(), tenantID, res.Streams); err != nil {
			if errors.Is(err, chstore.ErrTooManyPartitions) {
				distcopy.ReportDiscarded(r.Context(), h.Limits, tenantID, reasonTooManyPartitions, res.Streams, time.Now())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			level.Error(h.Logger).Log("msg", "write failed", "err", err) //nolint:gosec // G104: go-kit logger errors are not actionable
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if verr != nil {
		http.Error(w, verr.Error(), res.Status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
