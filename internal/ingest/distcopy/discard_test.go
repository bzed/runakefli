package distcopy_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/bzed/runakefli/internal/ingest/distcopy"
)

// discardedByRetention returns loki_discarded_samples_total of a tenant and
// reason, keyed by the retention_hours label.
func discardedByRetention(t *testing.T, tenant, reason string) map[string]float64 {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, f := range fams {
		if f.GetName() != "loki_discarded_samples_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			l := map[string]string{}
			for _, p := range m.GetLabel() {
				l[p.GetName()] = p.GetValue()
			}
			if l["tenant"] == tenant && l["reason"] == reason {
				out[l["retention_hours"]] += m.GetCounter().GetValue()
			}
		}
	}
	return out
}

// A discard carries the stream's retention hours on every path that reports
// one (Loki passes parsed.retentionHours to every reportDiscardedData call).
func TestPrepare_DiscardsCarryRetentionHours(t *testing.T) {
	now := time.Now()
	e := func(line string) logproto.Entry { return logproto.Entry{Timestamp: now, Line: line} }

	t.Run("missing enforced labels", func(t *testing.T) {
		o := overrides(t, func(l *validation.Limits) {
			l.RetentionPeriod = model.Duration(48 * time.Hour)
			l.EnforcedLabels = []string{"team"}
		})
		_, err := distcopy.Prepare(context.Background(), o, "t-ret-enforced", req(`{a="b"}`, e("x")), now)
		require.Error(t, err)
		require.Equal(t, map[string]float64{"48": 1}, discardedByRetention(t, "t-ret-enforced", validation.MissingEnforcedLabels))
	})

	t.Run("blocked ingestion", func(t *testing.T) {
		o := overrides(t, func(l *validation.Limits) {
			l.RetentionPeriod = model.Duration(48 * time.Hour)
			l.BlockIngestionUntil = flagext.Time(now.Add(time.Hour))
			l.BlockIngestionStatusCode = http.StatusOK
		})
		_, err := distcopy.Prepare(context.Background(), o, "t-ret-blocked", req(`{a="b"}`, e("x")), now)
		require.NoError(t, err)
		require.Equal(t, map[string]float64{"48": 1}, discardedByRetention(t, "t-ret-blocked", validation.BlockedIngestion))
	})

	t.Run("entry discard", func(t *testing.T) {
		o := overrides(t, func(l *validation.Limits) {
			l.RetentionPeriod = model.Duration(48 * time.Hour)
			l.MaxLineSize = 3
			l.MaxLineSizeTruncate = false
		})
		_, err := distcopy.Prepare(context.Background(), o, "t-ret-entry", req(`{a="b"}`, e("too long")), now)
		require.Error(t, err)
		require.Equal(t, map[string]float64{"48": 1}, discardedByRetention(t, "t-ret-entry", validation.LineTooLong))
	})

	t.Run("per-stream retention rule", func(t *testing.T) {
		o := overrides(t, func(l *validation.Limits) {
			l.RetentionPeriod = model.Duration(48 * time.Hour)
			l.StreamRetention = []validation.StreamRetention{{
				Period:   model.Duration(24 * time.Hour),
				Selector: `{a="b"}`,
			}}
			l.EnforcedLabels = []string{"team"}
		})
		_, err := distcopy.Prepare(context.Background(), o, "t-ret-rule", req(`{a="b"}`, e("x")), now)
		require.Error(t, err)
		require.Equal(t, map[string]float64{"24": 1}, discardedByRetention(t, "t-ret-rule", validation.MissingEnforcedLabels))
	})

	t.Run("ReportDiscarded", func(t *testing.T) {
		o := overrides(t, func(l *validation.Limits) { l.RetentionPeriod = model.Duration(48 * time.Hour) })
		streams := []logproto.Stream{{Labels: `{a="b"}`, Entries: []logproto.Entry{e("x"), e("y")}}}
		distcopy.ReportDiscarded(context.Background(), o, "t-ret-report", validation.RateLimited, streams, now)
		require.Equal(t, map[string]float64{"48": 2}, discardedByRetention(t, "t-ret-report", validation.RateLimited))
	})
}

// Blocked ingestion with a status other than 200 does not stop at the first
// stream: every stream is counted and dropped, and the push is answered with
// the configured status (Loki's ingestionBlockedError).
func TestPrepare_BlockedIngestionProcessesAllStreams(t *testing.T) {
	now := time.Now()
	o := overrides(t, func(l *validation.Limits) {
		l.BlockIngestionUntil = flagext.Time(now.Add(time.Hour))
		l.BlockIngestionStatusCode = 260
	})
	r := logproto.FromPushRequest(&logproto.PushRequest{Streams: []logproto.Stream{
		{Labels: `{a="1"}`, Entries: []logproto.Entry{{Timestamp: now, Line: "x"}}},
		{Labels: `{a="2"}`, Entries: []logproto.Entry{{Timestamp: now, Line: "y"}, {Timestamp: now.Add(time.Nanosecond), Line: "z"}}},
	}})
	res, err := distcopy.Prepare(context.Background(), o, "t-blocked-all", r, now)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ingestion blocked")
	require.Equal(t, 260, res.Status)
	require.Empty(t, res.Streams)
	require.Equal(t, map[string]float64{"0": 3}, discardedByRetention(t, "t-blocked-all", validation.BlockedIngestion))
}

// A 400 from validation takes precedence over a blocked stream's status, in
// either stream order; the blocked stream is still counted.
func TestPrepare_ValidationErrorBeatsBlockedStatus(t *testing.T) {
	now := time.Now()
	bad := logproto.Stream{Labels: `{a="1",b="2"}`, Entries: []logproto.Entry{{Timestamp: now, Line: "x"}}}
	blocked := logproto.Stream{Labels: `{a="1"}`, Entries: []logproto.Entry{{Timestamp: now, Line: "y"}}}
	for name, streams := range map[string][]logproto.Stream{
		"invalid first": {bad, blocked},
		"blocked first": {blocked, bad},
	} {
		t.Run(name, func(t *testing.T) {
			o := overrides(t, func(l *validation.Limits) {
				l.BlockIngestionUntil = flagext.Time(now.Add(time.Hour))
				l.BlockIngestionStatusCode = 260
				l.MaxLabelNamesPerSeries = 1
			})
			tenant := "t-blocked-400-" + name
			res, err := distcopy.Prepare(context.Background(), o, tenant, logproto.FromPushRequest(&logproto.PushRequest{Streams: append([]logproto.Stream(nil), streams...)}), now)
			require.Error(t, err)
			require.Equal(t, http.StatusBadRequest, res.Status)
			require.Equal(t, map[string]float64{"0": 1}, discardedByRetention(t, tenant, validation.BlockedIngestion))
		})
	}
}

// A status net/http cannot write must not panic the handler.
func TestPrepare_BlockedStatusOutOfRange(t *testing.T) {
	now := time.Now()
	for _, code := range []int{0, 99, 1000} {
		o := overrides(t, func(l *validation.Limits) {
			l.BlockIngestionUntil = flagext.Time(now.Add(time.Hour))
			l.BlockIngestionStatusCode = code
		})
		res, err := distcopy.Prepare(context.Background(), o, "t-blocked-range", req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: "x"}), now)
		require.Error(t, err)
		require.Equal(t, http.StatusInternalServerError, res.Status, "code %d", code)
	}
}

// Every error Prepare returns carries a status.
func TestPrepare_NoStreamsIsBadRequest(t *testing.T) {
	res, err := distcopy.Prepare(context.Background(), overrides(t, nil), "t", logproto.FromPushRequest(&logproto.PushRequest{}), time.Now())
	require.Error(t, err)
	require.Equal(t, http.StatusBadRequest, res.Status)
}

func TestPrepare_StatusIsZeroWithoutError(t *testing.T) {
	now := time.Now()
	res, err := distcopy.Prepare(context.Background(), overrides(t, nil), "t", req(`{a="b"}`, logproto.Entry{Timestamp: now, Line: "x"}), now)
	require.NoError(t, err)
	require.Zero(t, res.Status)
}
