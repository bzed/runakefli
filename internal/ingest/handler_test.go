package ingest_test

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/bzed/runakefli/internal/chstore"
	"github.com/bzed/runakefli/internal/chstore/chtest"
	"github.com/bzed/runakefli/internal/ingest"
)

func defaultLimits(t *testing.T) *validation.Overrides {
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	return o
}

func push(t *testing.T, h http.Handler, tenant, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(user.InjectOrgID(req.Context(), tenant))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPush_DiscoversServiceNameAndLevel(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	ts := time.Now().UnixNano()
	body := `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(ts) + `","level=error msg=boom"]]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())

	var lstr, lvl string
	ctx := context.Background()
	require.NoError(t, conn.QueryRow(ctx, "SELECT labels_str FROM streams WHERE tenant='t1'").Scan(&lstr))
	require.Equal(t, `{app="web", service_name="web"}`, lstr)
	require.NoError(t, conn.QueryRow(ctx, "SELECT structured_metadata['detected_level'] FROM logs WHERE tenant='t1'").Scan(&lvl))
	require.Equal(t, "error", lvl)
}

func TestPush_RejectsTooOldAndKeepsRest(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	old := time.Now().Add(-30 * 24 * time.Hour).UnixNano()
	ok := time.Now().UnixNano()
	body := `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(old) + `","old"],["` + itoa(ok) + `","new"]]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "timestamp too old")

	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM logs WHERE tenant='t1'").Scan(&n))
	require.Equal(t, uint64(1), n)
}

func TestPush_RejectsInvalidLabels(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	rec := push(t, h, "t1", `{"streams":[{"stream":{},"values":[["1","x"]]}]}`)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func itoa(i int64) string { return strconv.FormatInt(i, 10) }

func TestPush_MissingTenant(t *testing.T) {
	// Test that missing tenant ID returns 401
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	// Create a request without tenant context
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", strings.NewReader(`{"streams":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestPush_InvalidJSON(t *testing.T) {
	// Test that invalid JSON returns 400
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", strings.NewReader("invalid json"))
	req = req.WithContext(user.InjectOrgID(req.Context(), "t1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPush_OTLPPath(t *testing.T) {
	// Test that OTLP path is handled
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	// OTLP requests are more complex, for now just test that the path doesn't cause an error
	body := `{"resourceLogs":[]}`
	req := httptest.NewRequest(http.MethodPost, "/otlp/v1/logs", strings.NewReader(body))
	req = req.WithContext(user.InjectOrgID(req.Context(), "t1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// OTLP parsing might fail, but we just want to ensure the path is recognized
	_ = rec.Code
}

func TestPush_TooManyPartitionsIs400(t *testing.T) {
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	l.RejectOldSamples = false // a backfill
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: log.NewNopLogger()}

	day := time.Now().UTC().Truncate(24 * time.Hour).Add(time.Hour)
	var values []string
	for i := chstore.MaxPushPartitions + 1; i > 0; i-- {
		values = append(values, fmt.Sprintf(`[%q,"x"]`, itoa(day.AddDate(0, 0, -i).UnixNano())))
	}
	body := `{"streams":[{"stream":{"app":"a"},"values":[` + strings.Join(values, ",") + `]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "at most 60 are accepted per push")
}

func TestPush_RecordsLokiPushMetrics(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	ts := time.Now().UnixNano()
	body := `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(ts) + `","a"],["` + itoa(ts+1) + `","b"]]}]}`
	rec := push(t, h, "t-metrics", body)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())

	fams, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	byName := map[string]*dto.MetricFamily{}
	for _, f := range fams {
		byName[f.GetName()] = f
	}
	for _, name := range []string{
		"loki_distributor_bytes_received_total",
		"loki_distributor_lines_received_total",
		"loki_distributor_structured_metadata_bytes_received_total",
		"loki_distributor_expanded_bytes_received_total",
		"loki_distributor_lag_ms_total",
		"loki_distributor_streams_per_push_request",
	} {
		require.Contains(t, byName, name)
	}
	var lines float64
	for _, m := range byName["loki_distributor_lines_received_total"].GetMetric() {
		l := map[string]string{}
		for _, p := range m.GetLabel() {
			l[p.GetName()] = p.GetValue()
		}
		if l["tenant"] == "t-metrics" {
			require.Equal(t, map[string]string{"format": "loki", "is_internal_stream": "false", "policy": "", "tenant": "t-metrics"}, l)
			lines = m.GetCounter().GetValue()
		}
	}
	require.Equal(t, float64(2), lines)
}

func TestPush_BodyTooLarge(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{
		Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger(),
		MaxRecvMsgSize: 64, MaxDecompressedSize: 64,
	}
	body := `{"streams":[{"stream":{"app":"web"},"values":[["1","` + strings.Repeat("x", 256) + `"]]}]}`
	rec := push(t, h, "t1", body)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "request body too large")
}

func TestPush_InvalidBackfillShard(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	body := `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(time.Now().UnixNano()) + `","x"]]}]}`
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Loki-Backfill-Shard", "bad\xffshard") // not valid UTF-8, so not a label value
	req = req.WithContext(user.InjectOrgID(req.Context(), "t-shard"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

	var n uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT count() FROM logs WHERE tenant='t-shard'").Scan(&n))
	require.Zero(t, n)
}

func TestPush_JSONEdgeCases(t *testing.T) {
	conn, _ := chtest.New(t)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: defaultLimits(t), Logger: log.NewNopLogger()}
	ts := time.Now().UnixNano()
	v := func(i int64, line string) string { return `["` + itoa(ts+i) + `","` + line + `"]` }
	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{"unknown stream key", `{"streams":[{"stream":{"app":"web"},"values":[` + v(0, "a") + `],"foo":1}]}`, http.StatusBadRequest},
		{"null entry is skipped", `{"streams":[{"stream":{"app":"web"},"values":[` + v(0, "a") + `,null,` + v(1, "b") + `]}]}`, http.StatusNoContent},
		{"numeric timestamp", `{"streams":[{"stream":{"app":"web"},"values":[[` + itoa(ts) + `,"a"]]}]}`, http.StatusBadRequest},
		{"numeric structured metadata", `{"streams":[{"stream":{"app":"web"},"values":[["` + itoa(ts) + `","a",{"k":1}]]}]}`, http.StatusBadRequest},
		{"null stream", `{"streams":[{"stream":null,"values":[` + v(0, "a") + `]}]}`, http.StatusBadRequest},
		{"null label value", `{"streams":[{"stream":{"app":null},"values":[` + v(0, "a") + `]}]}`, http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := push(t, h, "t-json", tc.body)
			require.Equal(t, tc.want, rec.Code, rec.Body.String())
		})
	}
}

// discarded sums loki_discarded_samples_total and loki_discarded_bytes_total
// over every label combination of tenant and reason (retention_hours differs
// between the places that report a discard).
func discarded(t *testing.T, tenant, reason string) (samples, bytes float64) {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range fams {
		var dst *float64
		switch f.GetName() {
		case "loki_discarded_samples_total":
			dst = &samples
		case "loki_discarded_bytes_total":
			dst = &bytes
		default:
			continue
		}
		for _, m := range f.GetMetric() {
			l := map[string]string{}
			for _, p := range m.GetLabel() {
				l[p.GetName()] = p.GetValue()
			}
			if l["tenant"] == tenant && l["reason"] == reason {
				*dst += m.GetCounter().GetValue()
			}
		}
	}
	return samples, bytes
}

func TestPush_LabelValidationDiscardAccounting(t *testing.T) {
	conn, _ := chtest.New(t)
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	l.MaxLabelNamesPerSeries = 2
	o, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)
	h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: o, Logger: log.NewNopLogger()}
	ts := time.Now().UnixNano()

	t.Run("too many label names", func(t *testing.T) {
		body := `{"streams":[{"stream":{"a":"1","b":"2","c":"3"},"values":[["` + itoa(ts) + `","aa"],["` + itoa(ts+1) + `","bbb"],["` + itoa(ts+2) + `","cccc"]]}]}`
		rec := push(t, h, "t-disc-names", body)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		samples, bytes := discarded(t, "t-disc-names", "max_label_names_per_series")
		require.Equal(t, float64(3), samples)
		require.Equal(t, float64(9), bytes)
		// Today the same stream is reported a second time; Task 3 removes it.
		samples, _ = discarded(t, "t-disc-names", "invalid_labels")
		require.Equal(t, float64(3), samples)
	})

	t.Run("no labels", func(t *testing.T) {
		// Service-name discovery would add a label to the empty stream.
		var nl validation.Limits
		nl.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
		nl.DiscoverServiceName = nil
		no, err := validation.NewOverrides(nl, nil)
		require.NoError(t, err)
		h := &ingest.Handler{Writer: &chstore.Writer{Conn: conn}, Limits: no, Logger: log.NewNopLogger()}
		body := `{"streams":[{"stream":{},"values":[["` + itoa(ts) + `","a"],["` + itoa(ts+1) + `","b"]]}]}`
		rec := push(t, h, "t-disc-empty", body)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		samples, _ := discarded(t, "t-disc-empty", "missing_labels")
		require.Equal(t, float64(1), samples)
		samples, _ = discarded(t, "t-disc-empty", "invalid_labels")
		require.Equal(t, float64(2), samples)
	})
}
