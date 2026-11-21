package chstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"

	"github.com/bzed/runakefli/internal/chstore"
	"github.com/bzed/runakefli/internal/chstore/chtest"
)

func TestLabelAndSeries(t *testing.T) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1",
		logproto.Stream{Labels: `{app="a", env="dev"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}},
		logproto.Stream{Labels: `{app="b", env="prod"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	seed(t, &chstore.Writer{Conn: conn}, "t2",
		logproto.Stream{Labels: `{secret="1"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	q := &chstore.Querier{Conn: conn}
	ctx := user.InjectOrgID(context.Background(), "t1")
	start, end := t0.Add(-time.Hour), t0.Add(time.Hour)

	names, err := q.Label(ctx, &logproto.LabelRequest{Start: &start, End: &end})
	require.NoError(t, err)
	require.Equal(t, []string{"app", "env"}, names.Values)

	vals, err := q.Label(ctx, &logproto.LabelRequest{Name: "app", Values: true, Start: &start, End: &end, Query: `{env="prod"}`})
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, vals.Values)

	ser, err := q.Series(ctx, &logproto.SeriesRequest{Start: start, End: end, Groups: []string{`{app=~"a|b"}`}})
	require.NoError(t, err)
	require.Len(t, ser.Series, 2)
	require.Equal(t, "app", ser.Series[0].Labels[0].Key)
	require.Equal(t, "a", ser.Series[0].Labels[0].Value)
}

// Loki leaves its internal labels out of the label name responses but still
// serves their values, so a client can query by them.
func internalLabelsFixture(t *testing.T) (*chstore.Querier, context.Context, time.Time, time.Time) {
	conn, _ := chtest.New(t)
	seed(t, &chstore.Writer{Conn: conn}, "t1",
		logproto.Stream{Labels: `{app="a"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}},
		logproto.Stream{Labels: `{app="b", __time_shard__="1", __stream_shard__="0", __pattern__="x", __aggregated_metric__="y"}`, Entries: []logproto.Entry{{Timestamp: t0, Line: "x"}}})
	return &chstore.Querier{Conn: conn}, user.InjectOrgID(context.Background(), "t1"), t0.Add(-time.Hour), t0.Add(time.Hour)
}

func TestLabel_HidesInternalLabelNames(t *testing.T) {
	q, ctx, start, end := internalLabelsFixture(t)
	names, err := q.Label(ctx, &logproto.LabelRequest{Start: &start, End: &end})
	require.NoError(t, err)
	require.Equal(t, []string{"app"}, names.Values)
}

func TestLabel_ServesValuesOfInternalLabels(t *testing.T) {
	q, ctx, start, end := internalLabelsFixture(t)
	vals, err := q.Label(ctx, &logproto.LabelRequest{Name: "__time_shard__", Values: true, Start: &start, End: &end})
	require.NoError(t, err)
	require.Equal(t, []string{"1"}, vals.Values)
}
