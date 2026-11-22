// Package chtest provides throwaway ClickHouse databases for tests.
package chtest

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/bzed/runakefli/internal/chstore"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Topology is the layout tests run against: a single server by default,
// or the cluster named by RUNAKEFLI_TEST_CH_CLUSTER (see test/cluster/up.sh).
func Topology() chstore.Topology {
	c := os.Getenv("RUNAKEFLI_TEST_CH_CLUSTER")
	return chstore.Topology{Cluster: c, Replicated: c != "" && os.Getenv("RUNAKEFLI_TEST_CH_REPLICATED") != "0"}
}

// Options returns connection options for the test server with the given default database.
// RUNAKEFLI_TEST_CH_TLS (unset/empty/0 = off, any other value = TLS with the
// system roots) and RUNAKEFLI_TEST_CH_TLS_CA (PEM CA file, replaces the system
// roots) select a TLS server, e.g. ClickHouse Cloud's native port 9440.
func Options(db string) *clickhouse.Options {
	o := &clickhouse.Options{
		Addr: []string{env("RUNAKEFLI_TEST_CH_ADDR", "localhost:9000")},
		Auth: clickhouse.Auth{
			Database: db,
			Username: env("RUNAKEFLI_TEST_CH_USER", "default"),
			Password: os.Getenv("RUNAKEFLI_TEST_CH_PASSWORD"),
		},
		Settings: Topology().Settings(),
	}
	if tlsEnv := os.Getenv("RUNAKEFLI_TEST_CH_TLS"); tlsEnv != "" && tlsEnv != "0" {
		tlsCfg := &tls.Config{}
		if ca := os.Getenv("RUNAKEFLI_TEST_CH_TLS_CA"); ca != "" {
			b, err := os.ReadFile(ca) //nolint:gosec // G304: path from the test operator's environment
			if err != nil {
				panic(fmt.Sprintf("RUNAKEFLI_TEST_CH_TLS_CA %s: %v", ca, err)) // test helper: programmer error at test start
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(b) {
				panic(fmt.Sprintf("RUNAKEFLI_TEST_CH_TLS_CA %s: no PEM certificate", ca))
			}
			tlsCfg.RootCAs = pool
		}
		o.TLS = tlsCfg
	}
	return o
}

// New creates and migrates a fresh database, dropped (on every node) when the test ends.
func New(t testing.TB) (driver.Conn, string) {
	t.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	db := "lh_test_" + hex.EncodeToString(b)
	ctx := context.Background()
	topo := Topology()

	admin, err := clickhouse.Open(Options("default"))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("clickhouse not reachable (set RUNAKEFLI_TEST_CH_ADDR or start: podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9): %v", err)
	}
	if err := chstore.Migrate(ctx, admin, db, topo); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	conn, err := clickhouse.Open(Options(db))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = chstore.DropDatabase(ctx, admin, db, topo)
		_ = admin.Close()
	})
	return conn, db
}

// LogsRowsRead sums read_rows of the finished initial SELECT queries on the
// logs table in database db that started at or after since. Tests use it to
// prove that an index skipped granules. On a cluster the initiator's row
// carries the total over all shards.
func LogsRowsRead(t testing.TB, conn driver.Conn, db string, since time.Time) uint64 {
	t.Helper()
	ctx := context.Background()
	topo := Topology()
	flush := "SYSTEM FLUSH LOGS query_log"
	src := "system.query_log"
	if topo.Cluster != "" {
		flush = "SYSTEM FLUSH LOGS ON CLUSTER " + topo.Cluster
		src = fmt.Sprintf("clusterAllReplicas(%s, system, query_log)", topo.Cluster)
	}
	if err := conn.Exec(ctx, flush); err != nil {
		t.Fatalf("flush query_log: %v", err)
	}
	var n uint64
	// The position() on query_log keeps this helper's own query out of the sum.
	q := "SELECT sum(read_rows) FROM " + src + " WHERE type = 'QueryFinish' AND is_initial_query AND query_kind = 'Select' AND current_database = ? AND event_time_microseconds >= fromUnixTimestamp64Micro(?) AND position(query, 'FROM logs') > 0 AND position(query, 'query_log') = 0"
	if err := conn.QueryRow(ctx, q, db, since.UnixMicro()).Scan(&n); err != nil {
		t.Fatalf("read query_log: %v", err)
	}
	return n
}
