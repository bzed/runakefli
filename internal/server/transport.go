package server

import (
	"net"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/bzed/runakefli/internal/config"
)

// warnCleartextTransport warns once when the ClickHouse transport is not
// encrypted and at least one addr is not loopback: password, tenant names and
// log data cross that network in cleartext (see docs/divergences.md, Storage
// transport). Warns regardless of authentication: the data itself is the
// secret.
func warnCleartextTransport(logger log.Logger, cfg *config.Config) {
	if cfg.ClickHouse.TLS.Enabled {
		return
	}
	for _, addr := range cfg.ClickHouse.Addr {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		if host == "" || host == "localhost" {
			continue
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			continue
		}
		level.Warn(logger).Log("msg", "clickhouse transport is not encrypted: password and log data cross the network in cleartext; enable clickhouse.tls", "addr", addr) //nolint:gosec // G104: logger Log error is not actionable (as in Loki)
		return
	}
}
