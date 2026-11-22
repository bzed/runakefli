package server

import (
	"bytes"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/bzed/runakefli/internal/config"
)

func newSwap() (log.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return log.NewLogfmtLogger(&buf), &buf
}

func TestWarnCleartextTransport(t *testing.T) {
	cfg := &config.Config{}

	// No addr configured, then loopback (incl. empty-host) addresses: silent.
	logger, out := newSwap()
	warnCleartextTransport(logger, cfg)
	cfg.ClickHouse.Addr = []string{"localhost:9000", "127.0.0.1:9000", "127.0.0.2:9000", "[::1]:9000", ":9440"}
	warnCleartextTransport(logger, cfg)
	require.Empty(t, out.String())

	// A non-loopback address warns, naming the address.
	logger, out = newSwap()
	cfg.ClickHouse.Addr = []string{"db.internal:9000"}
	warnCleartextTransport(logger, cfg)
	require.Contains(t, out.String(), "not encrypted")
	require.Contains(t, out.String(), "db.internal:9000")

	// With TLS on, no warning.
	logger, out = newSwap()
	cfg.ClickHouse.TLS.Enabled = true
	warnCleartextTransport(logger, cfg)
	require.Empty(t, out.String())
}
