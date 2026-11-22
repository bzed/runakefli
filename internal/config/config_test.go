package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	lokipush "github.com/grafana/loki/v3/pkg/loghttp/push"
	"github.com/stretchr/testify/require"

	"github.com/bzed/runakefli/internal/chstore"
)

func TestLoad_DefaultsAndOverrides(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
auth_enabled: true
server: {http_listen_port: 3101}
clickhouse: {addr: ["ch:9000"], database: logs}
limits_config: {max_line_size: 1024}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.True(t, c.AuthEnabled)
	require.Equal(t, 3101, c.Server.HTTPListenPort)
	require.Equal(t, []string{"ch:9000"}, c.ClickHouse.Addr)
	require.EqualValues(t, 1024, c.LimitsConfig.MaxLineSize)
	require.Equal(t, 7*24*time.Hour, time.Duration(c.LimitsConfig.RejectOldSamplesMaxAge)) // Loki default kept
	require.Equal(t, chstore.Topology{}, c.Topology())                                     // single node by default
}

func TestLoad_DistributorMaxInflightBytes(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("distributor:\n  max_inflight_bytes: 1048576\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, 1048576, c.Distributor.MaxInflightBytes)

	require.NoError(t, os.WriteFile(p, []byte("distributor:\n  max_inflight_bytes: -1\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "max inflight bytes cannot be less than zero")

	// Only max_inflight_bytes is supported in the block.
	require.NoError(t, os.WriteFile(p, []byte("distributor:\n  foo: 1\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "foo")
}

// config.Load applies Loki's distributor OTLP defaults to the limits, so
// service.name is a stream label for OTLP pushes (Loki's cmd/loki/main.go).
func TestLoad_OTLPDefaultsIndexServiceName(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("{}\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.NotNil(t, c.LimitsConfig.OTLPConfig)
	require.Equal(t, lokipush.IndexLabel, c.LimitsConfig.OTLPConfig.ActionForResourceAttribute("service.name"))
}

func TestLoad_Compactor(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
compactor: {delete_request_cancel_period: 2h}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, 2*time.Hour, c.Compactor.DeleteRequestCancelPeriod)
	require.Equal(t, 10*time.Minute, c.Compactor.CompactionInterval) // default kept
	require.Equal(t, 24*time.Hour, c.Compactor.DeleteMaxInterval)    // default kept
}

func TestLoad_CompactorRetentionEnabled(t *testing.T) {
	load := func(y string) *Config {
		p := filepath.Join(t.TempDir(), "c.yaml")
		require.NoError(t, os.WriteFile(p, []byte(y), 0o600))
		c, err := Load(p, false)
		require.NoError(t, err)
		return c
	}
	require.True(t, load("auth_enabled: true\n").Compactor.RetentionEnabled, "runakefli default is true (Loki's is false)")
	require.False(t, load("compactor: {retention_enabled: false}\n").Compactor.RetentionEnabled)
}

func TestLoad_Cluster(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
clickhouse:
  addr: ["ch1:9000", "ch3:9000"]
  cluster: runakefli
  settings: {insert_quorum: 2, max_execution_time: 60}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, chstore.Topology{Cluster: "runakefli", Replicated: true}, c.Topology())
	s := c.CHSettings()
	require.EqualValues(t, 2, s["insert_quorum"]) // explicit setting wins over the topology default
	require.EqualValues(t, 60, s["max_execution_time"])
	require.EqualValues(t, 1, s["distributed_foreground_insert"])
}

func TestLoad_RetentionStreamMatchersPopulated(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`limits_config:
  retention_stream:
    - selector: '{app="a"}'
      priority: 1
      period: 48h
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Len(t, c.LimitsConfig.StreamRetention, 1)
	require.NotEmpty(t, c.LimitsConfig.StreamRetention[0].Matchers, "Validate must populate matchers")
}

func TestLoad_InvalidRetentionStreamRejected(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`limits_config:
  retention_stream:
    - selector: '{app="a"}'
      priority: 1
      period: 1h
`), 0o600))
	_, err := Load(p, false)
	require.ErrorContains(t, err, "retention period must be >= 24h")
}

func TestLoad_RegisterInstrumentation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.True(t, c.Server.RegisterInstrumentation) // Loki/dskit default true

	require.NoError(t, os.WriteFile(p, []byte("server: {register_instrumentation: false}\n"), 0o600))
	c2, err := Load(p, false)
	require.NoError(t, err)
	require.False(t, c2.Server.RegisterInstrumentation)
}

func TestLoad_RulerDisabledByDefault(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.False(t, c.RulerEnabled())
	require.Equal(t, "rules-temp", c.Ruler.RulePath)    // runakefli default (Loki: /rules)
	require.Equal(t, time.Minute, c.Ruler.PollInterval) // Loki default kept
	require.Equal(t, "local", c.Ruler.Evaluation.Mode)  // Loki default kept
}

func TestLoad_Ruler(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
ruler:
  external_url: http://grafana.example
  alertmanager_url: http://am:9093
  poll_interval: 5s
  remote_write:
    enabled: true
    clients:
      prom: {url: "http://prom:9090/api/v1/write"}
ruler_storage:
  backend: filesystem
  filesystem: {dir: /var/lib/runakefli/rules}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.True(t, c.RulerEnabled())
	require.Equal(t, "http://grafana.example", c.Ruler.ExternalURL.String())
	require.Equal(t, "http://am:9093", c.Ruler.AlertmanagerURL)
	require.Equal(t, 5*time.Second, c.Ruler.PollInterval)
	require.Equal(t, time.Minute, c.Ruler.EvaluationInterval) // Loki default kept
	require.Equal(t, "http://prom:9090/api/v1/write", c.Ruler.RemoteWrite.Clients["prom"].URL.String())
	require.Equal(t, "filesystem", c.RulerStorage.Backend)
	require.Equal(t, "/var/lib/runakefli/rules", c.RulerStorage.Filesystem.Directory)
}

func TestLoad_RulerRejectsUnsupportedModes(t *testing.T) {
	for name, tc := range map[string]struct{ ruler, want string }{
		"sharding":          {"enable_sharding: true", "enable_sharding"},
		"remote evaluation": {"evaluation: {mode: remote}", "evaluation.mode"},
		"remote write without clients": {
			"remote_write: {enabled: true}", "no clients",
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "c.yaml")
			require.NoError(t, os.WriteFile(p, []byte("ruler: {"+tc.ruler+"}\nruler_storage: {backend: filesystem, filesystem: {dir: /tmp/r}}\n"), 0o600))
			_, err := Load(p, false)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestLoad_ServerTimeoutDefaults(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	// dskit server defaults, as in Loki.
	require.Equal(t, 30*time.Second, c.Server.HTTPServerReadTimeout)
	require.Equal(t, time.Duration(0), c.Server.HTTPServerReadHeaderTimeout)
	require.Equal(t, 30*time.Second, c.Server.HTTPServerWriteTimeout)
	require.Equal(t, 120*time.Second, c.Server.HTTPServerIdleTimeout)
	require.Equal(t, 30*time.Second, c.Server.GracefulShutdownTimeout)
}

func TestLoad_ServerTimeouts(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`server:
  http_server_read_timeout: 1s
  http_server_read_header_timeout: 2s
  http_server_write_timeout: 3s
  http_server_idle_timeout: 4s
  graceful_shutdown_timeout: 5s
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second},
		[]time.Duration{c.Server.HTTPServerReadTimeout, c.Server.HTTPServerReadHeaderTimeout, c.Server.HTTPServerWriteTimeout, c.Server.HTTPServerIdleTimeout, c.Server.GracefulShutdownTimeout})
}

func TestLoad_UnknownKeyRejected(t *testing.T) {
	for name, tc := range map[string]struct{ yaml, key string }{
		"top level": {"auth_enabld: false\n", "auth_enabld"},
		"nested":    {"server:\n  http_listen_prot: 3100\n", "http_listen_prot"},
	} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "c.yaml")
			require.NoError(t, os.WriteFile(p, []byte(tc.yaml), 0o600))
			_, err := Load(p, false)
			require.ErrorContains(t, err, tc.key)
		})
	}
}

func TestLoad_ExpandEnv(t *testing.T) {
	t.Setenv("LH_TEST_DB", "fromenv")
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  database: ${LH_TEST_DB}\n  username: ${LH_TEST_USER:-reader}\n"), 0o600))

	c, err := Load(p, true)
	require.NoError(t, err)
	require.Equal(t, "fromenv", c.ClickHouse.Database)
	require.Equal(t, "reader", c.ClickHouse.Username)

	c, err = Load(p, false) // without the flag the text is taken literally, as in Loki
	require.NoError(t, err)
	require.Equal(t, "${LH_TEST_DB}", c.ClickHouse.Database)
}

func TestLoad_PasswordFile(t *testing.T) {
	dir := t.TempDir()
	pw := filepath.Join(dir, "pw")
	require.NoError(t, os.WriteFile(pw, []byte("s3cret\n"), 0o600))
	p := filepath.Join(dir, "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  password_file: "+pw+"\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, "s3cret", c.ClickHouse.Password, "trailing newline is stripped")

	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  password: x\n  password_file: "+pw+"\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "password and password_file")

	require.NoError(t, os.WriteFile(p, []byte("clickhouse:\n  password_file: "+filepath.Join(dir, "missing")+"\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "password_file")
}

func TestLoad_Logging(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, "info", c.Server.LogLevel.String())
	require.Equal(t, "logfmt", c.Server.LogFormat)

	require.NoError(t, os.WriteFile(p, []byte("server: {log_level: debug, log_format: json}\n"), 0o600))
	c, err = Load(p, false)
	require.NoError(t, err)
	require.Equal(t, "debug", c.Server.LogLevel.String())
	require.Equal(t, "json", c.Server.LogFormat)

	require.NoError(t, os.WriteFile(p, []byte("server: {log_format: xml}\n"), 0o600))
	_, err = Load(p, false)
	require.ErrorContains(t, err, "log_format")
}

func TestLoad_MultiTenantQueriesKey(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("auth_enabled: false\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	require.False(t, c.Query.Querier.MultiTenantQueriesEnabled, "defaults to false")

	require.NoError(t, os.WriteFile(p, []byte("query:\n  querier: {multi_tenant_queries_enabled: true}\n"), 0o600))
	c, err = Load(p, false)
	require.NoError(t, err)
	require.True(t, c.Query.Querier.MultiTenantQueriesEnabled)
}

func TestLoad_ExampleConfig(t *testing.T) {
	_, err := Load(filepath.Join("..", "..", "runakefli.example.yaml"), false)
	require.NoError(t, err)
}

func TestLoad_PackagedConfig(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", t.TempDir())
	c, err := Load(filepath.Join("..", "..", "runakefli.example.yaml"), true) // the package installs it as /etc/runakefli/runakefli.yaml
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", c.Server.HTTPListenAddress, "the package listens on localhost by default")
}

func TestLoad_ClickHouseClientDefaults(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("{}\n"), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	o := c.CHOptions("logs")
	require.Equal(t, 5*time.Second, o.DialTimeout)
	require.Equal(t, 5*time.Minute, o.ReadTimeout)
	require.Equal(t, 100, o.MaxOpenConns)
	require.Equal(t, 20, o.MaxIdleConns)
	require.Equal(t, "logs", o.Auth.Database)
	require.Equal(t, []string{"localhost:9000"}, o.Addr)
}

func TestLoad_ClickHouseClientOverrides(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(`
clickhouse: {dial_timeout: 2s, read_timeout: 1m, max_open_conns: 8, max_idle_conns: 4}
`), 0o600))
	c, err := Load(p, false)
	require.NoError(t, err)
	o := c.CHOptions("default")
	require.Equal(t, 2*time.Second, o.DialTimeout)
	require.Equal(t, time.Minute, o.ReadTimeout)
	require.Equal(t, 8, o.MaxOpenConns)
	require.Equal(t, 4, o.MaxIdleConns)
}

func TestLoad_ClickHouseClientInvalid(t *testing.T) {
	for _, body := range []string{
		"clickhouse: {max_open_conns: 0}\n",
		"clickhouse: {max_open_conns: 4, max_idle_conns: 5}\n",
		"clickhouse: {dial_timeout: 0s}\n",
		"clickhouse: {read_timeout: -1s}\n",
	} {
		p := filepath.Join(t.TempDir(), "c.yaml")
		require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
		_, err := Load(p, false)
		require.Error(t, err, body)
	}
}

// writeTLSCerts writes a self-signed certificate/key pair (valid for
// tls.LoadX509KeyPair) and a CA bundle (the same certificate) into dir.
func writeTLSCerts(t *testing.T, dir string) (certPath, keyPath, caPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "runakefli-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	certPath, keyPath, caPath = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(caPath, certPEM, 0o600))
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certPath, keyPath, caPath
}

// loadCH loads a config from YAML (the package's other tests write a
// t.TempDir() file and call Load the same way).
func loadCH(t *testing.T, ch string) (*Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "runakefli.yaml")
	require.NoError(t, os.WriteFile(p, []byte("server:\n  http_listen_port: 0\n"+ch), 0o600))
	return Load(p, false)
}

func TestClickHouseTLSDisabled(t *testing.T) {
	c, err := loadCH(t, "clickhouse:\n  addr: [localhost:9000]\n")
	require.NoError(t, err)
	require.Nil(t, c.CHOptions("db").TLS) // default: cleartext, exactly today
}

func TestClickHouseTLSEnabledBare(t *testing.T) {
	c, err := loadCH(t, "clickhouse:\n  addr: [localhost:9000]\n  tls:\n    enabled: true\n")
	require.NoError(t, err)
	require.NotNil(t, c.CHOptions("db").TLS)
	require.Empty(t, c.CHOptions("db").TLS.ServerName)
	require.False(t, c.CHOptions("db").TLS.InsecureSkipVerify)
	require.Nil(t, c.CHOptions("db").TLS.RootCAs) // system roots
}

func TestClickHouseTLSFull(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, caPath := writeTLSCerts(t, dir)
	y := fmt.Sprintf("clickhouse:\n  addr: [localhost:9000]\n  tls:\n    enabled: true\n    ca_file: %q\n    cert_file: %q\n    key_file: %q\n    server_name: ch.internal\n    insecure_skip_verify: true\n", caPath, certPath, keyPath)
	c, err := loadCH(t, y)
	require.NoError(t, err)
	tc := c.CHOptions("db").TLS
	require.NotNil(t, tc)
	require.Equal(t, "ch.internal", tc.ServerName)
	require.True(t, tc.InsecureSkipVerify)
	require.NotNil(t, tc.RootCAs)
	require.Len(t, tc.Certificates, 1) // the loaded client keypair for mTLS
}

func TestClickHouseTLSErrors(t *testing.T) {
	certPath, _, _ := writeTLSCerts(t, t.TempDir()) // keyPath unused here: Go rejects an unused variable

	// cert_file and key_file must be set together — even when TLS is disabled.
	_, err := loadCH(t, fmt.Sprintf("clickhouse:\n  tls:\n    cert_file: %q\n", certPath))
	require.ErrorContains(t, err, "cert_file and key_file must be set together")

	// Unreadable CA file fails at Load, like password_file.
	_, err = loadCH(t, "clickhouse:\n  tls:\n    enabled: true\n    ca_file: /no/such/ca.pem\n")
	require.ErrorContains(t, err, "ca_file /no/such/ca.pem")

	// A CA file without PEM certificates fails at Load.
	garbage := filepath.Join(t.TempDir(), "garbage.pem")
	require.NoError(t, os.WriteFile(garbage, []byte("not a certificate\n"), 0o600))
	_, err = loadCH(t, fmt.Sprintf("clickhouse:\n  tls:\n    enabled: true\n    ca_file: %q\n", garbage))
	require.ErrorContains(t, err, "no PEM certificate")

	// An unreadable client keypair fails at Load.
	_, err = loadCH(t, "clickhouse:\n  tls:\n    enabled: true\n")
	require.NoError(t, err) // enabled alone (system roots, no client cert) is valid
	_, err = loadCH(t, fmt.Sprintf("clickhouse:\n  tls:\n    enabled: true\n    cert_file: %q\n    key_file: /no/such/key.pem\n", certPath))
	require.ErrorContains(t, err, "key_file /no/such/key.pem")
}
