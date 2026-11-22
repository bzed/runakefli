# ClickHouse TLS Configuration (#45) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the ClickHouse connection a `clickhouse.tls` block (enabled, ca_file, cert_file, key_file, server_name, insecure_skip_verify) that wires an `*tls.Config` into every connection Rúnakefli opens, warns at startup when the transport would cross a non-loopback network in cleartext, and lets the test helpers select a TLS server (prerequisite for ClickHouse Cloud verification).

**Architecture:** Rúnakefli builds one pooled `clickhouse.Options` through `Config.CHOptions(db)` (`internal/config/config.go:212-223`) — the pooled ingest/query connection, the `-schema.migrate` admin connection, and every test helper all derive from it, so setting `Options.TLS` there covers everything. The clickhouse-go driver (v2.49.0) starts TLS when `opt.TLS != nil` (`tls.DialWithDialer`, `conn.go:34-42`); with an empty `ServerName` it verifies each address's own host, which is what a multi-node cluster needs. Material (CA, client keypair) is loaded and validated at config-load time, fail-fast like `clickhouse.password_file` (`config.go:154-168`); the resulting `*tls.Config` is cached in an unexported `Config` field. `chtest.Options` gains env-driven TLS so the oracle can later run against ClickHouse Cloud's native TLS port 9440 (roadmap).

**Tech Stack:** Go stdlib only (`crypto/tls`, `crypto/x509`, `encoding/pem`, `crypto/ecdsa` for test fixtures). No new third-party dependencies.

**Spec:** https://github.com/bzed/runakefli/issues/45 and its two validation comments (the second rules out any workaround through `clickhouse.addr`); `docs/roadmap.md`'s "ClickHouse® Cloud verification" item; the current "Storage transport" divergence entry (added in commit `222e24a`).

## Global Constraints

From `AGENTS.md` (read it before starting):

- `make check` must pass before push; unit tests need a ClickHouse at `localhost:9000` (if not serving: `podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9`). Tasks 1-2 of this plan do NOT need it (pure config tests); run the config tests without the cluster if it is unavailable.
- No new third-party dependencies (stdlib `crypto/*` only).
- Conventional commits, one logical change per commit; every commit leaves tests green.
- The strict decoder (`dec.KnownFields(true)`, `config.go:126-127`) is why the new keys must be added to the `Config` struct, not just to docs.
- Docs key tests are enforced by tests, not convention: `TestConfigurationDocsCoverEveryKey` (each dotted key must appear in backticks in `docs/reference/configuration.md`), `TestExampleShowsEveryKey` and `TestExampleWithEveryOptionUncommented` (each key in `runakefli.example.yaml`, active or commented; the all-uncommented variant must load and validate). Task 1 satisfies them in the same commit that adds the keys.
- Files are created `0o600` unless wider modes are required; secrets never in logs/errors (the new code logs no key material).
- Every user-visible change carries a `CHANGELOG.md` line under `## [Unreleased]` (Task 4); the divergence entry is updated in the same task.

## Review Focus

1. **All connection sites inherit TLS**: the pooled connection (`internal/server/server.go` `openClickHouse`, ~`:196-208`), the `-schema.migrate` admin connection (`server.go:216-227`) and the chtest helpers all call `CHOptions` — Task 1's CHOptions test covers them all by covering the one builder. Do not add `TLS` anywhere else.
2. **Startup fail-fast on bad material**: missing/unparseable `ca_file`, missing `cert_file`/`key_file` must fail at `config.Load`, with errors that name the file — exactly like `password_file` does; `cert_file` without `key_file` (and vice versa) is rejected even when TLS is disabled, but file *contents* are only read when enabled.
3. **Multi-address semantics**: leave `server_name` empty and `tls.DialWithDialer` verifies each `clickhouse.addr` host against that host's own name; a non-empty `server_name` applies to *all* addresses — document that in the configuration reference and do not "fix" it per address (the driver has no per-addr TLS hook).
4. **`chtest.Options` cannot return an error** (its signature is used in ~10 call sites): an unreadable/invalid `RUNAKEFLI_TEST_CH_TLS_CA` panics with a message naming the env var — acceptable for a test helper (programmer error at test start), and the only place panic is allowed in this plan.
5. **The example yaml must stay loadable when every commented option is uncommented** (`TestExampleWithEveryOptionUncommented`): use empty/false values in the commented `clickhouse.tls` block so `enabled: true` + no files is a *valid* configuration (bare `&tls.Config{}`).

---

### Task 1: the `clickhouse.tls` block, from YAML to `Options.TLS`

**Files:**
- Modify: `internal/config/config.go` — `Config.ClickHouse` struct (`:43-58`), a new `loadTLS` call in `Load` next to `loadPasswordFile` (`:154-168`), validation next to `validateClickHouseClient` (`:191-209`), `CHOptions` (`:211-223`)
- Test: `internal/config/config_test.go`
- Modify: `runakefli.example.yaml` (commented `clickhouse.tls` block)
- Modify: `docs/reference/configuration.md` (six key rows in the `clickhouse` table + a short "TLS" subsection after "Password handling")

**Interfaces:**
- Consumes: `CHOptions(db string) *clickhouse.Options` as is.
- Produces: `Config.ClickHouse.TLS` (anonymous struct, yaml key `tls`, fields: `Enabled bool \`yaml:"enabled"\``, `CAFile string \`yaml:"ca_file"\``, `CertFile string \`yaml:"cert_file"\``, `KeyFile string \`yaml:"key_file"\``, `ServerName string \`yaml:"server_name"\``, `InsecureSkipVerify bool \`yaml:"insecure_skip_verify"\``), and a `CHOptions` that sets `TLS *tls.Config` iff `Enabled`.

- [ ] **Step 1: Write the failing tests**

In `internal/config/config_test.go`, add (adjust import list; the cert helper uses stdlib only):

```go
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
```

```go
// loadCH loads a ClickHouse-only config from YAML. If a minimal YAML like the
// ones below is rejected by Load, copy the minimal-config pattern from the
// neighbouring tests in config_test.go instead - the point is loading only the
// clickhouse block.
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
	certPath, keyPath, _ := writeTLSCerts(t, t.TempDir())

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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestClickHouseTLS' -count=1`
Expected: FAIL — compile error (unknown field `TLS`) or Load rejecting `clickhouse.tls` as an unknown key (`field tls not found` — the strict decoder proves the key really is new).

- [ ] **Step 3: Implement the config block, loading and validation**

In `internal/config/config.go`:

1. Add to `Config.ClickHouse` (after `MaxIdleConns`, inside the anonymous struct):

```go
		TLS struct {
			Enabled            bool   `yaml:"enabled"`
			CAFile             string `yaml:"ca_file"`
			CertFile           string `yaml:"cert_file"`
			KeyFile            string `yaml:"key_file"`
			ServerName         string `yaml:"server_name"`
			InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
		} `yaml:"tls"`
```

2. Add an unexported field on `Config` (top-level, next to the yaml-tagged fields; no yaml tag — reflection walkers skip it, which is what keeps the docs tests honest):

```go
	chTLS *tls.Config // loaded at Load; set only when ClickHouse.TLS.Enabled
```

3. Validation — add to `validateClickHouseClient` (it runs on every Load, enabled or not — the structural error must be caught even when TLS is off, but file contents are read only by step 4, so validation stays side-effect free):

```go
	if (ch.TLS.CertFile != "") != (ch.TLS.KeyFile != "") {
		return errors.New("clickhouse: tls: cert_file and key_file must be set together")
	}
```

4. `loadTLS` — called from `Load` next to wherever `loadPasswordFile` runs. When `!c.ClickHouse.TLS.Enabled`, return nil without reading anything. When enabled:

```go
	c.chTLS = &tls.Config{
		ServerName:         c.ClickHouse.TLS.ServerName,
		InsecureSkipVerify: c.ClickHouse.TLS.InsecureSkipVerify, //nolint:gosec // G402: the operator explicitly opted into skipping verification
	}
	if ca := c.ClickHouse.TLS.CAFile; ca != "" {
		b, err := os.ReadFile(ca)
		if err != nil {
			return fmt.Errorf("clickhouse: tls: read ca_file %s: %w", ca, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return fmt.Errorf("clickhouse: tls: ca_file %s contains no PEM certificate", ca)
		}
		c.chTLS.RootCAs = pool
	}
	if cert, key := c.ClickHouse.TLS.CertFile, c.ClickHouse.TLS.KeyFile; cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return fmt.Errorf("clickhouse: tls: load key_file %s (cert_file %s): %w", key, cert, err)
		}
		c.chTLS.Certificates = []tls.Certificate{pair}
	}
```

(The error strings are pinned: the tests assert `ca_file /no/such/ca.pem`, `no PEM certificate`, `cert_file and key_file must be set together` and `key_file /no/such/key.pem` with `require.ErrorContains`.)

Check `Load`'s error wrapping convention against `loadPasswordFile` and match it; if files there are read with a helper that already wraps errors, reuse it.

5. `CHOptions` — set the field:

```go
	if c.ClickHouse.TLS.Enabled {
		o.TLS = c.chTLS
	}
```

(with `o` being the returned literal's variable; keep `CHOptions`'s current structure).

- [ ] **Step 4: Update the example config and the reference**

In `runakefli.example.yaml`, inside the `clickhouse:` block (commented out, empty/false values so `TestExampleWithEveryOptionUncommented` stays valid — see Review Focus 5):

```yaml
  # Native protocol over TLS: enabled wrap the connection in TLS; with no other
  # key this uses the system root CAs and verifies each addr's host name.
  #tls:
  #  enabled: true
  #  ca_file: ""
  #  cert_file: ""
  #  key_file: ""
  #  server_name: ""
  #  insecure_skip_verify: false
```

In `docs/reference/configuration.md`, add six rows to the `clickhouse` key table (after `max_idle_conns`) — descriptions: `tls.enabled` (default `false`; connect over TLS, e.g. port 9440 on TLS-enforced servers; bare enabled = system root CAs, per-address host verification), `tls.ca_file` (default `""`; PEM CA bundle replacing the system roots; read at startup, unreadable/empty = startup error), `tls.cert_file`/`tls.key_file` (client certificate for mutual TLS, only together; read at startup), `tls.server_name` (default `""`; overrides the verified name for **all** addresses — leave empty with a cluster), `tls.insecure_skip_verify` (default `false`; development only). Add a short "### TLS" subsection after "Password handling": three sentences — TLS off (default) means the password and every log line cross the network in cleartext; keep `clickhouse.addr` on loopback or enable TLS; reference the divergences page.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/config/ -count=1`
Expected: PASS — the four new tests, `TestExampleShowsEveryKey`, `TestExampleWithEveryOptionUncommented`, `TestConfigurationDocsCoverEveryKey` and `TestLoad_UnknownKeyRejected` all green.

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go runakefli.example.yaml docs/reference/configuration.md
git commit -m "feat: clickhouse.tls config block wires TLS into the native protocol (#45)"
```

### Task 2: Startup warning for cleartext transport to non-loopback addresses

**Files:**
- Create: `internal/server/transport.go` — `warnCleartextTransport(logger log.Logger, cfg *config.Config)`
- Modify: `internal/server/server.go` — call it first thing in `Run`
- Test: `internal/server/transport_test.go`

**Interfaces:**
- Consumes: `cfg.ClickHouse.TLS.Enabled` and `cfg.ClickHouse.Addr` (Task 1).
- Produces: none.

- [ ] **Step 1: Write the failing test**

`internal/server/transport_test.go` is deliberately `package server` (internal test for an unexported helper; Go allows it next to the `server_test` files):

```go
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
	sw := &log.SwapLogger{} // zero value discards; Swap installs the capture
	sw.Swap(log.NewLogfmtLogger(&buf))
	return sw, &buf
}

func TestWarnCleartextTransport(t *testing.T) {
	cfg := &config.Config{}

	// No addr configured, then loopback (incl. empty-host) addresses: silent.
	logger, out := newSwap()
	warnCleartextTransport(logger, cfg)
	cfg.ClickHouse.Addr = []string{"localhost:9000", "127.0.0.1:9000", ":9440"}
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/server/ -run TestWarnCleartextTransport -count=1`
Expected: FAIL — `warnCleartextTransport` undefined.

- [ ] **Step 3: Implement**

`internal/server/transport.go` (package `server`):

```go
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
		switch host {
		case "", "localhost", "127.0.0.1", "::1":
			continue
		}
		level.Warn(logger).Log("msg", "clickhouse transport is not encrypted: password and log data cross the network in cleartext; enable clickhouse.tls", "addr", addr)
		return
	}
}
```

Call it at the very top of `Run` in `internal/server/server.go`, before any connection is opened (the warning must precede a possibly-long dial). Run has the logger available from its `Options` (read `Run`'s head; if the logger is resolved after other early calls, put the call right after the logger exists).

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/server/ -run TestWarnCleartextTransport -count=1 && go build ./...`
Expected: PASS and build clean.

- [ ] **Step 5: Commit**

```bash
git add internal/server/transport.go internal/server/transport_test.go internal/server/server.go
git commit -m "feat: warn at startup on cleartext ClickHouse transport to non-loopback addresses (#45)"
```

### Task 3: chtest env switches for TLS servers (ClickHouse Cloud prerequisite)

**Files:**
- Modify: `internal/chstore/chtest/chtest.go` — `Options` (`:33-44`)
- Test: `internal/chstore/chtest/chtest_test.go` (create if absent)

**Interfaces:**
- Consumes: nothing (clickhouse-go `Options.TLS` directly).
- Produces: env contract `RUNAKEFLI_TEST_CH_TLS` (unset/empty/`0` = off, any other value = on with `&tls.Config{}` — system roots, per-addr host verification) and optional `RUNAKEFLI_TEST_CH_TLS_CA` (PEM file loaded into `RootCAs`; unreadable or invalid panics naming the env var — Review Focus 5).

- [ ] **Step 1: Write the failing test**

```go
func TestOptionsTLS(t *testing.T) {
	// Unset-in-spirit (""), explicit "0": off. Anything else: on, bare config.
	for _, v := range []string{"", "0"} {
		t.Run("off", func(t *testing.T) {
			t.Setenv("RUNAKEFLI_TEST_CH_TLS", v)
			require.Nil(t, Options("db").TLS)
		})
	}
	t.Run("on", func(t *testing.T) {
		t.Setenv("RUNAKEFLI_TEST_CH_TLS", "1")
		o := Options("db")
		require.NotNil(t, o.TLS)
		require.Nil(t, o.TLS.RootCAs) // system roots unless a CA is given
	})
}
```

Add a third case in the same test after implementing: `RUNAKEFLI_TEST_CH_TLS=1` plus `RUNAKEFLI_TEST_CH_TLS_CA` pointing at a self-written TempDir PEM (reuse the PEM-writing pattern from Task 1's `writeTLSCerts`, or inline a minimal `pem.EncodeToMemory`) → `RootCAs` non-nil. Note: a fully *unset* `RUNAKEFLI_TEST_CH_TLS` is also off by construction (`os.Getenv` returns ""), which the `""` case pins.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/chstore/chtest/ -run TestOptionsTLS -count=1`
Expected: FAIL — `Options(...).TLS` is nil on the "on" case.

- [ ] **Step 3: Implement**

In `chtest.Options`, after building the literal, mirror the config semantics:

```go
	o := &clickhouse.Options{ /* current fields unchanged */ }
	if tlsEnv := os.Getenv("RUNAKEFLI_TEST_CH_TLS"); tlsEnv != "" && tlsEnv != "0" {
		tlsCfg := &tls.Config{}
		if ca := os.Getenv("RUNAKEFLI_TEST_CH_TLS_CA"); ca != "" {
			b, err := os.ReadFile(ca)
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
```

Add the cursor line to the `Topology`/docs comment in chtest.go naming the new env vars so the next reader finds them.

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/chstore/chtest/ -count=1`
Expected: PASS (`NewV1`/`NewV2` reuse `Options`, so they inherit TLS with no changes — read them once to confirm).

- [ ] **Step 5: Commit**

```bash
git add internal/chstore/chtest/chtest.go internal/chstore/chtest/chtest_test.go
git commit -m "test: chtest selects a TLS ClickHouse via RUNAKEFLI_TEST_CH_TLS (#45)"
```

### Task 4: Divergences, roadmap, CHANGELOG

**Files:**
- Modify: `docs/divergences.md` — the "Storage transport" section (added in `222e24a`)
- Modify: `docs/roadmap.md` — the "ClickHouse® Cloud verification" bullet
- Modify: `CHANGELOG.md` — `## [Unreleased]`

**Interfaces:**
- Consumes: Tasks 1-3 (their behaviour makes the new text true).
- Produces: docs consistent with the shipped code.

- [ ] **Step 1: Rewrite the divergence entry**

Replace the "Cleartext-only ClickHouse connection" bullet with:

```markdown
- **ClickHouse transport is cleartext by default:** the native connection is plain TCP unless `clickhouse.tls.enabled` is set; there is no transport encryption on by default, so with the default configuration the password (also one read from `clickhouse.password_file`) crosses the network in cleartext in the first handshake packet, together with log data, label values and tenant names. Rúnakefli warns at startup when TLS is off and any `clickhouse.addr` is not loopback. With `clickhouse.tls.enabled: true` the connection is TLS (`ca_file`, client `cert_file`/`key_file`, `server_name`, `insecure_skip_verify` as usual; bare enabled = system root CAs, per-address host verification).
```

- [ ] **Step 2: Update the roadmap bullet**

The "ClickHouse® Cloud verification" bullet currently says the TLS port "must be wired into `chtest.Options` and `clickhouse.*` config (a `tls` key)" — that wiring now exists. Rewrite so the bullet asks only for what remains: a Cloud account, and running unit suite + oracle against it with `RUNAKEFLI_TEST_CH_ADDR=<host>:9440 RUNAKEFLI_TEST_CH_TLS=1 RUNAKEFLI_TEST_CH_USER/PASSWORD=...`.

- [ ] **Step 3: CHANGELOG**

Under `## [Unreleased]`, add an `### Added` section (above `### Changed`, Keep a Changelog order) if it does not exist, then:

```markdown
- `clickhouse.tls` (`enabled`, `ca_file`, `cert_file`, `key_file`, `server_name`, `insecure_skip_verify`): the native ClickHouse connection can now use TLS (e.g. native-TLS servers and ClickHouse Cloud's port 9440), with the material read and validated at startup like `password_file`. Until now the transport was cleartext-only. Rúnakefli also warns at startup when TLS is off and `clickhouse.addr` names a non-loopback address (#45).
```

Also drop the `(45)`-tagged `### Changed` line added on 2026-11-21 that records the cleartext-only divergence *as a new entry* (it describes the pre-fix state that this release changes; keep the divergence page as the record).

- [ ] **Step 4: Verify**

Run: `make docs` and `go test ./internal/config/ -count=1`
Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add docs/divergences.md docs/roadmap.md CHANGELOG.md
git commit -m "docs: ClickHouse transport with the new clickhouse.tls block (#45)"
