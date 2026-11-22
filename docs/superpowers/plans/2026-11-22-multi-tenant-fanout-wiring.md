# Multi-Tenant Index Fan-Out Wiring (#42) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `query.querier.multi_tenant_queries_enabled: true` fan out the index and drilldown endpoints (`index/stats`, `index/volume`, `index/volume_range`, `detected_labels`, `patterns`) per tenant, as the docs promise, by wiring the existing-but-never-set `api.API.MultiTenant` switch in `server.newAPI`.

**Architecture:** All the fan-out code already exists and is unit-tested: `multitenant.Index` (`internal/multitenant/index.go`, one call per tenant, merged as Loki merges) wraps the per-tenant adapter `tenantIndex` (`internal/api/index.go`) when `api.API.MultiTenant` is true, and the codec handler routes the five endpoints through `api.API.index()` (`internal/api/lokicodec.go:22-49`). The only missing piece is production wiring: `server.newAPI` builds the fan-out querier for the engine and `Meta` (`internal/server/server.go:240-244`) but never sets `MultiTenant` on the `api.API` literal (`server.go:246-269`). This plan adds that one line, plus an end-to-end regression test through the real `server.Run` path — the existing unit tests never catch this class of bug because they self-wire the field (`internal/multitenant/querier_test.go:85` sets `MultiTenant: mt` on a hand-built API).

**Tech Stack:** Go, ClickHouse through `chtest` (a live server is required, see Global Constraints), end-to-end tests via `server.Run` + `net/http`.

**Spec:** https://github.com/bzed/runakefli/issues/42 — the issue and its validation comment contain the full evidence; `docs/divergences.md`'s current "Multi-tenant queries" entry (corrected in commit `5c69976`) documents the broken scope this plan removes.

## Global Constraints

From `AGENTS.md` (read it before starting):

- `make check` must pass before push. Unit tests need a ClickHouse at
  `localhost:9000` (if it is not serving:
  `podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9`).
- No new third-party dependencies.
- Conventional commits (`fix:`, `test:`, `docs:`), one logical change per commit; every commit leaves the tree buildable and tests green.
- This fix is user-visible because it makes documented behaviour real: it carries a `CHANGELOG.md` line under `## [Unreleased]`.
- Tests never write outside `t.TempDir()`, no sleeps — poll with `require.Eventually` when waiting for state.
- Where docs and this plan disagree with Loki naming or semantics, Loki wins; use exact Loki error messages.

## Review Focus

1. `index/volume_range` needs no extra wiring: it is decoded to the same `*logproto.VolumeRequest` branch that `/index/volume` takes (`internal/api/lokicodec.go:27-32`), so the `Volume` fan-out (`internal/multitenant/index.go:59-66`) covers both endpoints. The e2e test therefore covers `/index/volume`, not `/index/volume_range`.
2. Flag OFF must keep rejecting multi-tenant ids — `TestMultiTenant_IndexDisabledRejects` (`internal/multitenant/querier_test.go:209-215`) must keep passing; Task 2 strengthens it to pin the exact status code.
3. The engine path (`query`, `query_range`, `series`, labels, `detected_fields`) must not change: `internal/pushdown/engine.go:86-94` already routes multi-tenant requests to Tier B. Run `go test ./internal/api/... ./internal/pushdown/... ./internal/server/... ./internal/multitenant/...` after the wiring.
4. If the new e2e test is flaky because a just-pushed line is not yet visible to `index/stats` (async insert flushing), poll the first multi-tenant GET with `require.Eventually` (e.g. 10 s, 250 ms tick) instead of sleeping. For the red run, a plain assertion is clearer: it fails fast with the 500 body.
5. `Patterns` and `Tail` keep the plain `*chstore.Querier` (`server.go:266`, `:271`): patterns fans out through `tenantIndex.Patterns` (`internal/api/index.go:49-51`) once `MultiTenant` is set; tail stays single-tenant (nothing ever promised tail fan-out). Do not retype those fields.

---

### Task 1: Wire the fan-out behind an end-to-end regression test

**Files:**
- Create: `internal/server/multitenant_fanout_test.go` (package `server_test`)
- Modify: `internal/server/server.go` — the `api.API` literal in `newAPI`, lines 246-269
- Modify: `internal/multitenant/querier_test.go` — the stale `newMTServer` comment (`:29-31`) and `TestMultiTenant_IndexDisabledRejects` (`:209-215`)

**Interfaces:**
- Consumes (all in `internal/server/server_test.go`): `testConfig(t *testing.T, serverYAML, extra string) *config.Config` (`:37`), `start(t *testing.T, cfg *config.Config, logger log.Logger) *running` (`:88`, `*running.addr` is `host:port`), `push(t *testing.T, addr, tenant, body string) int` (`:125`), `pushBody(ts time.Time, line string) string` (`:121`), `defaultServer` (`:33`).
- Produces: none (this is the last code task's dependency root; `server.newAPI` behaviour is the deliverable).

- [ ] **Step 1: Write the failing test**

```go
package server_test

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestRun_MultiTenantFanout(t *testing.T) {
	cfg := testConfig(t, defaultServer,
		"query:\n  querier:\n    multi_tenant_queries_enabled: true\n")
	r := start(t, cfg, log.NewNopLogger())
	now := time.Now()
	require.Equal(t, http.StatusNoContent, push(t, r.addr, "t1", pushBody(now, "one")))
	require.Equal(t, http.StatusNoContent, push(t, r.addr, "t2", pushBody(now, "two")))

	get := func(path, tenant string) (int, string) {
		t.Helper()
		v := url.Values{"query": {`{app="a"}`},
			"start":  {fmt.Sprint(now.Add(-time.Hour).UnixNano())},
			"end":    {fmt.Sprint(now.Add(time.Minute).UnixNano())}}
		req, err := http.NewRequest(http.MethodGet, "http://"+r.addr+path+"?"+v.Encode(), nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", tenant)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(b)
	}

	// The index and drilldown endpoints fan out per tenant, merged like Loki.
	code, body := get("/loki/api/v1/index/stats", "t1|t2")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"entries":2`)

	code, body = get("/loki/api/v1/index/volume", "t1|t2")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"app":"a"`)

	code, body = get("/loki/api/v1/patterns", "t1|t2")
	require.Equal(t, http.StatusOK, code, body)

	// A single tenant stays scoped to its own data.
	code, body = get("/loki/api/v1/index/stats", "t1")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"entries":1`)
}
```

- [ ] **Step 2: Run the test to verify it fails for the documented reason**

Run: `go test ./internal/server/ -run TestRun_MultiTenantFanout -count=1 -v`
Expected: FAIL on the first `index/stats` assertion — status 500, body contains `multiple org IDs present` (the fan-out switch is never set). If it fails with `clickhouse not reachable`, start the server per Global Constraints first.
If the push or storage is not yet visible (flaky), apply Review Focus item 4 to the first fan-out GET only, and re-run to confirm the failure is still the 500.

- [ ] **Step 3: Implement the wiring**

In `internal/server/server.go`, in the `api.API` literal of `newAPI` (lines 246-269), add one field next to `AuthEnabled` (line 268):

```go
		AuthEnabled: cfg.AuthEnabled,
		MultiTenant: cfg.Query.Querier.MultiTenantQueriesEnabled,
```

Nothing else in `newAPI` changes. The engine/Meta wiring at lines 240-244 stays as is.

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/server/ -run TestRun_MultiTenantFanout -count=1 -v`
Expected: PASS. All five assertions hold: fan-out merged on `index/stats`, `index/volume`, `patterns` and single-tenant scoping on `index/stats`.

- [ ] **Step 5: Run the neighbouring packages**

Run: `go test ./internal/server/... ./internal/multitenant/... ./internal/api/... -count=1`
Expected: PASS (no behaviour change for the engine path or single-tenant requests).

- [ ] **Step 6: Strengthen the disabled-rejection test**

In `internal/multitenant/querier_test.go`, `TestMultiTenant_IndexDisabledRejects` (lines 209-215) asserts only `require.NotEqual(t, http.StatusOK, code)`. Replace that with the exact status the multi-tenant rejection produces on this path, so a regression to any other code fails loudly:

```go
	code, body := indexGet(t, srv.URL, "/loki/api/v1/index/stats", "t1|t2", `{app="a"}`, now)
	require.Equal(t, http.StatusInternalServerError, code, body)
	require.Contains(t, body, "multiple org IDs present")
```

Also update the stale comment above `newMTServer` (lines 29-31): it says the helper builds the API *"the way server.newAPI will after Task 10 wiring"* — reword to state that production `server.newAPI` sets the same fields (`newMTServer` mirrors its wiring, only with a test `config`).

Run: `go test ./internal/multitenant/... -count=1`
Expected: PASS. If the observed code on the disabled path is not 500, pin the observed value — the invariant is that both flag values produce the same rejection on this endpoint while the flag is off.

- [ ] **Step 7: Commit**

```bash
git add internal/server/multitenant_fanout_test.go internal/server/server.go internal/multitenant/querier_test.go
git commit -m "fix: fan the index and drilldown endpoints out per tenant when multi-tenant queries are on (#42)"
```

### Task 2: Restore the documented scope (docs + CHANGELOG)

**Files:**
- Modify: `docs/divergences.md` — "Multi-tenant queries" entry (currently narrowed by commit `5c69976`)
- Modify: `docs/reference/configuration.md` — the `query.querier.multi_tenant_queries_enabled` row (currently `:164`)
- Modify: `CHANGELOG.md` — `## [Unreleased]`

**Interfaces:**
- Consumes: Task 1's wiring (the docs statements become true).
- Produces: docs consistent with behaviour (the `TestConfigurationDocsCoverEveryKey` test is untouched: no keys are added or removed).

- [ ] **Step 1: Update the divergence entry**

In `docs/divergences.md`, replace the "Multi-tenant queries" bullet with the entry restored to the (now true) scope, keeping the corrections commit `5c69976` added (engine-path wording, `detected_fields`):

```markdown
- **Multi-tenant queries** (`query.querier.multi_tenant_queries_enabled`): Rúnakefli reuses Loki's `pkg/querier/multi_tenant_querier.go` fan-out for the log and sample select paths (`query`, `query_range`), `label/values*`, `series`, `detected_fields` (through the query engine), `index/stats`, `index/volume`, `index/volume_range`, `detected_labels` and `patterns`. Rúnakefli does not serve `detected_metric_names`, and the tail is single-tenant. With the flag on, multi-tenant queries always run on the plain engine (no ClickHouse push-down, which is single-tenant by construction).
```

- [ ] **Step 2: Update the configuration reference row**

In `docs/reference/configuration.md`, in the `query.querier.multi_tenant_queries_enabled` row, replace the "It does **not** cover … (#42)" sentence (everything after `detected_fields`.) with:

```markdown
This also applies to `index/stats`, `index/volume`, `index/volume_range`, `detected_labels` and `patterns`; the tail is single-tenant.
```

- [ ] **Step 3: CHANGELOG**

In `CHANGELOG.md` under `## [Unreleased]`: remove the `### Changed` entry about the fan-out scope that mentions #42 (it described the gap this commit closes), and add under `### Fixed`:

```markdown
- The index and drilldown endpoints (`index/stats`, `index/volume`, `index/volume_range`, `detected_labels`, `patterns`) now fan out per tenant with `query.querier.multi_tenant_queries_enabled: true`, as documented since 0.7.0. The server wiring was missing, so they answered `multiple org IDs present` (500) even with the flag on (#42).
```

- [ ] **Step 4: Verify**

Run: `make docs` (mkdocs `--strict`) and `go test ./internal/config/ -count=1`
Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add docs/divergences.md docs/reference/configuration.md CHANGELOG.md
git commit -m "docs: multi-tenant index fan-out works again; #42 is fixed"
```
