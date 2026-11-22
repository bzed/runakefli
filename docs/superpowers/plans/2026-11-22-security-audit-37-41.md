# Security audit fixes #37–#41 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the five open security-audit issues (#37 auth-disabled tenant selection, #38 filter-only deletes, #39 unbounded in-flight push bytes, #40 process-wide tail limit, #41 OTLP nil resolver panic) on branch `fix/security-audit-37-41`, one commit per issue.

**Architecture:** Each fix moves runakefli back onto Loki's behaviour by reusing (or copying with a header) the pinned Loki code: fakeauth semantics in `internal/api`, Loki's `shouldProcessRequest` in the delete processor, Loki's `distributor.max_inflight_bytes` + `limits_config.max_push_size` in ingest, Loki's per-tenant tailer count, and Loki's request-scoped stream resolver for the push parsers.

**Tech Stack:** Go, Loki @ `acaa752c17c1560eead5a33d04d63ed877d4a6de` (do not bump), dskit, ClickHouse 26.9 for tests.

**Spec:** GitHub issues #37, #38, #39, #40, #41 (`gh issue view <n>`). Read the issue before its task.

## Global Constraints

- Follow `AGENTS.md` exactly: `make check` passes before every commit (gofmt/goimports, vet, golangci-lint, govulncheck, `-race` tests, coverage ≥ 85 %).
- ClickHouse for tests: `podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9`.
- Tasks 2, 3 and 5 touch ingest/delete/query code: also run `make test-cluster`, `make oracle-all`, `make oracle-cluster` before the final commit of the branch.
- Never bump `LOKI_COMMIT`. Code copied from Loki starts with `// Copied from grafana/loki@acaa752c17c1560eead5a33d04d63ed877d4a6de <path>; modifications: <list>`.
- Loki's exact config keys, metric names and error strings wherever Loki has them.
- Every user-visible change adds a line to `CHANGELOG.md` under `## [Unreleased]` → `### Fixed` (create the heading if missing) in the same commit, referencing the issue (`(#37)`).
- Every remaining observable difference from Loki goes into `docs/divergences.md` in the same commit; a divergence that the fix removes is deleted from it.
- TDD: write the test, watch it fail, then implement. No sleeps in tests; use `require.Eventually`. Never `t.Parallel()` in ruler tests.
- Commit messages: `fix: <summary> (#N)`, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Temporary files only under `/tmp` or `t.TempDir()`, never `$HOME`.

## Review Focus

1. **OTLP protobuf bodies** (`Content-Type: application/x-protobuf`), not just JSON, with ≥1 record must be stored — real exporters send protobuf. (Task 1 test.)
2. **The in-flight counter must return to 0 on every exit path** (400 parse error, 429 rate limit, 500 write failure, 204) and for bodies without `Content-Length` (chunked) — a leak permanently 503s ingest. (Task 3 test.)
3. **A tail/TailersCount context without an org ID** must return an error, never panic or count under a shared key. (Task 4 test.)
4. **A stored delete request whose tenant's mode later becomes `disabled` or invalid** must not be physically applied by the processor and must stay `received`. (Task 2 test.)
5. **`auth_enabled: true` behaviour is unchanged**: missing header → 401, `a|b` still selects two tenants with multi-tenant queries on. (Task 5 test.)

---

### Task 1: OTLP push gets a real stream resolver (#41)

**Files:**
- Create: `internal/ingest/resolver.go`
- Modify: `internal/ingest/parse.go` (copy header + `parsePush` signature, line 63–72), `internal/ingest/handler.go:63`
- Test: `internal/ingest/handler_test.go`

**Interfaces:**
- Produces: `func newStreamResolver(tenantID string, limits *validation.Overrides, logger log.Logger) push.StreamResolver` (unexported) and `parsePush(r, logger, tenantID, limits, parser, format, maxRecvMsgSize, maxDecompressedSize, resolver push.StreamResolver)`. Task 3 calls `parsePush` with this extra argument.

- [ ] **Step 1: Write the failing tests** in `internal/ingest/handler_test.go`:
  - `TestHandler_OTLPJSONWithRecordIsStored`: POST `/otlp/v1/logs`, `Content-Type: application/json`, body with one resourceLogs → scopeLogs → one logRecord (`body.stringValue: "hello"`, `timeUnixNano` = now), resource attribute `service.name: "svc"`. Assert status `204` and that a query for the tenant returns exactly one row with line `hello` (use the existing ClickHouse test helpers in that file).
  - `TestHandler_OTLPProtobufWithRecordIsStored`: same record built with `go.opentelemetry.io/collector/pdata/plog` (`plogotlp.NewExportRequestFromLogs(...).MarshalProto()`), `Content-Type: application/x-protobuf`. Same assertions. (Review Focus 1.)
- [ ] **Step 2: Run** `go test -race ./internal/ingest -run 'TestHandler_OTLP'` — expected FAIL: client error `EOF` / panic `nil pointer dereference` in `otlp.go:271`.
- [ ] **Step 3: Implement** `internal/ingest/resolver.go`: copy Loki's `requestScopedStreamResolver` + `newRequestScopedStreamResolver` from `pkg/distributor/distributor.go:1888-1932` (uses `retention.NewTenantRetentionSnapshot(limits, userID)` from `pkg/compactor/retention` and `limits.PoliciesStreamMapping(userID)`), with the copy header (modifications: renamed, takes `*validation.Overrides`). In `parsePush` pass `resolver` as the parser's StreamResolver argument (tracker stays `nil`; Loki's parsers nil-check the tracker). In `Handler.ServeHTTP` build it once per request: `newStreamResolver(tenantID, h.Limits, h.Logger)`. Update the `parse.go` copy header: drop "stream resolver" from the "no … " list.
- [ ] **Step 4: Run** the two tests plus `go test -race ./internal/ingest/...` — expected PASS. Existing metrics tests may now see non-empty `retention_hours`/`policy` labels as Loki would; update their expectations to Loki's values, do not special-case.
- [ ] **Step 5: CHANGELOG** `### Fixed`: "`POST /otlp/v1/logs` no longer aborts the connection on every request with log records; OTLP ingest works (#41)."
- [ ] **Step 6:** `make check`, then commit `fix: pass a stream resolver to the push parsers; OTLP ingest no longer panics (#41)`.

### Task 2: deletion_mode filter-only never physically deletes (#38)

**Files:**
- Modify: `internal/deletes/processor.go` (struct + `ProcessDue`), `internal/server/server.go:112-119`
- Test: `internal/deletes/processor_test.go` (helper `buildProcessor` line 38, `TestProcessor_PhysicallyRemovesRows` line 309)

**Interfaces:**
- Produces: new required field `Processor.Limits deletionLimits` (the existing interface in `handler.go`: `DeletionMode(userID string) string`). `server.go` passes `limits` (`*validation.Overrides`).

- [ ] **Step 1: Write the failing tests.** Give `buildProcessor` a `mode string` parameter and a tiny test type `modeLimits string` whose `DeletionMode` returns it; existing callers pass `"filter-and-delete"`.
  - `TestProcessor_FilterOnlyKeepsRows`: seed tenant rows like `TestProcessor_PhysicallyRemovesRows`, mode `"filter-only"`, add a delete request for the matching selector, `ProcessDue`. Assert the row count on disk is unchanged and `store.Unprocessed(ctx)` still returns the request (status `received`).
  - `TestProcessor_DisabledOrInvalidModeKeepsRows`: table test over `"disabled"` and `"bogus"`: same assertions, and `ProcessDue` returns no error for `"disabled"`; for `"bogus"` it may return an error but must not delete. (Review Focus 4.)
  - `TestProcessor_PhysicallyRemovesRows` keeps its assertions with mode `"filter-and-delete"`.
- [ ] **Step 2: Run** `go test -race ./internal/deletes -run TestProcessor` — expected FAIL: rows deleted under filter-only.
- [ ] **Step 3: Implement** in `ProcessDue`, before `p.process`: skip the request unless Loki's predicate holds — copy `shouldProcessRequest` from `pkg/compactor/deletion/delete_requests_manager.go:446-457` (`deletionmode.ParseMode(p.Limits.DeletionMode(r.UserID))`, process only `== deletionmode.FilterAndDelete`) with the copy header; on parse error log `"unable to determine deletion mode for user"` and record it as the pass error, continue. A skipped request is neither processed nor marked. Update the `Processor` doc comment. Wire `Limits: limits` in `server.go`.
- [ ] **Step 4: Run** the deletes tests — PASS. Then `go test -race ./internal/server/...`.
- [ ] **Step 5: Docs.** `docs/reference/configuration.md:177-179` already describes filter-only correctly; check it still matches. No divergence entry is needed (this is Loki's behaviour); remove any divergence text claiming otherwise. CHANGELOG `### Fixed`: "`deletion_mode: filter-only` no longer physically deletes rows; only `filter-and-delete` tenants' requests are applied by the processor (#38)."
- [ ] **Step 6:** `make check`, commit `fix: only apply delete requests physically for filter-and-delete tenants (#38)`.

### Task 3: in-flight push byte budget and Loki's push size cap (#39)

**Files:**
- Modify: `internal/config/config.go` (new block + validation), `internal/server/server.go` (handler wiring ~line 263), `internal/ingest/handler.go`
- Create: `internal/ingest/inflight.go` (budget + its two collectors)
- Modify docs: `docs/reference/configuration.md` (new `distributor` section; `limits_config.max_push_size` now bounds the decompressed body), `docs/divergences.md:13`, `docs/operations.md` (memory-limit advice), `runakefli.example.yaml`, `docker/runakefli.yaml`
- Test: `internal/ingest/handler_test.go`, `internal/config/config_test.go`

**Interfaces:**
- Consumes: `parsePush(..., resolver)` from Task 1.
- Produces: `config.Config.Distributor DistributorConfig` with `MaxInflightBytes int \`yaml:"max_inflight_bytes"\`` (top-level YAML key `distributor`; every other key under it still fails strict decoding). `ingest.Handler.MaxInflightBytes int64` (0 = disabled, Loki's default).

Behaviour (Loki `pkg/distributor/http.go:104-164` plus one tightening):
- Before parsing: if `MaxInflightBytes > 0 && inflight >= MaxInflightBytes` → `503` with Loki's text `the server cannot accept more requests at this time`; the body is not read.
- On admission, reserve `min(r.ContentLength, recvSize)` (0 when unknown); after a successful parse, adjust the reservation to `int64(req.Size())`; release everything with `defer` on every exit path. (Loki reserves only after parsing; reserving at admission is the divergence that lets a concurrent push be refused while the first is still decompressing. Record it.)
- Decompressed-size cap: when `MaxDecompressedSize == 0`, use `int64(h.Limits.MaxPushSize(tenantID))` (Loki's `limits_config.max_push_size`, default 2GB) instead of the 5 GiB constant; delete `maxDecompressedSize`. `MaxRecvMsgSize` stays 100 MiB.
- Metrics, package-level in `inflight.go` with Loki's names and help strings from `pkg/distributor/metrics.go:245-255`: gauge `loki_distributor_max_inflight_bytes` (set when the handler is built/first used) and summary `loki_distributor_inflight_bytes_high_watermark` (observe the new total on each reservation).
- Config validation: negative value → Loki's error `max inflight bytes cannot be less than zero`.

- [ ] **Step 1: Write the failing tests.**
  - `config_test.go` `TestLoad_DistributorMaxInflightBytes`: `distributor:\n  max_inflight_bytes: 1048576\n` loads to `1048576`; `-1` fails with `max inflight bytes cannot be less than zero`; `distributor:\n  foo: 1\n` still fails.
  - `handler_test.go` `TestHandler_InflightBudgetRejectsBeforeReading`: handler with `MaxInflightBytes: 1024`. Request A: body is an `io.Pipe` reader, `ContentLength = 2048`; serve it in a goroutine and block after writing the first byte (signal on a channel once the first byte was consumed). While A is blocked, request B with a body reader that records whether `Read` was called: assert `503`, Loki's message, and `Read` never called. Unblock A (close the pipe with a valid small push), assert A finishes, then request C (a valid push) returns `204`.
  - `TestHandler_InflightReleasedOnEveryOutcome`: `MaxInflightBytes: 1` (any reservation exceeds it); send sequentially a malformed body (400), an over-burst push with a rate limiter (429), a push with no `ContentLength` (`-1`, chunked) (204), and a valid push (204) — every request after the first is admitted, proving the counter returned to 0 each time. (Review Focus 2.)
  - `TestHandler_DecompressedCapIsMaxPushSize`: limits with `max_push_size: 1KB`, gzip body decompressing to 2 KiB → `400` whose message contains `ErrRequestBodyTooLarge`'s text.
- [ ] **Step 2: Run** `go test -race ./internal/ingest ./internal/config -run 'Inflight|DecompressedCap|Distributor'` — FAIL (unknown field / no 503).
- [ ] **Step 3: Implement** as specified above; wire `MaxInflightBytes: int64(cfg.Distributor.MaxInflightBytes)` in `server.go`. Keep the counter an `atomic.Int64` owned by the `Handler` (document it on the field). Comment copied Loki lines with the copy header.
- [ ] **Step 4: Run** the tests — PASS; then `go test -race ./internal/...`.
- [ ] **Step 5: Docs and shipped config.**
  - `configuration.md`: new `## distributor` table with `distributor.max_inflight_bytes` (int, default `0` = disabled, Loki semantics + admission-time reservation); note under `limits_config` that `max_push_size` caps the decompressed push body.
  - `divergences.md:13`: the `distributor` block now exists with only `max_inflight_bytes`; other distributor keys are still rejected. Add a bullet: in-flight bytes are reserved from `Content-Length` at admission (Loki reserves the parsed size only after parsing).
  - `runakefli.example.yaml` and `docker/runakefli.yaml`: set `distributor: max_inflight_bytes: 536870912` (512 MiB) with a one-line comment.
  - `operations.md`: short paragraph recommending `GOMEMLIMIT` and a systemd `MemoryMax=`/container memory limit sized above `max_inflight_bytes`. (No packaging change to `debian/runakefli.service`: a fixed limit cannot fit every host.)
  - CHANGELOG `### Fixed`: "Pushes are bounded: new `distributor.max_inflight_bytes` (Loki's key) refuses pushes with 503 while too many bytes are in flight, and the decompressed body is capped by `limits_config.max_push_size` instead of 5 GiB (#39)."
- [ ] **Step 6:** `make check`, commit `fix: bound in-flight push bytes and cap decompression at max_push_size (#39)`.

### Task 4: max_concurrent_tail_requests per tenant (#40)

**Files:**
- Modify: `internal/tail/ingester.go:28-96`
- Test: `internal/tail/ingester_test.go`

**Interfaces:**
- Produces: unchanged exported API. `Ingester.active` becomes `map[string]int` (guarded by `mu`, lazily allocated); `Tail` and `TailersCount` read the tenant with `tenant.TenantID(ctx)` (`github.com/grafana/dskit/tenant`) and return its error when absent.

- [ ] **Step 1: Write the failing tests** in `ingester_test.go`:
  - `TestIngester_TailersCountPerTenant`: two `Tail` calls with `user.InjectOrgID(ctx, "a")`, one with `"b"`. Assert `TailersCount(ctxA) == []uint32{2}`, `ctxB → {1}`, `ctxC ("c") → {0}`. Cancel both `a` contexts; `require.Eventually` that `a` reports `{0}` and `b` still `{1}`.
  - `TestIngester_NoOrgID`: `Tail(context.Background(), req)` and `TailersCount(context.Background())` both return a non-nil error and no panic. (Review Focus 3.)
- [ ] **Step 2: Run** `go test -race ./internal/tail -run 'TailersCount|NoOrgID'` — FAIL (`b` sees 3).
- [ ] **Step 3: Implement**: capture the tenant at increment, decrement the same key in the goroutine, delete the key at zero; keep the uint32 clamp. Goroutine ownership unchanged (it ends with the tail context).
- [ ] **Step 4: Run** `go test -race ./internal/tail/... ./internal/server/...` — PASS.
- [ ] **Step 5: Docs.** This now matches Loki (per-tenant ingester counts); no divergence entry. CHANGELOG `### Fixed`: "`max_concurrent_tail_requests` is enforced per tenant; one tenant's tails no longer block every other tenant's (#40)."
- [ ] **Step 6:** `make check`, commit `fix: count concurrent tails per tenant (#40)`.

### Task 5: auth_enabled=false ignores X-Scope-OrgID (#37)

**Files:**
- Modify: `internal/api/api.go:156-168` (`tenant`), `docs/reference/configuration.md:53`
- Test: `internal/api/api_test.go`; fix any test under `internal/` that sends `X-Scope-OrgID` with `AuthEnabled: false` expecting that tenant (switch it to `AuthEnabled: true`; candidates: `internal/api/*_test.go`, `internal/deletes/list_test.go`, `internal/server/*_test.go`, `internal/multitenant/querier_test.go`)

**Interfaces:**
- Produces: unchanged API. With `AuthEnabled == false` the wrapper never reads the header and injects the no-auth tenant `"fake"` (Loki's `fakeauth`, `pkg/util/fakeauth/fake_auth.go`). With `AuthEnabled == true` unchanged.

- [ ] **Step 1: Write the failing test** `TestAPI_AuthDisabledIgnoresOrgIDHeader` in `api_test.go`: router with `AuthEnabled: false` and an endpoint that reveals the tenant (reuse whatever `TestAPI_LabelsAndReady` uses to observe the tenant). Table: no header → `fake`; `victim` → `fake`; `a|b` (with `MultiTenant: true`) → `fake` only. Plus `TestAPI_AuthEnabledHonoursOrgIDHeader`: `AuthEnabled: true`: no header → `401 no org id`; `t1` → `t1`; `a|b` with `MultiTenant: true` → both tenants. (Review Focus 5.)
- [ ] **Step 2: Run** `go test -race ./internal/api -run 'AuthDisabled|AuthEnabled'` — FAIL (`victim` returned).
- [ ] **Step 3: Implement** the override branch exactly as in issue #37's "Smallest fix" (inject `"fake"` and return when auth is disabled; otherwise require the header).
- [ ] **Step 4: Run** `go test -race ./internal/... ./test/...` with ClickHouse; fix tests that relied on the old fallback by enabling auth in their fixture, not by changing the wrapper.
- [ ] **Step 5: Docs.** `configuration.md:53`: "When `false`, the header is ignored and every request runs as tenant `fake`, as in Loki." Remove any divergence about this from `divergences.md` (none should remain). CHANGELOG `### Fixed`: "With `auth_enabled: false`, `X-Scope-OrgID` is ignored and every request uses tenant `fake`, as in Loki; clients can no longer select a tenant (#37)." Also under `### Changed` if a user relying on the header must now enable auth: say so in the same line.
- [ ] **Step 6:** `make check`, then (branch-final) `make test-cluster`, `make oracle-all`, `make oracle-cluster`; commit `fix: ignore X-Scope-OrgID when auth is disabled, as Loki does (#37)`.

### Task 6: Branch verification and PR (controller, not implementer)

- [ ] Whole-branch review of `git diff main...` against the five issues and Review Focus.
- [ ] `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster` green.
- [ ] Push `fix/security-audit-37-41`, open a PR titled `fix: security audit findings #37–#41` with `Closes #37` … `Closes #41`, ending with the Claude Code attribution line.
