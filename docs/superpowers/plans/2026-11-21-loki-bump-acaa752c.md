# Loki Bump to acaa752c Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the pinned Loki commit from `39c256e50a804c045d5eb4e3c817a31653b535fe` (2026-10-27) to the current `main`, `acaa752c17c1560eead5a33d04d63ed877d4a6de` (2026-11-20); fix the discard double-counting Loki fixed upstream; hide Loki's internal labels from the label-name APIs as Loki now does; record the Loki features Rúnakefli does not implement in `docs/divergences.md`.

**Architecture:** Six commits: characterisation tests at the old pin (Task 1), the mechanical bump (Task 2), the discard-accounting fix (Task 3), the level-detection re-sync (Task 4), the label hiding (Task 5), the docs (Task 6). Of the 424 upstream commits only a few touch what Rúnakefli imports or copies. One package move (`pkg/logqlmodel` → `pkg/logql/model` and `pkg/logql/errors`) breaks the build; everything else is inherited through Loki's engine and parser. Copies in `internal/ingest/distcopy` that Loki changed are re-synced in their own commits.

**Tech Stack:** Go 1.27.4 (unchanged; Loki's `go.mod` is 1.27.4 too), Loki `pkg/logql`, `pkg/loghttp/push`, `pkg/util/constants`, ClickHouse 26.9 for the tests.

**Spec:** No spec file. Findings of the trial bump on 2026-11-21 (scratch worktree, discarded) and of an independent review of this plan:
- `go get …@<sha>` + `go mod tidy` falls back to the tag `v3.7.10` as soon as any import (also in files with the `oracle` or `bench` build tags) names a package the new commit removed; the build then fails inside Loki's own `ruler/storage/wal` and `storage/bucket/azure`. Fix: edit `go.mod` by hand, migrate all imports (including tagged files), then tidy.
- After the migration `go build ./...`, `go vet ./...` and `go test ./...` (against a local ClickHouse) were clean. The oracle and `go vet -tags oracle,bench ./test/...` were **not** run.
- Loki's `go.mod` replaces that changed since the pin: `thanos-io/objstore` → `github.com/grafana/objstore v0.0.0-20261106201104-4e98ee68171c`, `twmb/franz-go` → `v1.22.2`, `twmb/franz-go/pkg/kmsg` → `v1.15.0`. All other replaces are unchanged.
- Moved symbols. Rúnakefli's complete use of `logqlmodel.` is: `Result` (54), `Streams` (15), `NewSeriesLimitError` (3), `LimitError`, `ErrMaxQueryLength`, `ErrMaxEntriesLimit`, `ErrUnsupportedSyntaxForInstantQuery`. Rule: only `Result`, `Streams` and `ValueTypeStreams` stay in `pkg/logql/model` (import alias `logqlmodel` is kept); every other `logqlmodel.X` becomes `logqlerrors.X` from `pkg/logql/errors`; `…/logqlmodel/stats` and `…/metadata` move to `…/logql/model/stats` and `…/metadata`.
- Copied functions verified identical (apart from imports and the error package) between the two commits: `validateMaxEntriesLimits`, `validateMatchers`, `seriesLimitPartialResultsWarning`, `getVolume`, `countLabelsAndCardinality`, `LocalEvaluator.Eval`, `prunePatterns`, the delete-request handler files, the format-query handler, the multi-tenant fan-out, `ingestion_rate_strategy.go`, `split_by_interval.go` functions.
- Copies Loki **changed**: `process.go` (`parseStreamLabels` split into `parseStreamLabels` + `parseAndValidateStreamLabels`; `validateStreams`/`pushWithResolver` split into stages; `processStreamEntries` only renames `reportDiscardedDataWithTracker` → `reportDiscardedData`; #26317, #26316, #26324), `validator.go` (`ValidateLabels` returns `(reason, error)` and reports nothing; `IsInternalStream` → `constants.IsInternalStream`), `field_detection.go` (level detection rewritten allocation-free), `parse.go` (the parse part of `pushHandler` gained the `max_inflight_bytes` check, #25748; Rúnakefli has no such limit).
- **Rúnakefli double-counts label-validation discards today:** `distcopy/validator.go:181` reports `max_label_names_per_series` (and `label_name_too_long`, `label_value_too_long`, `duplicate_label_names`), then `distcopy/process.go:150` reports the same stream again as `invalid_labels`; a `{}` stream gets `missing_labels` (1 sample, 0 bytes) plus `invalid_labels`. Loki fixed this in #26317 (one report, under `ValidateLabels`' reason).
- Histogram functions parse but are not evaluated upstream. With `engine.enable-histograms` off (the default; Rúnakefli has no such key) Loki's engine returns `errHistogramsDisabled`, an `ErrParse` error with the message "histogram functions are experimental and not enabled, set -querier.engine.enable-histograms to enable them". `internal/pushdown/mapper.go` returns `ok=false` for unknown node types, so Rúnakefli falls back to the engine and fails the same way (the message names a flag Rúnakefli does not have: a divergence, Task 6).
- Loki now hides `__aggregated_metric__`, `__pattern__`, `__time_shard__` and `__stream_shard__` from the label-name and `detected_labels` responses unconditionally (`pkg/querier/http.go`); label *values* stay served. **`series` filtering is not unconditional:** it drops pattern-ingester streams only when the tenant has `limits_config.metric_aggregation_enabled` (default false) and no match group selects `__aggregated_metric__`; that is unchanged since the pin and out of scope.
- Config keys removed from Loki: `limits_config.max_chunks_per_query` (`-store.query-chunk-limit`). Added and irrelevant: `dataobj_deletion_sweep_enabled`, `tsdb_sharded_index_shard_factor`.

## Global Constraints

- This bump is its own change (AGENTS.md: "Never bump it in passing"): commits of its own, `make check` passing on each.
- New pin: `acaa752c17c1560eead5a33d04d63ed877d4a6de` (2026-11-20 23:00:41Z, "fix(deps): Update module golang.org/x/net to v0.63.0 (main) (#26652)"). Pseudo-versions: `github.com/grafana/loki/v3 v3.0.0-20261120230041-acaa752c17c1`, `github.com/grafana/loki/pkg/push` replace `v0.0.0-20261120230041-acaa752c17c1`. If `main` has moved when this runs, take its tip, recompute both, and compare the commits in between for the paths of Task 2 Step 4.
- The pin appears in: `Makefile` `LOKI_COMMIT`, `test/oracle/loki.go` `lokiCommit`, `go.mod`/`go.sum`, `scripts/govulncheck-ignore.txt` (comment), `internal/deletes/client.go` (comment), and the `// Copied from grafana/loki@…` headers. Short SHAs follow each file's existing style (`39c256e5` → `acaa752c`, `39c256e50a80` → `acaa752c17c1`, the full SHA where the file used it). **Left alone on purpose:** `internal/chstore/regexsimplify_test.go:36` and `test/oracle/compare.go:157` ("since Loki 39c256e5") are historical statements.
- A header moves to the new SHA only for functions verified identical or re-synced. A copy left behind keeps the old SHA until its re-sync commit.
- `go.mod` replaces mirror Loki's `go.mod` at the new commit.
- Observable differences from Loki are recorded in `docs/divergences.md` in the same change (AGENTS.md).
- No new third-party dependency. No `t.Parallel()` in ruler tests.
- Gates: `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster`. ClickHouse for tests: `podman run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 docker.io/clickhouse/clickhouse-server:26.9` (or the one already on `127.0.0.1:9000`). The oracle builds Loki at `lokiCommit` into `.cache/loki-bin/loki-<sha12>`: `checkoutLoki` runs `git fetch origin` in `test/oracle/.cache/loki-src` (network, long first build). The cluster (`ch1`–`ch4`, `keeper`) may be in use by another session: wait until idle, never tear it down.
- Temporary directories live under `/tmp` or the session scratchpad, never `$HOME`. `gofmt -l` must be run on the files you touch (the oracle cache under `test/oracle/.cache` is not ours).
- `CHANGELOG.md` `## [Unreleased]`: one line per user-visible change, in the commit that makes it.

## Review Focus

1. **Histogram queries.** `histogram_quantile(0.99, sum by (path)(histogram_over_time({app="api"} | json | unwrap duration_seconds(duration) [5m])))` on `/loki/api/v1/query_range`. Expected: HTTP 400 with the message above, never a panic or 500. Task 2 Step 3 pins it at engine and API level.
2. **Label values for internal names.** `/loki/api/v1/label/__time_shard__/values` still returns the values while `/labels` omits the name. Task 5 pins both.
3. **Push JSON edge cases** (Loki replaced its push JSON decoder: jsoniter instead of jsonparser). Expected: same status as before for an unknown stream key (400), `null` inside `values` (skipped, 204), a numeric timestamp (400), a numeric structured-metadata value (400), `"stream": null` (400). Task 1 pins them; Task 2 keeps them green.
4. **Discard accounting for invalid labels.** Today a stream over `max_label_names_per_series` with 3 lines counts 3 samples under that reason **and** 3 under `invalid_labels`; a `{}` stream counts `missing_labels` and `invalid_labels`. Expected after Task 3: only the specific reason (`invalid_labels` stays for labels that do not parse). Task 1 pins today's behaviour, Task 3 flips it.
5. **A Loki config that still has `max_chunks_per_query`.** Expected: startup fails with an unknown-key error naming the key (strict YAML, "as in Loki"). Task 2 Step 2 pins it, written before the bump and seen to fail.

---

### Task 1: Pin today's behaviour (at the old pin)

**Files:**
- Test: `internal/ingest/handler_test.go`, `internal/config/config_test.go`, `test/oracle/corpus/basic.logql`

**Interfaces:**
- Consumes: the helpers in `handler_test.go`: `defaultLimits(t) *validation.Overrides`, `push(t, h, tenant, body) *httptest.ResponseRecorder`, `itoa(int64) string`, `chtest.New(t)`, the `ingest.Handler` literal of `TestPush_RecordsLokiPushMetrics`; `config.Load(path string, expandEnv bool) (*config.Config, error)`; Loki's `validation.DiscardedSamples`/`validation.DiscardedBytes` collectors.
- Produces: tests that Tasks 2–3 must keep green, except the three assertions marked "flips in Task 3".

- [ ] **Step 1: Write `TestPush_JSONEdgeCases`** (table, tenant `t-json`, one valid stream `{"app":"web"}` unless noted; assert status only). Cases: (a) extra stream key `"foo":1` → 400; (b) a `null` between two valid `values` entries → 204; (c) timestamp as a JSON number → 400; (d) structured metadata `{"k":1}` → 400; (e) `"stream": null` → record today's answer and fix it in the test; (f) label value `null` (`{"stream":{"app":null},…}`) → record today's answer and fix it.

- [ ] **Step 2: Write `TestPush_LabelValidationDiscardAccounting`.** Set `max_label_names_per_series` to 2 on the `validation.Limits` before `NewOverrides`. Helper `discarded(t, tenant, reason) (samples, bytes float64)` sums every label-pair combination of the family from `prometheus.DefaultGatherer.Gather()` for that tenant and reason (do not use `WithLabelValues`: `retention_hours` differs between the two report sites). Cases, each with its own tenant: (a) a 3-label stream with 3 lines → status 400, `max_label_names_per_series` = 3 samples and the sum of the line lengths in bytes, `invalid_labels` = 3 samples **(flips in Task 3: 0)**; (b) stream `{}` with 2 lines → 400, `missing_labels` = 1 sample **(flips in Task 3: 2)**, `invalid_labels` = 2 **(flips in Task 3: 0)**.

- [ ] **Step 3: Write `TestLoad_RemovedLokiLimitIsRejected`** in `internal/config/config_test.go`: a config file with `limits_config: {max_chunks_per_query: 100}`; expected after the bump: `config.Load` returns an error containing `max_chunks_per_query`. **At the old pin this test fails** (the key is accepted): that is its red state; do not commit it in this task, keep it in the working tree for Task 2 Step 2.

- [ ] **Step 4: Add oracle corpus cases** to `test/oracle/corpus/basic.logql` in the style of its neighbours (`range:` prefix lines): `stddev_over_time` and `stdvar_over_time` over `| logfmt | unwrap duration`, a nanosecond offset on a `count_over_time` (`offset 1ns`), and a `| ip()` label filter on a field that is not an IP. The existing `pushdown.logql` already has `|= "" or "POST"`.

- [ ] **Step 5: Run them at the old pin**

Run: `RUNAKEFLI_TEST_CH_ADDR=127.0.0.1:9000 go test ./internal/ingest -run 'TestPush_(JSONEdgeCases|LabelValidationDiscardAccounting)' -count=1 -race` and `make oracle`.
Expected: PASS (the oracle may need the first Loki build). A failure means an assumption about today's behaviour is wrong: fix the test, not the code. A corpus case Loki itself rejects: drop it.

- [ ] **Step 6: Commit** (without the config test)

```bash
git add internal/ingest/handler_test.go test/oracle/corpus/basic.logql
git commit -m "test: pin push JSON edge cases, discard accounting and more oracle cases"
```

---

### Task 2: Bump Loki to acaa752c

**Files:**
- Modify: `Makefile:1`, `test/oracle/loki.go`, `go.mod`, `go.sum`, `scripts/govulncheck-ignore.txt`, `internal/deletes/client.go` (comment), `CHANGELOG.md`
- Modify (imports/symbols): every `.go` file that imports `pkg/logqlmodel` (`grep -rl 'pkg/logqlmodel' --include='*.go' . | grep -v .cache`, 21 files at the time of writing, including the tagged `test/` files), among them `internal/api/limits.go`, `internal/api/query.go`, `internal/pushdown/{split.go,tiera.go,split_test.go}`
- Modify (header SHA only, after verification): `internal/api/format_query.go`, `internal/api/limits.go`, `internal/chstore/{detected.go,stats.go}`, `internal/deletes/handler.go`, `internal/ingest/ratelimit.go`, `internal/multitenant/{index.go,querier.go}`, `internal/patterns/patterns.go`, `internal/pushdown/{legs.go,split.go}`, `internal/ruler/evaluator.go`. **Not moved here:** `internal/ingest/distcopy/*` (Tasks 3–4) and `internal/ingest/parse.go` (its header gains "no max_inflight_bytes check" in the modifications list first; then it moves).
- Test: `internal/pushdown/engine_test.go`, `internal/api/query_test.go`, `internal/config/config_test.go`

**Interfaces:**
- Consumes (Loki at the new pin): `pkg/logql/model` (package `model`, imported as `logqlmodel`: `Result`, `Streams`, `ValueTypeStreams`), `pkg/logql/model/stats`, `pkg/logql/model/metadata`, `pkg/logql/errors` (imported as `logqlerrors`: `ErrParse`, `ErrMaxQueryLength`, `ErrMaxEntriesLimit`, `ErrUnsupportedSyntaxForInstantQuery`, `NewSeriesLimitError(limit int) *LimitError`, `LimitError`).
- Produces: a building tree at the new pin with Task 1's tests green and the config test green.

- [ ] **Step 1: Confirm the red config test** from Task 1 Step 3 fails at the old pin (`go test ./internal/config -run TestLoad_RemovedLokiLimitIsRejected`).

- [ ] **Step 2: Move the pin, then migrate, then tidy** (in this order)
  - `Makefile` `LOKI_COMMIT` and `test/oracle/loki.go` `lokiCommit` → the full new SHA.
  - `go.mod`: `github.com/grafana/loki/v3` → `v3.0.0-20261120230041-acaa752c17c1`; the `pkg/push` replace → `v0.0.0-20261120230041-acaa752c17c1`; objstore, franz-go and kmsg replaces → the Spec values; then diff every other `replace` against `git show <sha>:go.mod` of Loki.
  - Migrate imports and symbols by the rule in the Spec; drop `logqlmodel` imports that become unused.
  - `go mod tidy`; then `grep -n 'grafana/loki/v3 ' go.mod` must still show the pseudo-version.

Run: `go build ./... && go vet ./... && go vet -tags oracle,bench ./test/...`
Expected: no output.

- [ ] **Step 3: Write the histogram tests, see them pass**
  - `TestEngine_HistogramQueryIsParseError` in `internal/pushdown/engine_test.go` (these tests use `chtest.New`; there is no ClickHouse-free engine setup): run the Review Focus 1 query through the engine the neighbouring tests build; assert `errors.Is(err, logqlerrors.ErrParse)` and that the message contains `histogram functions are experimental and not enabled`.
  - `TestAPI_HistogramQueryIs400` in `internal/api/query_test.go`, in the style of `TestAPI_InstantQueryInvalidLogQuery`: the same query on `/loki/api/v1/query_range` answers 400.

Run: `RUNAKEFLI_TEST_CH_ADDR=127.0.0.1:9000 go test ./internal/pushdown ./internal/api ./internal/config -count=1 -race`
Expected: PASS (including the config test and Task 1's tests). A 500 or a type-switch error means the mapper or engine needs a guard: stop and decide from the error, keeping Loki's message.

- [ ] **Step 4: Re-point the identical headers**
  - For each file of the "header SHA only" list run `git -C <loki clone> diff -U0 39c256e5 <sha> -- <upstream path of the header>` and confirm each function named in the header changed at most by imports and the error-package rename. Spec lists the ones already compared; compare any other before moving it. Add "no max_inflight_bytes check" to the modifications of `parse.go`'s header.
  - `scripts/govulncheck-ignore.txt`, `internal/deletes/client.go`: update the SHA/pseudo-version in the comment; run `scripts/govulncheck.sh` and keep `GO-2026-5115` only if still reported.

Run: `grep -rn "39c256e5" --exclude-dir=.git --exclude-dir=site --exclude-dir=.cache . | grep -v "docs/superpowers\|CHANGELOG.md\|debian/changelog\|regexsimplify_test.go\|compare.go\|distcopy"`
Expected: no output.

- [ ] **Step 5: Full gates**

Run, in order: `make check`, `make test-cluster`, `make oracle-all`, `make oracle-cluster`.
Expected: all pass, coverage ≥ 85 %. An oracle mismatch is an upstream behaviour change Rúnakefli's SQL does not follow: record the query, fix the pushdown or add the regression case to `test/oracle/corpus/`, never loosen `Compare`.

- [ ] **Step 6: CHANGELOG and commit**

Under `## [Unreleased]` → `### Changed`, one line: pin bumped from `39c256e5` to `acaa752c` (2026-11-20), listing what users see: Kahan summation in `stddev_over_time`/`stdvar_over_time`; nanosecond offsets; errors in the `ip()` label filter drop the line; `$.field` in `line_format`/`label_format`; logfmt values kept after a renamed key; leading empty operands of a line-filter chain ignored; `/format_query` keeps empty groupings and rejects stray characters with 400 (#26252); the query `stats` no longer carry `cache.logResult` and `cache.taskResult`; histogram functions answer 400 like in Loki (not evaluated yet); **`limits_config.max_chunks_per_query` (`-store.query-chunk-limit`) is gone upstream, so a configuration that still has it is rejected at startup**.

```bash
git add Makefile test/oracle go.mod go.sum internal/ scripts/govulncheck-ignore.txt CHANGELOG.md
git commit -m "build: bump the pinned Loki commit to acaa752c"
```
The body lists the gates that passed and the copies verified identical.

---

### Task 3: Count label-validation discards once

**Files:**
- Modify: `internal/ingest/distcopy/validator.go`, `internal/ingest/distcopy/validator_test.go`, `internal/ingest/distcopy/process.go`
- Modify: `internal/ingest/handler_test.go` (flip the three assertions), `CHANGELOG.md`

**Interfaces:**
- Consumes: upstream `pkg/distributor/validator.go` and `distributor.go` at the new commit (`ValidateLabels`, `parseStreamLabels`, `parseAndValidateStreamLabels`, `reportDiscardedData`, `constants.IsInternalStream`).
- Produces: `func (v Validator) ValidateLabels(vCtx validationContext, ls labels.Labels, stream logproto.InternalStreamAdapter) (reason string, err error)` (reports nothing); one reporting function `reportDiscardedData(ctx, reason, vCtx, lbs, retentionHours, policy, entrySize, entryCount, format)` (`reportDiscardedDataWithTracker` is removed); `IsInternalStream` replaced by `constants.IsInternalStream(ls.Has)`.

- [ ] **Step 1: Flip Task 1's three assertions** in `TestPush_LabelValidationDiscardAccounting` to the values marked "flips in Task 3". Run it: it fails (red).

- [ ] **Step 2: Re-sync `validator.go` and `process.go`** from upstream. `process.go` reports an invalid-labels stream once, under `ValidateLabels`' reason, and keeps `invalid_labels` for labels that do not parse; keep the existing modifications listed in the headers (no sharding, no limits service). Correct the header of `validator.go` ("reportDiscardedData* made no-ops": they report). Copy upstream's `validator_test.go` changes.

- [ ] **Step 3: Run**

Run: `RUNAKEFLI_TEST_CH_ADDR=127.0.0.1:9000 go test ./internal/ingest/... -count=1 -race`
Expected: PASS, including the flipped test (green).

- [ ] **Step 4: Gates and commit.** `make check`; `make oracle` (push-side accounting is not compared by the oracle: it must stay unchanged). `### Fixed` line: a push rejected for invalid labels counts its samples and bytes once, under the specific reason (`max_label_names_per_series`, `label_name_too_long`, `label_value_too_long`, `duplicate_label_names`, `missing_labels`), no longer also under `invalid_labels` (Loki #26317). Move the headers of `validator.go`, `validator_test.go` and `process.go` to the new SHA.

```bash
git commit -am "fix: count label-validation discards once, as Loki does"
```

---

### Task 4: Re-sync level detection

**Files:**
- Modify: `internal/ingest/distcopy/field_detection.go`, `internal/ingest/distcopy/field_detection_test.go`

**Interfaces:**
- Consumes: upstream `pkg/distributor/field_detection.go` and `_test.go` at the new commit.
- Produces: no signature changes (`detectLevelFromLogLine(s string) string` keeps its name and result values).

- [ ] **Step 1: Copy upstream's updated test file** over the copy (keep the header's modifications). Run `go test ./internal/ingest/distcopy -count=1`: it passes at the old implementation if the rewrite really keeps the results; any failure is a behaviour difference: stop and record it in `docs/divergences.md` Task 6 style before continuing.
- [ ] **Step 2: Copy upstream's implementation** (lookup tables, `hasFullWordPrefix`), keep the modifications; header → new SHA.
- [ ] **Step 3: Run and commit.** `go test ./internal/ingest/... -race -count=1 && make check`; no CHANGELOG line (performance only, same results).

```bash
git commit -am "refactor: re-sync level detection with Loki's allocation-free version"
```

---

### Task 5: Hide Loki's internal labels from the label-name APIs

**Files:**
- Modify: `internal/chstore/metadata.go` (`Label`), `internal/chstore/detected.go` (`DetectedLabels`), `CHANGELOG.md`
- Test: `internal/chstore/metadata_test.go`, `internal/chstore/querier_test.go` (the ClickHouse-backed `DetectedLabels` tests live there)

**Interfaces:**
- Consumes: `constants.IsInternalLabel(name string) bool` (true for `__aggregated_metric__`, `__pattern__`, `__time_shard__`, `__stream_shard__`) from `github.com/grafana/loki/v3/pkg/util/constants`; the fixture helpers of the neighbouring tests.
- Produces: nothing for other tasks. `Series` is deliberately **not** changed (see Spec).

- [ ] **Step 1: Write the failing tests.** Fixture: tenant with streams `{app="a"}` and `{app="b", __time_shard__="1", __stream_shard__="0", __pattern__="x", __aggregated_metric__="y"}`.
  - `TestLabel_HidesInternalLabelNames`: `Label(Values:false)` returns exactly `["app"]`.
  - `TestLabel_ServesValuesOfInternalLabels`: `Label(Values:true, Name:"__time_shard__")` returns `["1"]`.
  - `TestDetectedLabels_HidesInternalLabels`: no entry whose label is one of the four names; `app` is present.

- [ ] **Step 2: Run them, see the first and third fail**

Run: `RUNAKEFLI_TEST_CH_ADDR=127.0.0.1:9000 go test ./internal/chstore -run 'TestLabel_|TestDetectedLabels_HidesInternal' -count=1`
Expected: FAIL for the names tests; the values test passes and must keep passing.

- [ ] **Step 3: Implement:** in `Label`, skip names with `constants.IsInternalLabel` when `!req.Values`; in `DetectedLabels`, skip the same names while building `values`.

- [ ] **Step 4: Run, gate, commit.** `go test ./internal/chstore ./internal/api ./internal/multitenant -race -count=1 && make check && make oracle` (the corpus has no internal labels: unchanged). `### Fixed` line: `/labels` and `/detected_labels` hide Loki's internal labels as Loki does; label values stay queryable.

```bash
git add internal/chstore CHANGELOG.md
git commit -m "fix: hide internal labels from the label name and detected label APIs"
```

---

### Task 6: Record what Rúnakefli does not implement

**Files:**
- Modify: `docs/divergences.md`, `docs/migrating.md` (only if it lists unsupported Loki settings), `docs/roadmap.md` (only for items worth implementing), `CHANGELOG.md`
- Scratch test, never committed and deleted before `make check`: `internal/config/scratch_keys_test.go`

**Interfaces:**
- Consumes: `config.Load(path string, expandEnv bool) (*config.Config, error)`; the running binary for the endpoint checks.
- Produces: divergence entries that other docs link to.

- [ ] **Step 1: Establish each claim by evidence.** Config items: the scratch test loads a YAML containing the key via `config.Load` and prints accepted / rejected; for accepted keys, grep `internal/` for a use to tell enforced from silently ignored. HTTP items: start `bin/runakefli` against the local ClickHouse and `curl` the path. Items: `limits_config.max_query_bytes_read` and `max_querier_bytes_read`; a top-level `distributor:` block with `max_inflight_bytes`; `limits_config.metric_aggregation_enabled`; `dataobj_deletion_sweep_enabled` and `tsdb_sharded_index_shard_factor` (accepted, ignored); per-policy `chunk_idle_period` and `limits_service_streams_expiry`; `GET /loki/api/v1/exposed-config`, `GET /config`, `GET /config?mode=diff`; the histogram error message that names `-querier.engine.enable-histograms`, a flag Rúnakefli does not have (Review Focus 1).

- [ ] **Step 2: Write the divergences.** One bullet per verified item in the matching section of `docs/divergences.md` (Ingestion for the distributor and policy items, Query for the limits and the histogram message, a new bullet under Observability for the config endpoints): what Loki does, what Rúnakefli does, why. An accepted-but-not-enforced key is the important case: say so. Cheap and worthwhile items go to `docs/roadmap.md` too.

- [ ] **Step 3: Verify and commit.** Delete the scratch test; `make check` (and `mkdocs build --strict` if installed). `CHANGELOG.md` `### Changed`: one line that the divergences list now covers these items.

```bash
git add docs CHANGELOG.md
git commit -m "docs: record unsupported Loki limits and endpoints as divergences"
```
