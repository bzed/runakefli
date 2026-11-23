---
description: What is deferred or still open in Rúnakefli, the Loki®-compatible log server backed by ClickHouse®.
---

# Roadmap

## Open items

- **ClickHouse® Cloud verification:** run the unit suite and the oracle against
  a ClickHouse Cloud service (single-server DDL, which Cloud maps to
  SharedMergeTree) and record the result here. It needs only a Cloud account: run
  the suites with `RUNAKEFLI_TEST_CH_ADDR=<host>:9440 RUNAKEFLI_TEST_CH_TLS=1
  RUNAKEFLI_TEST_CH_USER=... RUNAKEFLI_TEST_CH_PASSWORD=...` (add
  `RUNAKEFLI_TEST_CH_TLS_CA=<pem>` if the service's CA is not in the system
  roots). The TLS port and the `clickhouse.tls` config block are wired.
- **podman compose example:** a ready-to-run Rúnakefli plus ClickHouse setup.
- **Per-policy ingestion limits** (`policy_stream_mapping`): see the
  [divergences](https://github.com/bzed/runakefli/blob/main/docs/divergences.md#ingestion).
- **SQL push-down for multi-tenant queries:** with
  `query.querier.multi_tenant_queries_enabled`, multi-tenant queries run on the
  plain engine, because push-down is single-tenant by construction.
- **`detected_metric_names`:** not served by Rúnakefli.
