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
			"start": {fmt.Sprint(now.Add(-time.Hour).UnixNano())},
			"end":   {fmt.Sprint(now.Add(time.Minute).UnixNano())}}
		// NewRequestWithContext: AGENTS.md requires it, and golangci-lint's noctx flags http.NewRequest.
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+r.addr+path+"?"+v.Encode(), nil)
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

	code, body = get("/loki/api/v1/detected_labels", "t1|t2")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"label":"app"`)

	// No patterns are mined from two lines; the point is that the request is
	// accepted (200, `{"status":"success","data":[]}`) instead of rejected.
	code, body = get("/loki/api/v1/patterns", "t1|t2")
	require.Equal(t, http.StatusOK, code, body)

	// A single tenant stays scoped to its own data.
	code, body = get("/loki/api/v1/index/stats", "t1")
	require.Equal(t, http.StatusOK, code, body)
	require.Contains(t, body, `"entries":1`)
}
