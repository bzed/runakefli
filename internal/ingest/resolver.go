// Copied from grafana/loki@acaa752c17c1560eead5a33d04d63ed877d4a6de pkg/distributor/distributor.go (requestScopedStreamResolver, newRequestScopedStreamResolver); modifications: renamed to streamResolver/newStreamResolver, takes *validation.Overrides instead of the distributor's Limits interface, the warning log carries runakefli's gosec nolint directive.

package ingest

import (
	"context"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/v3/pkg/compactor/retention"
	"github.com/grafana/loki/v3/pkg/loghttp/push"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/prometheus/model/labels"
)

// streamResolver is Loki's request-scoped policy and retention resolver. It
// snapshots the tenant's rules when a push request arrives, so every parser
// and every stream of that request sees the same values.
type streamResolver struct {
	userID               string
	policyStreamMappings validation.PolicyStreamMapping
	retention            *retention.TenantRetentionSnapshot

	logger log.Logger
}

func newStreamResolver(userID string, limits *validation.Overrides, logger log.Logger) push.StreamResolver {
	return &streamResolver{
		userID:               userID,
		policyStreamMappings: limits.PoliciesStreamMapping(userID),
		retention:            retention.NewTenantRetentionSnapshot(limits, userID),
		logger:               logger,
	}
}

func (r streamResolver) RetentionPeriodFor(lbs labels.Labels) time.Duration {
	return r.retention.RetentionPeriodFor(lbs)
}

func (r streamResolver) RetentionHoursFor(lbs labels.Labels) string {
	return r.retention.RetentionHoursFor(lbs)
}

func (r streamResolver) PolicyFor(ctx context.Context, lbs labels.Labels) string {
	policies := r.policyStreamMappings.PolicyFor(ctx, lbs)

	var policy string
	if len(policies) > 0 {
		policy = policies[0]
		if len(policies) > 1 {
			level.Warn(r.logger).Log( //nolint:gosec // G104: go-kit logger errors are not actionable
				"msg", "multiple policies matched for the same stream",
				"org_id", r.userID,
				"stream", lbs.String(),
				"policy", policy,
				"policies", strings.Join(policies, ","),
				"insight", "true",
			)
		}
	}

	return policy
}
