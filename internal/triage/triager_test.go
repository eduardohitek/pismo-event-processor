package triage_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eduardohitek/pismo-event-processor/internal/domain"
	"github.com/eduardohitek/pismo-event-processor/internal/triage"
)

const testConfig = `
rules:
  - match:
      type: "com.pismo.payment.authorized.v1"
    route:
      category: "transactional"
      priority: 1

  - match:
      type: "com.pismo.monitoring.*"
    route:
      category: "observability"
      priority: 3

default:
  category: "uncategorized"
  priority: 2

registered_tenants:
  - tenant-A
  - tenant-B
`

const testConfigNoDefault = `
rules:
  - match:
      type: "com.pismo.payment.authorized.v1"
    route:
      category: "transactional"
      priority: 1

registered_tenants:
  - tenant-A
`

func newTriager(t *testing.T, content string) *triage.RuleBasedTriager {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routing.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	tr, err := triage.New(path)
	require.NoError(t, err)
	return tr
}

func TestRoute(t *testing.T) {
	cases := []struct {
		name       string
		config     string
		tenantID   string
		eventType  string
		wantErr    domain.QuarantineReason
		wantCat    string
		wantPrio   int
		wantTarget string
	}{
		{
			name:       "exact match",
			config:     testConfig,
			tenantID:   "tenant-A",
			eventType:  "com.pismo.payment.authorized.v1",
			wantCat:    "transactional",
			wantPrio:   1,
			wantTarget: "tenant-A",
		},
		{
			name:       "glob match",
			config:     testConfig,
			tenantID:   "tenant-A",
			eventType:  "com.pismo.monitoring.heartbeat.v1",
			wantCat:    "observability",
			wantPrio:   3,
			wantTarget: "tenant-A",
		},
		{
			name:       "fallback to default",
			config:     testConfig,
			tenantID:   "tenant-B",
			eventType:  "com.pismo.unknown.event.v1",
			wantCat:    "uncategorized",
			wantPrio:   2,
			wantTarget: "tenant-B",
		},
		{
			name:      "unregistered tenant",
			config:    testConfig,
			tenantID:  "tenant-unknown",
			eventType: "com.pismo.payment.authorized.v1",
			wantErr:   domain.ReasonUnregisteredTenant,
		},
		{
			name:      "empty tenant",
			config:    testConfig,
			tenantID:  "",
			eventType: "com.pismo.payment.authorized.v1",
			wantErr:   domain.ReasonUnregisteredTenant,
		},
		{
			name:      "no matching rule and no default",
			config:    testConfigNoDefault,
			tenantID:  "tenant-A",
			eventType: "com.pismo.unknown.event.v1",
			wantErr:   domain.ReasonNoRoutingRule,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTriager(t, tc.config)
			evt := &domain.Event{TenantID: tc.tenantID, Type: tc.eventType}

			routing, err := tr.Route(evt)

			if tc.wantErr != "" {
				require.Error(t, err)
				var te *triage.TriageError
				require.True(t, errors.As(err, &te))
				assert.Equal(t, tc.wantErr, te.Reason)
				assert.Nil(t, routing)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, routing)
			assert.Equal(t, tc.wantTarget, routing.TargetClient)
			assert.Equal(t, tc.wantCat, routing.Category)
			assert.Equal(t, tc.wantPrio, routing.Priority)
		})
	}
}
