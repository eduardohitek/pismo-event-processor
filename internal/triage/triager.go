package triage

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/eduardohitek/pismo-event-processor/internal/domain"
)

type TriageError struct {
	Reason domain.QuarantineReason
	Detail string
}

func (e *TriageError) Error() string {
	return fmt.Sprintf("triage error: %s: %s", e.Reason, e.Detail)
}

type ruleMatch struct {
	Type string `yaml:"type"`
}

type ruleRoute struct {
	Category string `yaml:"category"`
	Priority int    `yaml:"priority"`
}

type rule struct {
	Match ruleMatch `yaml:"match"`
	Route ruleRoute `yaml:"route"`
}

type config struct {
	Rules             []rule     `yaml:"rules"`
	Default           *ruleRoute `yaml:"default"`
	RegisteredTenants []string   `yaml:"registered_tenants"`
}

type RuleBasedTriager struct {
	rules             []rule
	defaultRoute      *ruleRoute
	registeredTenants map[string]struct{}
}

func New(path string) (*RuleBasedTriager, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read routing config: %w", err)
	}

	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse routing config: %w", err)
	}

	tenants := make(map[string]struct{}, len(cfg.RegisteredTenants))
	for _, t := range cfg.RegisteredTenants {
		tenants[t] = struct{}{}
	}

	return &RuleBasedTriager{
		rules:             cfg.Rules,
		defaultRoute:      cfg.Default,
		registeredTenants: tenants,
	}, nil
}

func (t *RuleBasedTriager) Route(evt *domain.Event) (*domain.Routing, error) {
	if _, ok := t.registeredTenants[evt.TenantID]; !ok {
		return nil, &TriageError{
			Reason: domain.ReasonUnregisteredTenant,
			Detail: fmt.Sprintf("tenant %q is not registered", evt.TenantID),
		}
	}

	for _, r := range t.rules {
		if matchType(r.Match.Type, evt.Type) {
			return &domain.Routing{
				TargetClient: evt.TenantID,
				Category:     r.Route.Category,
				Priority:     r.Route.Priority,
			}, nil
		}
	}

	if t.defaultRoute != nil {
		return &domain.Routing{
			TargetClient: evt.TenantID,
			Category:     t.defaultRoute.Category,
			Priority:     t.defaultRoute.Priority,
		}, nil
	}

	return nil, &TriageError{
		Reason: domain.ReasonNoRoutingRule,
		Detail: fmt.Sprintf("no routing rule matches event type %q", evt.Type),
	}
}

// matchType returns true if pattern matches eventType.
// Supports exact match and glob suffix ("com.pismo.monitoring.*").
func matchType(pattern, eventType string) bool {
	if strings.HasSuffix(pattern, ".*") {
		prefix := pattern[:len(pattern)-2]
		return strings.HasPrefix(eventType, prefix+".")
	}
	return pattern == eventType
}
