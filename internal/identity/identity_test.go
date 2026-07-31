package identity

import (
	"context"
	"strings"
	"testing"
	"time"

	identityv1 "github.com/agynio/tracing/.gen/go/agynio/api/identity/v1"
	"github.com/agynio/tracing/internal/cache"
)

type stubZitiResolver struct {
	resolved ResolvedIdentity
}

func (s stubZitiResolver) ResolveIdentity(context.Context, string) (ResolvedIdentity, error) {
	return s.resolved, nil
}

type stubAgentsResolver struct {
	identityID string
}

func (s *stubAgentsResolver) ResolveAgentIdentity(_ context.Context, identityID string) (AgentIdentity, error) {
	s.identityID = identityID
	return AgentIdentity{AgentID: "agent-1", OrganizationID: "org-1"}, nil
}

func newTestResolver(t *testing.T, identityType identityv1.IdentityType) (*Resolver, *stubAgentsResolver) {
	t.Helper()
	lru, err := cache.NewLRU[string, IdentityChain](8, time.Minute)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	agents := &stubAgentsResolver{}
	resolver, err := NewResolver(
		stubZitiResolver{resolved: ResolvedIdentity{IdentityID: "identity-1", IdentityType: identityType}},
		agents,
		lru,
	)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	return resolver, agents
}

// Agent workloads authenticate as their instance, so an instance identity has
// to resolve; identities minted before instances existed still present the
// class type and must keep working.
func TestResolveAcceptsAgentAndInstanceIdentities(t *testing.T) {
	for _, identityType := range []identityv1.IdentityType{
		identityv1.IdentityType_IDENTITY_TYPE_AGENT,
		identityv1.IdentityType_IDENTITY_TYPE_AGENT_INSTANCE,
	} {
		t.Run(identityType.String(), func(t *testing.T) {
			resolver, agents := newTestResolver(t, identityType)
			chain, err := resolver.Resolve(context.Background(), "source-1")
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if chain.AgentID != "agent-1" || chain.OrganizationID != "org-1" {
				t.Fatalf("unexpected chain: %+v", chain)
			}
			if chain.IdentityType != identityType {
				t.Fatalf("expected identity type %s, got %s", identityType, chain.IdentityType)
			}
			if agents.identityID != "identity-1" {
				t.Fatalf("expected the resolved identity id, got %q", agents.identityID)
			}
		})
	}
}

func TestResolveRejectsNonAgentIdentities(t *testing.T) {
	for _, identityType := range []identityv1.IdentityType{
		identityv1.IdentityType_IDENTITY_TYPE_RUNNER,
		identityv1.IdentityType_IDENTITY_TYPE_USER,
		identityv1.IdentityType_IDENTITY_TYPE_APP,
		identityv1.IdentityType_IDENTITY_TYPE_SANDBOX,
		identityv1.IdentityType_IDENTITY_TYPE_UNSPECIFIED,
	} {
		t.Run(identityType.String(), func(t *testing.T) {
			resolver, _ := newTestResolver(t, identityType)
			_, err := resolver.Resolve(context.Background(), "source-1")
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), "not an agent") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
