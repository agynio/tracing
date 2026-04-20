package identity

import (
	"context"
	"fmt"

	identityv1 "github.com/agynio/tracing/.gen/go/agynio/api/identity/v1"
	"github.com/agynio/tracing/internal/cache"
)

const (
	identityUserPrefix = "identity:"
	threadObjectPrefix = "thread:"
	threadReadRelation = "can_read"
)

type ResolvedIdentity struct {
	IdentityID   string
	IdentityType identityv1.IdentityType
}

type AgentIdentity struct {
	AgentID        string
	OrganizationID string
}

type IdentityChain struct {
	IdentityID     string
	IdentityType   identityv1.IdentityType
	AgentID        string
	OrganizationID string
}

type ZitiResolver interface {
	ResolveIdentity(ctx context.Context, sourceIdentity string) (ResolvedIdentity, error)
}

type AgentsResolver interface {
	ResolveAgentIdentity(ctx context.Context, identityID string) (AgentIdentity, error)
}

type AuthorizationChecker interface {
	Check(ctx context.Context, user, relation, object string) (bool, error)
}

type Resolver struct {
	zitiResolver   ZitiResolver
	agentsResolver AgentsResolver
	cache          *cache.LRU[string, IdentityChain]
}

func NewResolver(zitiResolver ZitiResolver, agentsResolver AgentsResolver, cache *cache.LRU[string, IdentityChain]) (*Resolver, error) {
	if zitiResolver == nil {
		return nil, fmt.Errorf("ziti resolver is required")
	}
	if agentsResolver == nil {
		return nil, fmt.Errorf("agents resolver is required")
	}
	if cache == nil {
		return nil, fmt.Errorf("cache is required")
	}

	return &Resolver{
		zitiResolver:   zitiResolver,
		agentsResolver: agentsResolver,
		cache:          cache,
	}, nil
}

func (r *Resolver) Resolve(ctx context.Context, sourceIdentity string) (IdentityChain, error) {
	if chain, ok := r.cache.Get(sourceIdentity); ok {
		return chain, nil
	}

	resolved, err := r.zitiResolver.ResolveIdentity(ctx, sourceIdentity)
	if err != nil {
		return IdentityChain{}, fmt.Errorf("resolve ziti identity: %w", err)
	}
	if resolved.IdentityType != identityv1.IdentityType_IDENTITY_TYPE_AGENT {
		return IdentityChain{}, fmt.Errorf("identity type is not agent: %s", resolved.IdentityType.String())
	}

	agentIdentity, err := r.agentsResolver.ResolveAgentIdentity(ctx, resolved.IdentityID)
	if err != nil {
		return IdentityChain{}, fmt.Errorf("resolve agent identity: %w", err)
	}

	chain := IdentityChain{
		IdentityID:     resolved.IdentityID,
		IdentityType:   resolved.IdentityType,
		AgentID:        agentIdentity.AgentID,
		OrganizationID: agentIdentity.OrganizationID,
	}
	r.cache.Add(sourceIdentity, chain)
	return chain, nil
}

type ThreadAuthorizer struct {
	authz AuthorizationChecker
	cache *cache.LRU[string, bool]
}

func NewThreadAuthorizer(authz AuthorizationChecker, cache *cache.LRU[string, bool]) (*ThreadAuthorizer, error) {
	if authz == nil {
		return nil, fmt.Errorf("authorization checker is required")
	}
	if cache == nil {
		return nil, fmt.Errorf("cache is required")
	}
	return &ThreadAuthorizer{authz: authz, cache: cache}, nil
}

func (t *ThreadAuthorizer) CanRead(ctx context.Context, identityID, threadID string) (bool, error) {
	cacheKey := identityID + ":" + threadID
	if allowed, ok := t.cache.Get(cacheKey); ok {
		return allowed, nil
	}

	allowed, err := t.authz.Check(ctx, identityUserPrefix+identityID, threadReadRelation, threadObjectPrefix+threadID)
	if err != nil {
		return false, err
	}

	t.cache.Add(cacheKey, allowed)
	return allowed, nil
}
