package agentsclient

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	agentsv1 "github.com/agynio/tracing/.gen/go/agynio/api/agents/v1"
	"github.com/agynio/tracing/internal/identity"
)

type Client struct {
	conn   *grpc.ClientConn
	client agentsv1.AgentsServiceClient
}

func NewClient(target string) (*Client, error) {
	if strings.TrimSpace(target) == "" {
		return nil, fmt.Errorf("target is required")
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:   conn,
		client: agentsv1.NewAgentsServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) ResolveAgentIdentity(ctx context.Context, identityID string) (identity.AgentIdentity, error) {
	trimmed := strings.TrimSpace(identityID)
	if trimmed == "" {
		return identity.AgentIdentity{}, fmt.Errorf("identity id is required")
	}

	response, err := c.client.ResolveAgentIdentity(ctx, &agentsv1.ResolveAgentIdentityRequest{IdentityId: trimmed})
	if err != nil {
		return identity.AgentIdentity{}, err
	}

	agentID := strings.TrimSpace(response.GetAgentId())
	if agentID == "" {
		return identity.AgentIdentity{}, fmt.Errorf("agent id missing")
	}

	orgID := strings.TrimSpace(response.GetOrganizationId())
	if orgID == "" {
		return identity.AgentIdentity{}, fmt.Errorf("organization id missing")
	}

	return identity.AgentIdentity{AgentID: agentID, OrganizationID: orgID}, nil
}
