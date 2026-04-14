package zitimgmtclient

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	zitimgmtv1 "github.com/agynio/tracing/.gen/go/agynio/api/ziti_management/v1"
)

type Client struct {
	conn   *grpc.ClientConn
	client zitimgmtv1.ZitiManagementServiceClient
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
		client: zitimgmtv1.NewZitiManagementServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) RequestServiceIdentity(ctx context.Context, serviceType zitimgmtv1.ServiceType) (string, []byte, error) {
	if serviceType == zitimgmtv1.ServiceType_SERVICE_TYPE_UNSPECIFIED {
		return "", nil, fmt.Errorf("service type is required")
	}

	response, err := c.client.RequestServiceIdentity(ctx, &zitimgmtv1.RequestServiceIdentityRequest{
		ServiceType: serviceType,
	})
	if err != nil {
		return "", nil, err
	}

	identityID := strings.TrimSpace(response.GetZitiIdentityId())
	if identityID == "" {
		return "", nil, fmt.Errorf("ziti identity id missing")
	}

	identityJSON := response.GetIdentityJson()
	if len(identityJSON) == 0 {
		return "", nil, fmt.Errorf("identity json missing")
	}

	return identityID, identityJSON, nil
}

func (c *Client) ExtendIdentityLease(ctx context.Context, zitiIdentityID string) error {
	trimmed := strings.TrimSpace(zitiIdentityID)
	if trimmed == "" {
		return fmt.Errorf("ziti identity id is required")
	}

	_, err := c.client.ExtendIdentityLease(ctx, &zitimgmtv1.ExtendIdentityLeaseRequest{
		ZitiIdentityId: trimmed,
	})
	return err
}
