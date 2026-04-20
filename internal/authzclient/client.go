package authzclient

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	authorizationv1 "github.com/agynio/tracing/.gen/go/agynio/api/authorization/v1"
)

type Client struct {
	conn   *grpc.ClientConn
	client authorizationv1.AuthorizationServiceClient
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
		client: authorizationv1.NewAuthorizationServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Check(ctx context.Context, user, relation, object string) (bool, error) {
	user = strings.TrimSpace(user)
	relation = strings.TrimSpace(relation)
	object = strings.TrimSpace(object)
	if user == "" || relation == "" || object == "" {
		return false, fmt.Errorf("user, relation, and object are required")
	}

	response, err := c.client.Check(ctx, &authorizationv1.CheckRequest{
		TupleKey: &authorizationv1.TupleKey{
			User:     user,
			Relation: relation,
			Object:   object,
		},
	})
	if err != nil {
		return false, err
	}

	return response.GetAllowed(), nil
}
