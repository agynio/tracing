package threadsclient

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	threadsv1 "github.com/agynio/tracing/.gen/go/agynio/api/threads/v1"
)

const messagePageSize int32 = 200

type Client struct {
	conn   *grpc.ClientConn
	client threadsv1.ThreadsServiceClient
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
		client: threadsv1.NewThreadsServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) MessageBelongsToThread(ctx context.Context, identityID, identityType, threadID, messageID string) (bool, error) {
	identityID = strings.TrimSpace(identityID)
	identityType = strings.TrimSpace(identityType)
	threadID = strings.TrimSpace(threadID)
	messageID = strings.TrimSpace(messageID)
	if identityID == "" {
		return false, fmt.Errorf("identity id is required")
	}
	if identityType == "" {
		return false, fmt.Errorf("identity type is required")
	}
	if threadID == "" {
		return false, fmt.Errorf("thread id is required")
	}
	if messageID == "" {
		return false, fmt.Errorf("message id is required")
	}

	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"x-identity-id",
		identityID,
		"x-identity-type",
		identityType,
	)

	pageToken := ""
	for {
		resp, err := c.client.GetMessages(ctx, &threadsv1.GetMessagesRequest{
			ThreadId:  threadID,
			PageSize:  messagePageSize,
			PageToken: pageToken,
			Order:     threadsv1.MessageOrder_MESSAGE_ORDER_OLDEST_FIRST,
		})
		if err != nil {
			return false, err
		}

		for _, message := range resp.GetMessages() {
			if strings.TrimSpace(message.GetId()) == messageID {
				return true, nil
			}
		}

		nextToken := strings.TrimSpace(resp.GetNextPageToken())
		if nextToken == "" {
			return false, nil
		}
		pageToken = nextToken
	}
}
