package notifier

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

	notificationsv1 "github.com/agynio/tracing/.gen/go/agynio/api/notifications/v1"
)

const (
	spanCreatedEvent = "span.created"
	spanUpdatedEvent = "span.updated"
	spanSource       = "tracing"
)

type Notifier struct {
	client notificationsv1.NotificationsServiceClient
}

func New(client notificationsv1.NotificationsServiceClient) *Notifier {
	return &Notifier{client: client}
}

func (n *Notifier) PublishSpanEvent(ctx context.Context, traceID, spanID []byte, organizationID string, isNew bool) error {
	event := spanUpdatedEvent
	if isNew {
		event = spanCreatedEvent
	}
	trimmedOrgID := strings.TrimSpace(organizationID)
	if trimmedOrgID == "" {
		return fmt.Errorf("organization id required")
	}
	parsedOrgID, err := uuid.Parse(trimmedOrgID)
	if err != nil {
		return fmt.Errorf("invalid organization id: %w", err)
	}
	traceHex := hex.EncodeToString(traceID)
	spanHex := hex.EncodeToString(spanID)
	payload, err := structpb.NewStruct(map[string]any{
		"trace_id":        traceHex,
		"span_id":         spanHex,
		"organization_id": parsedOrgID.String(),
	})
	if err != nil {
		return fmt.Errorf("build payload: %w", err)
	}
	_, err = n.client.Publish(ctx, &notificationsv1.PublishRequest{
		Event:   event,
		Rooms:   []string{fmt.Sprintf("trace:%s", traceHex)},
		Payload: payload,
		Source:  spanSource,
	})
	if err != nil {
		return fmt.Errorf("publish notification: %w", err)
	}
	return nil
}
