package ingest

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"

	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agynio/tracing/internal/identity"
	"github.com/agynio/tracing/internal/notifier"
	"github.com/agynio/tracing/internal/server"
	"github.com/agynio/tracing/internal/store"
	"github.com/agynio/tracing/internal/ziticonn"
)

const (
	attrIdentityID     = "agyn.identity.id"
	attrAgentID        = "agyn.agent.id"
	attrOrganizationID = "agyn.organization.id"
	attrThreadID       = "agyn.thread.id"
	attrMessageID      = "agyn.thread.message.id"
)

type messageVerifier interface {
	MessageBelongsToThread(ctx context.Context, threadID, messageID string) (bool, error)
}

type Handler struct {
	collectortracev1.UnimplementedTraceServiceServer
	store            *store.Store
	notifier         *notifier.Notifier
	identityResolver *identity.Resolver
	threadAuthorizer *identity.ThreadAuthorizer
	messageVerifier  messageVerifier
}

func NewHandler(
	store *store.Store,
	notifier *notifier.Notifier,
	identityResolver *identity.Resolver,
	threadAuthorizer *identity.ThreadAuthorizer,
	messageVerifier messageVerifier,
) *Handler {
	return &Handler{
		store:            store,
		notifier:         notifier,
		identityResolver: identityResolver,
		threadAuthorizer: threadAuthorizer,
		messageVerifier:  messageVerifier,
	}
}

func (h *Handler) Export(ctx context.Context, req *collectortracev1.ExportTraceServiceRequest) (*collectortracev1.ExportTraceServiceResponse, error) {
	identityChain, hasIdentity, err := h.resolveIdentityChain(ctx)
	if err != nil {
		return nil, err
	}

	threadIDs := map[string]struct{}{}
	messageIDsByThread := map[string]map[string]struct{}{}
	for _, resourceSpans := range req.GetResourceSpans() {
		if resourceSpans == nil {
			continue
		}
		resource := ensureResource(resourceSpans)
		if hasIdentity {
			injectIdentity(resource, identityChain)
		}

		threadID, err := resourceStringAttribute(resource, attrThreadID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid %s: %v", attrThreadID, err)
		}
		messageID, err := resourceStringAttribute(resource, attrMessageID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid %s: %v", attrMessageID, err)
		}
		if messageID != "" && threadID == "" {
			return nil, status.Error(codes.InvalidArgument, "thread id is required for message verification")
		}
		if threadID != "" {
			threadIDs[threadID] = struct{}{}
		}
		if messageID != "" {
			messageIDs := messageIDsByThread[threadID]
			if messageIDs == nil {
				messageIDs = map[string]struct{}{}
				messageIDsByThread[threadID] = messageIDs
			}
			messageIDs[messageID] = struct{}{}
		}
	}

	if hasIdentity && len(threadIDs) > 0 {
		if h.threadAuthorizer == nil {
			return nil, status.Error(codes.Internal, "thread authorization unavailable")
		}
		for threadID := range threadIDs {
			allowed, err := h.threadAuthorizer.CanRead(ctx, identityChain.IdentityID, threadID)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "authorize thread: %v", err)
			}
			if !allowed {
				return nil, status.Error(codes.PermissionDenied, "thread access denied")
			}
		}
	}

	if len(messageIDsByThread) > 0 {
		if h.messageVerifier == nil {
			return nil, status.Error(codes.Internal, "message verification unavailable")
		}
		for threadID, messageIDs := range messageIDsByThread {
			for messageID := range messageIDs {
				ok, err := h.messageVerifier.MessageBelongsToThread(ctx, threadID, messageID)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "verify message: %v", err)
				}
				if !ok {
					return nil, status.Errorf(codes.InvalidArgument, "message id %s does not belong to thread %s", messageID, threadID)
				}
			}
		}
	}

	var rejectedSpans int64
	for _, resourceSpans := range req.GetResourceSpans() {
		if resourceSpans == nil {
			continue
		}
		resourceData, err := server.MarshalResource(resourceSpans.GetResource())
		if err != nil {
			count := countResourceSpans(resourceSpans)
			rejectedSpans += int64(count)
			log.Printf("ingest: marshal resource: %v", err)
			continue
		}
		for _, scopeSpans := range resourceSpans.GetScopeSpans() {
			scopeData, err := server.MarshalInstrumentationScope(scopeSpans.GetScope())
			if err != nil {
				count := len(scopeSpans.GetSpans())
				rejectedSpans += int64(count)
				log.Printf("ingest: marshal scope: %v", err)
				continue
			}
			for _, span := range scopeSpans.GetSpans() {
				row, err := spanToRow(span, resourceData, scopeData)
				if err != nil {
					rejectedSpans++
					log.Printf("ingest: span rejected: %v", err)
					continue
				}
				isNew, err := h.store.UpsertSpan(ctx, row)
				if err != nil {
					rejectedSpans++
					log.Printf("ingest: upsert span: %v", err)
					continue
				}
				if err := h.notifier.PublishSpanEvent(ctx, row.TraceID, row.SpanID, isNew); err != nil {
					log.Printf("ingest: publish span event: %v", err)
				}
			}
		}
	}

	resp := &collectortracev1.ExportTraceServiceResponse{}
	if rejectedSpans > 0 {
		resp.PartialSuccess = &collectortracev1.ExportTracePartialSuccess{
			RejectedSpans: rejectedSpans,
			ErrorMessage:  fmt.Sprintf("rejected %d spans", rejectedSpans),
		}
	}
	return resp, nil
}

func (h *Handler) resolveIdentityChain(ctx context.Context) (identity.IdentityChain, bool, error) {
	if h.identityResolver == nil {
		return identity.IdentityChain{}, false, nil
	}

	sourceIdentity, ok := ziticonn.SourceIdentityFromContext(ctx)
	if !ok {
		return identity.IdentityChain{}, false, status.Error(codes.Unauthenticated, "source identity missing")
	}

	chain, err := h.identityResolver.Resolve(ctx, sourceIdentity)
	if err != nil {
		return identity.IdentityChain{}, false, status.Errorf(codes.Unauthenticated, "resolve identity: %v", err)
	}

	return chain, true, nil
}

func countResourceSpans(resourceSpans *tracev1.ResourceSpans) int {
	if resourceSpans == nil {
		return 0
	}
	total := 0
	for _, scopeSpans := range resourceSpans.GetScopeSpans() {
		total += len(scopeSpans.GetSpans())
	}
	return total
}

func spanToRow(span *tracev1.Span, resourceData, scopeData []byte) (store.SpanRow, error) {
	if span == nil {
		return store.SpanRow{}, fmt.Errorf("span is nil")
	}
	startTime, err := toInt64(span.GetStartTimeUnixNano(), "start_time_unix_nano")
	if err != nil {
		return store.SpanRow{}, err
	}
	endTime, err := toInt64(span.GetEndTimeUnixNano(), "end_time_unix_nano")
	if err != nil {
		return store.SpanRow{}, err
	}
	flags, err := toInt32(span.GetFlags(), "flags")
	if err != nil {
		return store.SpanRow{}, err
	}
	kind, err := toInt16(int32(span.GetKind()), "kind")
	if err != nil {
		return store.SpanRow{}, err
	}
	attributes, err := server.MarshalKeyValues(span.GetAttributes())
	if err != nil {
		return store.SpanRow{}, fmt.Errorf("marshal attributes: %w", err)
	}
	droppedAttributes, err := toInt32(span.GetDroppedAttributesCount(), "dropped_attributes_count")
	if err != nil {
		return store.SpanRow{}, err
	}
	events, err := server.MarshalEvents(span.GetEvents())
	if err != nil {
		return store.SpanRow{}, fmt.Errorf("marshal events: %w", err)
	}
	droppedEvents, err := toInt32(span.GetDroppedEventsCount(), "dropped_events_count")
	if err != nil {
		return store.SpanRow{}, err
	}
	links, err := server.MarshalLinks(span.GetLinks())
	if err != nil {
		return store.SpanRow{}, fmt.Errorf("marshal links: %w", err)
	}
	droppedLinks, err := toInt32(span.GetDroppedLinksCount(), "dropped_links_count")
	if err != nil {
		return store.SpanRow{}, err
	}
	statusCode := int16(0)
	statusMessage := ""
	if status := span.GetStatus(); status != nil {
		statusCode, err = toInt16(int32(status.GetCode()), "status_code")
		if err != nil {
			return store.SpanRow{}, err
		}
		statusMessage = status.GetMessage()
	}
	parentSpanID := span.GetParentSpanId()
	if parentSpanID == nil {
		parentSpanID = []byte{}
	}

	return store.SpanRow{
		TraceID:                span.GetTraceId(),
		SpanID:                 span.GetSpanId(),
		TraceState:             span.GetTraceState(),
		ParentSpanID:           parentSpanID,
		Flags:                  flags,
		Name:                   span.GetName(),
		Kind:                   kind,
		StartTimeUnixNano:      startTime,
		EndTimeUnixNano:        endTime,
		Attributes:             attributes,
		DroppedAttributesCount: droppedAttributes,
		Events:                 events,
		DroppedEventsCount:     droppedEvents,
		Links:                  links,
		DroppedLinksCount:      droppedLinks,
		StatusCode:             statusCode,
		StatusMessage:          statusMessage,
		Resource:               resourceData,
		InstrumentationScope:   scopeData,
	}, nil
}

func toInt64(value uint64, field string) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("%s overflows int64", field)
	}
	return int64(value), nil
}

func toInt32(value uint32, field string) (int32, error) {
	if value > math.MaxInt32 {
		return 0, fmt.Errorf("%s overflows int32", field)
	}
	return int32(value), nil
}

func toInt16(value int32, field string) (int16, error) {
	if value > math.MaxInt16 || value < math.MinInt16 {
		return 0, fmt.Errorf("%s overflows int16", field)
	}
	return int16(value), nil
}

func ensureResource(resourceSpans *tracev1.ResourceSpans) *resourcev1.Resource {
	resource := resourceSpans.GetResource()
	if resource == nil {
		resource = &resourcev1.Resource{}
		resourceSpans.Resource = resource
	}
	return resource
}

func injectIdentity(resource *resourcev1.Resource, chain identity.IdentityChain) {
	upsertResourceAttribute(resource, attrIdentityID, chain.IdentityID)
	upsertResourceAttribute(resource, attrAgentID, chain.AgentID)
	upsertResourceAttribute(resource, attrOrganizationID, chain.OrganizationID)
}

func upsertResourceAttribute(resource *resourcev1.Resource, key, value string) {
	attrs := make([]*commonv1.KeyValue, 0, len(resource.Attributes)+1)
	for _, attr := range resource.Attributes {
		if attr.GetKey() == key {
			continue
		}
		attrs = append(attrs, attr)
	}
	attrs = append(attrs, stringAttr(key, value))
	resource.Attributes = attrs
}

func stringAttr(key, value string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: value}},
	}
}

func resourceStringAttribute(resource *resourcev1.Resource, key string) (string, error) {
	if resource == nil {
		return "", nil
	}
	for _, attr := range resource.Attributes {
		if attr.GetKey() != key {
			continue
		}
		value := attr.GetValue()
		if value == nil {
			return "", fmt.Errorf("attribute value missing")
		}
		switch typed := value.GetValue().(type) {
		case *commonv1.AnyValue_StringValue:
			trimmed := strings.TrimSpace(typed.StringValue)
			if trimmed == "" {
				return "", fmt.Errorf("attribute value is empty")
			}
			return trimmed, nil
		default:
			return "", fmt.Errorf("attribute value must be a string")
		}
	}
	return "", nil
}
