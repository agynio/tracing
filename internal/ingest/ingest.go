package ingest

import (
	"context"
	"fmt"
	"log"
	"math"

	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/agynio/tracing/internal/notifier"
	"github.com/agynio/tracing/internal/server"
	"github.com/agynio/tracing/internal/store"
)

type Handler struct {
	collectortracev1.UnimplementedTraceServiceServer
	store    *store.Store
	notifier *notifier.Notifier
}

func NewHandler(store *store.Store, notifier *notifier.Notifier) *Handler {
	return &Handler{store: store, notifier: notifier}
}

func (h *Handler) Export(ctx context.Context, req *collectortracev1.ExportTraceServiceRequest) (*collectortracev1.ExportTraceServiceResponse, error) {
	var rejectedSpans int64
	for _, resourceSpans := range req.GetResourceSpans() {
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
				if err := h.notifier.PublishSpanEvent(ctx, row.TraceID, isNew); err != nil {
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

	return store.SpanRow{
		TraceID:                span.GetTraceId(),
		SpanID:                 span.GetSpanId(),
		TraceState:             span.GetTraceState(),
		ParentSpanID:           span.GetParentSpanId(),
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
