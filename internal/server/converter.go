package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/agynio/tracing/internal/store"
)

var protoJSON = protojson.MarshalOptions{EmitUnpopulated: true}

type resourceGroup struct {
	resource   *resourcev1.Resource
	scopeOrder [][32]byte
	scopes     map[[32]byte]*scopeGroup
}

type scopeGroup struct {
	scope *commonv1.InstrumentationScope
	spans []*tracev1.Span
}

func SpanRowsToResourceSpans(rows []store.SpanRow) ([]*tracev1.ResourceSpans, error) {
	if len(rows) == 0 {
		return []*tracev1.ResourceSpans{}, nil
	}
	resourceGroups := map[[32]byte]*resourceGroup{}
	resourceOrder := make([][32]byte, 0)
	for i := range rows {
		row := rows[i]
		resourceHash := sha256.Sum256(row.Resource)
		group, ok := resourceGroups[resourceHash]
		if !ok {
			resource, err := unmarshalResource(row.Resource)
			if err != nil {
				return nil, fmt.Errorf("unmarshal resource: %w", err)
			}
			group = &resourceGroup{
				resource: resource,
				scopes:   map[[32]byte]*scopeGroup{},
			}
			resourceGroups[resourceHash] = group
			resourceOrder = append(resourceOrder, resourceHash)
		}
		scopeHash := hashResourceScope(row.Resource, row.InstrumentationScope)
		scope, ok := group.scopes[scopeHash]
		if !ok {
			scopeValue, err := unmarshalInstrumentationScope(row.InstrumentationScope)
			if err != nil {
				return nil, fmt.Errorf("unmarshal instrumentation scope: %w", err)
			}
			scope = &scopeGroup{scope: scopeValue}
			group.scopes[scopeHash] = scope
			group.scopeOrder = append(group.scopeOrder, scopeHash)
		}
		span, err := spanRowToProtoSpan(&row)
		if err != nil {
			return nil, err
		}
		scope.spans = append(scope.spans, span)
	}

	resourceSpans := make([]*tracev1.ResourceSpans, 0, len(resourceOrder))
	for _, resourceHash := range resourceOrder {
		group := resourceGroups[resourceHash]
		scopeSpans := make([]*tracev1.ScopeSpans, 0, len(group.scopeOrder))
		for _, scopeHash := range group.scopeOrder {
			scopeGroup := group.scopes[scopeHash]
			scopeSpans = append(scopeSpans, &tracev1.ScopeSpans{
				Scope: scopeGroup.scope,
				Spans: scopeGroup.spans,
			})
		}
		resourceSpans = append(resourceSpans, &tracev1.ResourceSpans{
			Resource:   group.resource,
			ScopeSpans: scopeSpans,
		})
	}
	return resourceSpans, nil
}

func spanRowToProtoSpan(row *store.SpanRow) (*tracev1.Span, error) {
	attributes, err := unmarshalKeyValues(row.Attributes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal attributes: %w", err)
	}
	events, err := unmarshalEvents(row.Events)
	if err != nil {
		return nil, fmt.Errorf("unmarshal events: %w", err)
	}
	links, err := unmarshalLinks(row.Links)
	if err != nil {
		return nil, fmt.Errorf("unmarshal links: %w", err)
	}
	span := &tracev1.Span{
		TraceId:                row.TraceID,
		SpanId:                 row.SpanID,
		TraceState:             row.TraceState,
		ParentSpanId:           row.ParentSpanID,
		Flags:                  uint32(row.Flags),
		Name:                   row.Name,
		Kind:                   tracev1.Span_SpanKind(row.Kind),
		StartTimeUnixNano:      uint64(row.StartTimeUnixNano),
		EndTimeUnixNano:        uint64(row.EndTimeUnixNano),
		Attributes:             attributes,
		DroppedAttributesCount: uint32(row.DroppedAttributesCount),
		Events:                 events,
		DroppedEventsCount:     uint32(row.DroppedEventsCount),
		Links:                  links,
		DroppedLinksCount:      uint32(row.DroppedLinksCount),
		Status: &tracev1.Status{
			Code:    tracev1.Status_StatusCode(row.StatusCode),
			Message: row.StatusMessage,
		},
	}
	return span, nil
}

func unmarshalResource(data []byte) (*resourcev1.Resource, error) {
	if len(data) == 0 {
		return nil, nil
	}
	resource := &resourcev1.Resource{}
	if err := protojson.Unmarshal(data, resource); err != nil {
		return nil, err
	}
	return resource, nil
}

func unmarshalInstrumentationScope(data []byte) (*commonv1.InstrumentationScope, error) {
	if len(data) == 0 {
		return nil, nil
	}
	scope := &commonv1.InstrumentationScope{}
	if err := protojson.Unmarshal(data, scope); err != nil {
		return nil, err
	}
	return scope, nil
}

func unmarshalKeyValues(data []byte) ([]*commonv1.KeyValue, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("attributes json is empty")
	}
	wrapper := wrapJSONArray("values", data)
	list := &commonv1.KeyValueList{}
	if err := protojson.Unmarshal(wrapper, list); err != nil {
		return nil, err
	}
	return list.Values, nil
}

func unmarshalEvents(data []byte) ([]*tracev1.Span_Event, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("events json is empty")
	}
	wrapper := wrapJSONArray("events", data)
	span := &tracev1.Span{}
	if err := protojson.Unmarshal(wrapper, span); err != nil {
		return nil, err
	}
	return span.Events, nil
}

func unmarshalLinks(data []byte) ([]*tracev1.Span_Link, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("links json is empty")
	}
	wrapper := wrapJSONArray("links", data)
	span := &tracev1.Span{}
	if err := protojson.Unmarshal(wrapper, span); err != nil {
		return nil, err
	}
	return span.Links, nil
}

func MarshalResource(resource *resourcev1.Resource) ([]byte, error) {
	if resource == nil {
		return nil, nil
	}
	data, err := protoJSON.Marshal(resource)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func MarshalInstrumentationScope(scope *commonv1.InstrumentationScope) ([]byte, error) {
	if scope == nil {
		return nil, nil
	}
	data, err := protoJSON.Marshal(scope)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func MarshalKeyValues(values []*commonv1.KeyValue) ([]byte, error) {
	list := &commonv1.KeyValueList{Values: values}
	data, err := protoJSON.Marshal(list)
	if err != nil {
		return nil, err
	}
	return extractJSONArrayField(data, "values")
}

func MarshalEvents(events []*tracev1.Span_Event) ([]byte, error) {
	span := &tracev1.Span{Events: events}
	data, err := protoJSON.Marshal(span)
	if err != nil {
		return nil, err
	}
	return extractJSONArrayField(data, "events")
}

func MarshalLinks(links []*tracev1.Span_Link) ([]byte, error) {
	span := &tracev1.Span{Links: links}
	data, err := protoJSON.Marshal(span)
	if err != nil {
		return nil, err
	}
	return extractJSONArrayField(data, "links")
}

func extractJSONArrayField(data []byte, field string) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal json: %w", err)
	}
	raw, ok := payload[field]
	if !ok {
		return nil, fmt.Errorf("missing field %s", field)
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("field %s is empty", field)
	}
	if trimmed[0] != '[' {
		return nil, fmt.Errorf("field %s is not an array", field)
	}
	return trimmed, nil
}

func wrapJSONArray(field string, data []byte) []byte {
	buf := make([]byte, 0, len(field)+len(data)+4)
	buf = append(buf, '{', '"')
	buf = append(buf, field...)
	buf = append(buf, '"', ':')
	buf = append(buf, data...)
	buf = append(buf, '}')
	return buf
}

func hashResourceScope(resource, scope []byte) [32]byte {
	hasher := sha256.New()
	_, _ = hasher.Write(resource)
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(scope)
	var sum [32]byte
	copy(sum[:], hasher.Sum(nil))
	return sum
}
