//go:build e2e

package e2e_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	tracingv1 "github.com/agynio/tracing/.gen/go/agynio/api/tracing/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	pollInterval             = 200 * time.Millisecond
	pollTimeout              = 12 * time.Second
	requestTimeout           = 5 * time.Second
	instrumentationScopeName = "tracing-e2e"
)

type spanOption func(*tracev1.Span)

func withParentSpanID(parentSpanID []byte) spanOption {
	return func(span *tracev1.Span) {
		span.ParentSpanId = parentSpanID
	}
}

func withStatus(code tracev1.Status_StatusCode, message string) spanOption {
	return func(span *tracev1.Span) {
		span.Status = &tracev1.Status{Code: code, Message: message}
	}
}

func withEvents(events ...*tracev1.Span_Event) spanOption {
	return func(span *tracev1.Span) {
		span.Events = events
	}
}

func dialGRPC(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err, "dial %s", addr)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newTracingClients(t *testing.T) (collectortracev1.TraceServiceClient, tracingv1.TracingServiceClient) {
	t.Helper()
	conn := dialGRPC(t, tracingAddress)
	return collectortracev1.NewTraceServiceClient(conn), tracingv1.NewTracingServiceClient(conn)
}

func newTraceID() []byte {
	return mustRandBytes(16)
}

func newSpanID() []byte {
	return mustRandBytes(8)
}

func mustRandBytes(size int) []byte {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("rand: %v", err))
	}
	return buf
}

func buildSpan(
	name string,
	traceID []byte,
	spanID []byte,
	startNs uint64,
	endNs uint64,
	attrs []*commonv1.KeyValue,
	options ...spanOption,
) *tracev1.Span {
	span := &tracev1.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      make([]byte, 8),
		Name:              name,
		StartTimeUnixNano: startNs,
		EndTimeUnixNano:   endNs,
		Attributes:        attrs,
		Status:            &tracev1.Status{Code: tracev1.Status_STATUS_CODE_UNSET},
	}
	for _, option := range options {
		option(span)
	}
	return span
}

func buildResourceSpans(spans []*tracev1.Span, resourceAttrs []*commonv1.KeyValue) *tracev1.ResourceSpans {
	return &tracev1.ResourceSpans{
		Resource: &resourcev1.Resource{Attributes: resourceAttrs},
		ScopeSpans: []*tracev1.ScopeSpans{
			{
				Scope: &commonv1.InstrumentationScope{Name: instrumentationScopeName},
				Spans: spans,
			},
		},
	}
}

func stringAttr(key, value string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key: key,
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: value},
		},
	}
}

func intAttr(key string, value int64) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key: key,
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_IntValue{IntValue: value},
		},
	}
}

func exportSpans(
	ctx context.Context,
	client collectortracev1.TraceServiceClient,
	resourceSpans []*tracev1.ResourceSpans,
) (*collectortracev1.ExportTraceServiceResponse, error) {
	return client.Export(ctx, &collectortracev1.ExportTraceServiceRequest{ResourceSpans: resourceSpans})
}

func requireEventually(t *testing.T, assertion func(t *assert.CollectT)) {
	t.Helper()
	require.EventuallyWithT(t, assertion, pollTimeout, pollInterval)
}

func attributeMap(attrs []*commonv1.KeyValue) map[string]*commonv1.AnyValue {
	values := make(map[string]*commonv1.AnyValue, len(attrs))
	for _, attr := range attrs {
		if attr == nil {
			continue
		}
		values[attr.Key] = attr.Value
	}
	return values
}

func assertStringAttr(t assert.TestingT, attrs []*commonv1.KeyValue, key, expected string) bool {
	attrMap := attributeMap(attrs)
	value, ok := attrMap[key]
	if !assert.True(t, ok, "missing attribute %s", key) {
		return false
	}
	return assert.Equal(t, expected, value.GetStringValue(), "attribute %s", key)
}

func assertIntAttr(t assert.TestingT, attrs []*commonv1.KeyValue, key string, expected int64) bool {
	attrMap := attributeMap(attrs)
	value, ok := attrMap[key]
	if !assert.True(t, ok, "missing attribute %s", key) {
		return false
	}
	return assert.Equal(t, expected, value.GetIntValue(), "attribute %s", key)
}

func flattenSpans(resourceSpans []*tracev1.ResourceSpans) []*tracev1.Span {
	var spans []*tracev1.Span
	for _, resourceSpan := range resourceSpans {
		for _, scopeSpans := range resourceSpan.GetScopeSpans() {
			spans = append(spans, scopeSpans.GetSpans()...)
		}
	}
	return spans
}

func singleSpan(t assert.TestingT, resourceSpans []*tracev1.ResourceSpans) *tracev1.Span {
	spans := flattenSpans(resourceSpans)
	if !assert.Len(t, spans, 1) {
		return nil
	}
	return spans[0]
}

func firstResource(t assert.TestingT, resourceSpans []*tracev1.ResourceSpans) *resourcev1.Resource {
	if !assert.NotEmpty(t, resourceSpans) {
		return nil
	}
	resource := resourceSpans[0].GetResource()
	if !assert.NotNil(t, resource) {
		return nil
	}
	return resource
}

func spanNames(spans []*tracev1.Span) []string {
	names := make([]string, len(spans))
	for i, span := range spans {
		names[i] = span.GetName()
	}
	return names
}
