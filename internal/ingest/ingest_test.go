package ingest

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/agynio/tracing/internal/identity"
)

func TestSpanToRowNormalizesParentSpanID(t *testing.T) {
	span := &tracev1.Span{
		TraceId:           []byte{0x01},
		SpanId:            []byte{0x02},
		ParentSpanId:      nil,
		StartTimeUnixNano: 1,
		EndTimeUnixNano:   2,
	}

	row, err := spanToRow(span, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, row.ParentSpanID)
	require.Len(t, row.ParentSpanID, 0)
}

func TestInjectIdentityDedupesAttributes(t *testing.T) {
	resource := &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
		stringAttr(attrIdentityID, "old-identity"),
		stringAttr("other", "keep"),
		stringAttr(attrIdentityID, "duplicate"),
		stringAttr(attrAgentID, "old-agent"),
		stringAttr(attrOrganizationID, "old-org"),
		stringAttr(attrOrganizationID, "duplicate-org"),
	}}

	injectIdentity(resource, identity.IdentityChain{
		IdentityID:     "identity",
		AgentID:        "agent",
		OrganizationID: "organization",
	})

	require.Equal(t, []string{"identity"}, attrValues(resource.Attributes, attrIdentityID))
	require.Equal(t, []string{"agent"}, attrValues(resource.Attributes, attrAgentID))
	require.Equal(t, []string{"organization"}, attrValues(resource.Attributes, attrOrganizationID))
	require.Equal(t, []string{"keep"}, attrValues(resource.Attributes, "other"))
}

func attrValues(attrs []*commonv1.KeyValue, key string) []string {
	values := []string{}
	for _, attr := range attrs {
		if attr.GetKey() != key {
			continue
		}
		if value := attr.GetValue(); value != nil {
			if typed, ok := value.GetValue().(*commonv1.AnyValue_StringValue); ok {
				values = append(values, typed.StringValue)
			}
		}
	}
	return values
}
