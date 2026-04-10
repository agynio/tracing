package ingest

import (
	"testing"

	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
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
