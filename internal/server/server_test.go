package server

import (
	"context"
	"errors"
	"testing"

	tracingv1 "github.com/agynio/tracing/.gen/go/agynio/api/tracing/v1"
	"github.com/agynio/tracing/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockSpanStore struct {
	listSpans          func(ctx context.Context, filter store.SpanFilter, pageSize int32, cursor *store.SpanCursor, orderBy store.OrderBy) (store.SpanListResult, error)
	getSpan            func(ctx context.Context, traceID, spanID []byte) (store.SpanRow, error)
	getTrace           func(ctx context.Context, traceID []byte) ([]store.SpanRow, error)
	getTraceSummary    func(ctx context.Context, traceID []byte) (store.TraceSummary, error)
	getTraceSpanTotals func(ctx context.Context, filter store.TraceSpanTotalsFilter) (store.TraceSpanTotals, error)
}

func (m *mockSpanStore) ListSpans(ctx context.Context, filter store.SpanFilter, pageSize int32, cursor *store.SpanCursor, orderBy store.OrderBy) (store.SpanListResult, error) {
	if m.listSpans == nil {
		return store.SpanListResult{}, errors.New("not implemented")
	}
	return m.listSpans(ctx, filter, pageSize, cursor, orderBy)
}

func (m *mockSpanStore) GetSpan(ctx context.Context, traceID, spanID []byte) (store.SpanRow, error) {
	if m.getSpan == nil {
		return store.SpanRow{}, errors.New("not implemented")
	}
	return m.getSpan(ctx, traceID, spanID)
}

func (m *mockSpanStore) GetTrace(ctx context.Context, traceID []byte) ([]store.SpanRow, error) {
	if m.getTrace == nil {
		return nil, errors.New("not implemented")
	}
	return m.getTrace(ctx, traceID)
}

func (m *mockSpanStore) GetTraceSummary(ctx context.Context, traceID []byte) (store.TraceSummary, error) {
	if m.getTraceSummary == nil {
		return store.TraceSummary{}, errors.New("not implemented")
	}
	return m.getTraceSummary(ctx, traceID)
}

func (m *mockSpanStore) GetTraceSpanTotals(ctx context.Context, filter store.TraceSpanTotalsFilter) (store.TraceSpanTotals, error) {
	if m.getTraceSpanTotals == nil {
		return store.TraceSpanTotals{}, errors.New("not implemented")
	}
	return m.getTraceSpanTotals(ctx, filter)
}

func TestListSpansRequiresOrganizationID(t *testing.T) {
	server := New(nil)
	_, err := server.ListSpans(context.Background(), &tracingv1.ListSpansRequest{})
	require.Error(t, err)

	statusErr, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, statusErr.Code())
	assert.Contains(t, statusErr.Message(), "organization_id")
}

func TestGetTraceSummaryMapsCategoryCountsByName(t *testing.T) {
	traceID := []byte("1234567890123456")
	server := New(&mockSpanStore{
		getTraceSummary: func(_ context.Context, gotTraceID []byte) (store.TraceSummary, error) {
			assert.Equal(t, traceID, gotTraceID)
			return store.TraceSummary{
				TraceID:            gotTraceID,
				TotalSpans:         7,
				FirstSpanStartTime: 10,
				LastSpanStartTime:  20,
				LastSpanEndTime:    30,
				Rows: []store.TraceSummaryRow{
					{Name: "chat.completions", NameCount: 3, OkCount: 3},
				},
				CategoryCounts: map[store.SpanCategory]int64{
					store.SpanCategoryMessage: 1,
					store.SpanCategoryLLM:     3,
					store.SpanCategoryTool:    2,
				},
			}, nil
		},
	})

	resp, err := server.GetTraceSummary(context.Background(), &tracingv1.GetTraceSummaryRequest{TraceId: traceID})
	require.NoError(t, err)

	assert.Equal(t, traceID, resp.GetTraceId())
	assert.Equal(t, tracingv1.TraceStatus_TRACE_STATUS_COMPLETED, resp.GetStatus())
	assert.Equal(t, int64(3), resp.GetCountsByName()["chat.completions"])
	assert.Equal(t, int64(1), resp.GetCountsByName()["message"])
	assert.Equal(t, int64(3), resp.GetCountsByName()["llm"])
	assert.Equal(t, int64(2), resp.GetCountsByName()["tool"])
	assert.Equal(t, int64(3), resp.GetCountsByStatus()[tracingv1.SpanStatus_SPAN_STATUS_OK.String()])
	assert.Equal(t, int64(7), resp.GetTotalSpans())
}
