package server

import (
	"context"
	"errors"
	"fmt"
	"math"

	tracingv1 "github.com/agynio/tracing/.gen/go/agynio/api/tracing/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agynio/tracing/internal/store"
)

const (
	traceIDLength = 16
	spanIDLength  = 8
)

type Server struct {
	tracingv1.UnimplementedTracingServiceServer
	store *store.Store
}

func New(store *store.Store) *Server {
	return &Server{store: store}
}

func (s *Server) ListSpans(ctx context.Context, req *tracingv1.ListSpansRequest) (*tracingv1.ListSpansResponse, error) {
	if req.GetOrganizationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id is required")
	}
	filter, err := parseSpanFilter(req.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "filter: %v", err)
	}
	filter.OrganizationID = req.GetOrganizationId()
	orderBy, err := orderByFromProto(req.GetOrderBy())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "order_by: %v", err)
	}
	var cursor *store.SpanCursor
	if token := req.GetPageToken(); token != "" {
		tokenCursor, tokenOrderBy, err := store.DecodeSpanPageToken(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		if orderBy == store.OrderByUnspecified {
			orderBy = tokenOrderBy
		} else if tokenOrderBy != orderBy {
			return nil, status.Error(codes.InvalidArgument, "page_token order_by does not match request")
		}
		cursor = &tokenCursor
	}

	result, err := s.store.ListSpans(ctx, filter, req.GetPageSize(), cursor, orderBy)
	if err != nil {
		return nil, toStatusError(err)
	}
	resourceSpans, err := SpanRowsToResourceSpans(result.Spans)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build resource spans: %v", err)
	}
	resp := &tracingv1.ListSpansResponse{ResourceSpans: resourceSpans}
	if result.NextCursor != nil {
		token, err := store.EncodeSpanPageToken(*result.NextCursor, orderBy)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode page token: %v", err)
		}
		resp.NextPageToken = token
	}
	return resp, nil
}

func (s *Server) GetSpan(ctx context.Context, req *tracingv1.GetSpanRequest) (*tracingv1.GetSpanResponse, error) {
	if err := validateTraceID(req.GetTraceId()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "trace_id: %v", err)
	}
	if err := validateSpanID(req.GetSpanId()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "span_id: %v", err)
	}
	row, err := s.store.GetSpan(ctx, req.GetTraceId(), req.GetSpanId())
	if err != nil {
		return nil, toStatusError(err)
	}
	resourceSpans, err := SpanRowsToResourceSpans([]store.SpanRow{row})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build resource spans: %v", err)
	}
	return &tracingv1.GetSpanResponse{ResourceSpans: resourceSpans}, nil
}

func (s *Server) GetTrace(ctx context.Context, req *tracingv1.GetTraceRequest) (*tracingv1.GetTraceResponse, error) {
	if err := validateTraceID(req.GetTraceId()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "trace_id: %v", err)
	}
	rows, err := s.store.GetTrace(ctx, req.GetTraceId())
	if err != nil {
		return nil, toStatusError(err)
	}
	resourceSpans, err := SpanRowsToResourceSpans(rows)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build resource spans: %v", err)
	}
	return &tracingv1.GetTraceResponse{ResourceSpans: resourceSpans}, nil
}

func (s *Server) GetTraceSummary(ctx context.Context, req *tracingv1.GetTraceSummaryRequest) (*tracingv1.GetTraceSummaryResponse, error) {
	if err := validateTraceID(req.GetTraceId()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "trace_id: %v", err)
	}
	summary, err := s.store.GetTraceSummary(ctx, req.GetTraceId())
	if err != nil {
		return nil, toStatusError(err)
	}
	countsByName := make(map[string]int64, len(summary.Rows))
	var (
		runningCount int64
		okCount      int64
		errorCount   int64
	)
	for _, row := range summary.Rows {
		countsByName[row.Name] = row.NameCount
		runningCount += row.RunningCount
		okCount += row.OkCount
		errorCount += row.ErrorCount
	}
	countsByStatus := map[string]int64{
		tracingv1.SpanStatus_SPAN_STATUS_RUNNING.String(): runningCount,
		tracingv1.SpanStatus_SPAN_STATUS_OK.String():      okCount,
		tracingv1.SpanStatus_SPAN_STATUS_ERROR.String():   errorCount,
	}
	traceStatus := tracingv1.TraceStatus_TRACE_STATUS_COMPLETED
	if runningCount > 0 {
		traceStatus = tracingv1.TraceStatus_TRACE_STATUS_RUNNING
	} else if errorCount > 0 {
		traceStatus = tracingv1.TraceStatus_TRACE_STATUS_ERROR
	}
	return &tracingv1.GetTraceSummaryResponse{
		TraceId:            summary.TraceID,
		Status:             traceStatus,
		FirstSpanStartTime: uint64(summary.FirstSpanStartTime),
		LastSpanStartTime:  uint64(summary.LastSpanStartTime),
		LastSpanEndTime:    uint64(summary.LastSpanEndTime),
		CountsByName:       countsByName,
		CountsByStatus:     countsByStatus,
		TotalSpans:         summary.TotalSpans,
	}, nil
}

func (s *Server) GetTraceSpanTotals(ctx context.Context, req *tracingv1.GetTraceSpanTotalsRequest) (*tracingv1.GetTraceSpanTotalsResponse, error) {
	if err := validateTraceID(req.GetTraceId()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "trace_id: %v", err)
	}
	filter := store.TraceSpanTotalsFilter{
		TraceID: req.GetTraceId(),
	}
	if len(req.GetNames()) > 0 {
		filter.Names = req.GetNames()
	}
	if len(req.GetStatuses()) > 0 {
		statuses, err := mapSpanStatuses(req.GetStatuses())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "statuses: %v", err)
		}
		filter.Statuses = statuses
	}
	result, err := s.store.GetTraceSpanTotals(ctx, filter)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &tracingv1.GetTraceSpanTotalsResponse{
		SpanCount: result.SpanCount,
		TokenUsage: &tracingv1.TokenUsageTotals{
			InputTokens:          result.InputTokens,
			OutputTokens:         result.OutputTokens,
			CacheReadInputTokens: result.CacheReadInputTokens,
			ReasoningTokens:      result.ReasoningTokens,
			TotalTokens:          result.InputTokens + result.OutputTokens,
		},
	}, nil
}

func parseSpanFilter(filter *tracingv1.SpanFilter) (store.SpanFilter, error) {
	if filter == nil {
		return store.SpanFilter{}, nil
	}
	result := store.SpanFilter{}
	if len(filter.GetTraceId()) > 0 {
		if err := validateTraceID(filter.GetTraceId()); err != nil {
			return store.SpanFilter{}, fmt.Errorf("trace_id: %w", err)
		}
		result.TraceID = filter.GetTraceId()
	}
	if len(filter.GetParentSpanId()) > 0 {
		if err := validateSpanID(filter.GetParentSpanId()); err != nil {
			return store.SpanFilter{}, fmt.Errorf("parent_span_id: %w", err)
		}
		result.ParentSpanID = filter.GetParentSpanId()
	}
	if len(filter.GetNames()) > 0 {
		result.Names = filter.GetNames()
	} else if filter.GetName() != "" {
		result.Name = filter.GetName()
	}
	if filter.GetKind() != 0 {
		result.Kind = int16(filter.GetKind())
	}
	if filter.GetStartTimeMin() != 0 {
		if filter.GetStartTimeMin() > math.MaxInt64 {
			return store.SpanFilter{}, fmt.Errorf("start_time_min overflows int64")
		}
		result.StartTimeMin = int64(filter.GetStartTimeMin())
	}
	if filter.GetStartTimeMax() != 0 {
		if filter.GetStartTimeMax() > math.MaxInt64 {
			return store.SpanFilter{}, fmt.Errorf("start_time_max overflows int64")
		}
		result.StartTimeMax = int64(filter.GetStartTimeMax())
	}
	if len(filter.GetStatuses()) > 0 {
		statuses, err := mapSpanStatuses(filter.GetStatuses())
		if err != nil {
			return store.SpanFilter{}, fmt.Errorf("statuses: %w", err)
		}
		result.Statuses = statuses
	} else if filter.InProgress != nil {
		value := filter.GetInProgress()
		result.InProgress = &value
	}
	if filter.GetMessageId() != "" {
		result.MessageID = filter.GetMessageId()
	}
	return result, nil
}

func mapSpanStatuses(statuses []tracingv1.SpanStatus) ([]store.SpanStatus, error) {
	values := make([]store.SpanStatus, 0, len(statuses))
	for _, statusValue := range statuses {
		status, err := spanStatusFromProto(statusValue)
		if err != nil {
			return nil, err
		}
		values = append(values, status)
	}
	return values, nil
}

func spanStatusFromProto(status tracingv1.SpanStatus) (store.SpanStatus, error) {
	switch status {
	case tracingv1.SpanStatus_SPAN_STATUS_RUNNING:
		return store.SpanStatusRunning, nil
	case tracingv1.SpanStatus_SPAN_STATUS_OK:
		return store.SpanStatusOk, nil
	case tracingv1.SpanStatus_SPAN_STATUS_ERROR:
		return store.SpanStatusError, nil
	case tracingv1.SpanStatus_SPAN_STATUS_UNSPECIFIED:
		return 0, fmt.Errorf("status is unspecified")
	default:
		return 0, fmt.Errorf("invalid status: %d", status)
	}
}

func orderByFromProto(orderBy tracingv1.ListSpansOrderBy) (store.OrderBy, error) {
	switch orderBy {
	case tracingv1.ListSpansOrderBy_LIST_SPANS_ORDER_BY_UNSPECIFIED:
		return store.OrderByUnspecified, nil
	case tracingv1.ListSpansOrderBy_LIST_SPANS_ORDER_BY_START_TIME_DESC:
		return store.OrderByStartTimeDesc, nil
	case tracingv1.ListSpansOrderBy_LIST_SPANS_ORDER_BY_START_TIME_ASC:
		return store.OrderByStartTimeAsc, nil
	default:
		return store.OrderByUnspecified, fmt.Errorf("invalid order_by: %d", orderBy)
	}
}

func validateTraceID(traceID []byte) error {
	if len(traceID) != traceIDLength {
		return fmt.Errorf("must be %d bytes", traceIDLength)
	}
	return nil
}

func validateSpanID(spanID []byte) error {
	if len(spanID) != spanIDLength {
		return fmt.Errorf("must be %d bytes", spanIDLength)
	}
	return nil
}

func toStatusError(err error) error {
	switch {
	case errors.Is(err, store.ErrSpanNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrTraceNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}
