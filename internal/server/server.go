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
	filter, err := parseSpanFilter(req.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "filter: %v", err)
	}
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
	if filter.GetName() != "" {
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
	if filter.InProgress != nil {
		value := filter.GetInProgress()
		result.InProgress = &value
	}
	return result, nil
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
