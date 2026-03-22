package store

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	defaultPageSize int32 = 50
	maxPageSize     int32 = 100
)

func normalizePageSize(size int32) int32 {
	if size <= 0 {
		return defaultPageSize
	}
	if size > maxPageSize {
		return maxPageSize
	}
	return size
}

type spanPageToken struct {
	OrderBy           OrderBy `json:"order_by"`
	StartTimeUnixNano int64   `json:"start_time_unix_nano"`
	TraceID           string  `json:"trace_id"`
	SpanID            string  `json:"span_id"`
}

func EncodeSpanPageToken(cursor SpanCursor, orderBy OrderBy) (string, error) {
	payload := spanPageToken{
		OrderBy:           normalizeOrderBy(orderBy),
		StartTimeUnixNano: cursor.StartTimeUnixNano,
		TraceID:           hex.EncodeToString(cursor.TraceID),
		SpanID:            hex.EncodeToString(cursor.SpanID),
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func DecodeSpanPageToken(token string) (SpanCursor, OrderBy, error) {
	if token == "" {
		return SpanCursor{}, OrderByUnspecified, errors.New("empty token")
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return SpanCursor{}, OrderByUnspecified, fmt.Errorf("decode token: %w", err)
	}
	var payload spanPageToken
	if err := json.Unmarshal(data, &payload); err != nil {
		return SpanCursor{}, OrderByUnspecified, fmt.Errorf("unmarshal token: %w", err)
	}
	orderBy, err := parseOrderBy(payload.OrderBy)
	if err != nil {
		return SpanCursor{}, OrderByUnspecified, err
	}
	traceID, err := hex.DecodeString(payload.TraceID)
	if err != nil {
		return SpanCursor{}, OrderByUnspecified, fmt.Errorf("decode trace id: %w", err)
	}
	spanID, err := hex.DecodeString(payload.SpanID)
	if err != nil {
		return SpanCursor{}, OrderByUnspecified, fmt.Errorf("decode span id: %w", err)
	}
	return SpanCursor{
		StartTimeUnixNano: payload.StartTimeUnixNano,
		TraceID:           traceID,
		SpanID:            spanID,
	}, orderBy, nil
}

func normalizeOrderBy(orderBy OrderBy) OrderBy {
	switch orderBy {
	case OrderByStartTimeAsc, OrderByStartTimeDesc:
		return orderBy
	default:
		return OrderByStartTimeDesc
	}
}

func parseOrderBy(orderBy OrderBy) (OrderBy, error) {
	switch orderBy {
	case OrderByStartTimeAsc, OrderByStartTimeDesc:
		return orderBy, nil
	default:
		return OrderByUnspecified, fmt.Errorf("invalid order_by: %d", orderBy)
	}
}
