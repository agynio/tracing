package store

import "errors"

var (
	ErrSpanNotFound  = errors.New("span not found")
	ErrTraceNotFound = errors.New("trace not found")
)

type SpanRow struct {
	TraceID                []byte
	SpanID                 []byte
	TraceState             string
	ParentSpanID           []byte
	Flags                  int32
	Name                   string
	Kind                   int16
	StartTimeUnixNano      int64
	EndTimeUnixNano        int64
	Attributes             []byte
	DroppedAttributesCount int32
	Events                 []byte
	DroppedEventsCount     int32
	Links                  []byte
	DroppedLinksCount      int32
	StatusCode             int16
	StatusMessage          string
	Resource               []byte
	InstrumentationScope   []byte
}

type SpanFilter struct {
	TraceID      []byte
	ParentSpanID []byte
	Name         string
	Kind         int16
	StartTimeMin int64
	StartTimeMax int64
	InProgress   *bool
}

type SpanCursor struct {
	StartTimeUnixNano int64
	TraceID           []byte
	SpanID            []byte
}

type SpanListResult struct {
	Spans      []SpanRow
	NextCursor *SpanCursor
}

type OrderBy int

const (
	OrderByUnspecified   OrderBy = 0
	OrderByStartTimeDesc OrderBy = 1
	OrderByStartTimeAsc  OrderBy = 2
)
