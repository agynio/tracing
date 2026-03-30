package store

import "errors"

var (
	ErrSpanNotFound  = errors.New("span not found")
	ErrTraceNotFound = errors.New("trace not found")
)

type SpanStatus int16

const (
	SpanStatusRunning SpanStatus = 1 // end_time = 0, status_code != 2 (ERROR)
	SpanStatusOk      SpanStatus = 2 // end_time > 0, status_code != 2
	SpanStatusError   SpanStatus = 3 // status_code = 2
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
	Names        []string
	Kind         int16
	StartTimeMin int64
	StartTimeMax int64
	InProgress   *bool
	Statuses     []SpanStatus
}

type TraceSummary struct {
	TraceID            []byte
	TotalSpans         int64
	FirstSpanStartTime int64
	LastSpanStartTime  int64
	LastSpanEndTime    int64
	Rows               []TraceSummaryRow
}

type TraceSummaryRow struct {
	Name         string
	NameCount    int64
	RunningCount int64
	OkCount      int64
	ErrorCount   int64
}

type TraceSpanTotalsFilter struct {
	TraceID  []byte
	Names    []string
	Statuses []SpanStatus
}

type TraceSpanTotals struct {
	SpanCount            int64
	InputTokens          int64
	OutputTokens         int64
	CacheReadInputTokens int64
	ReasoningTokens      int64
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
