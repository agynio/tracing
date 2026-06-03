package store

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxmock "github.com/pashagolub/pgxmock/v4"
)

func TestListSpansMessageIDPrefersApplicationSpans(t *testing.T) {
	matcher := newQueryRecorder(t)
	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatalf("create pgx mock: %v", err)
	}
	defer mock.Close()

	filter := SpanFilter{
		OrganizationID: "org-id",
		MessageID:      "message-id",
	}

	mock.ExpectQuery("list message spans").
		WithArgs(filter.OrganizationID, filter.MessageID, 51).
		WillReturnRows(emptySpanRows())

	_, err = newStoreWithQuerier(mock).ListSpans(context.Background(), filter, 50, nil, OrderByStartTimeDesc)
	if err != nil {
		t.Fatalf("list spans: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}

	query := matcher.queries[0]
	if !containsAll(query, "message_id = $2", "invocation.message", "gen_ai.system", "agyn.tool.name") {
		t.Fatalf("expected message lookup to include application span preference, got %s", query)
	}
}

func TestListSpansMessageIDUsesGeneratedColumn(t *testing.T) {
	matcher := newQueryRecorder(t)
	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatalf("create pgx mock: %v", err)
	}
	defer mock.Close()

	filter := SpanFilter{
		OrganizationID: "org-id",
		MessageID:      "message-id",
	}

	mock.ExpectQuery("list message spans").
		WithArgs(filter.OrganizationID, filter.MessageID, 51).
		WillReturnRows(emptySpanRows())

	_, err = newStoreWithQuerier(mock).ListSpans(context.Background(), filter, 50, nil, OrderByStartTimeDesc)
	if err != nil {
		t.Fatalf("list spans: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}

	query := matcher.queries[0]
	if !containsAll(query, "message_id = $2") {
		t.Fatalf("expected message lookup to use generated message_id column, got %s", query)
	}
	if containsAll(query, "agyn.thread.message.id") {
		t.Fatalf("expected message lookup to avoid duplicating generated-column expression, got %s", query)
	}
}

func TestListSpansMessageIDTraceScopedKeepsExactTrace(t *testing.T) {
	matcher := newQueryRecorder(t)
	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatalf("create pgx mock: %v", err)
	}
	defer mock.Close()

	filter := SpanFilter{
		OrganizationID: "org-id",
		TraceID:        []byte("trace-id-0000000"),
		MessageID:      "message-id",
	}

	mock.ExpectQuery("list trace message spans").
		WithArgs(filter.OrganizationID, filter.TraceID, filter.MessageID, 51).
		WillReturnRows(emptySpanRows())

	_, err = newStoreWithQuerier(mock).ListSpans(context.Background(), filter, 50, nil, OrderByStartTimeDesc)
	if err != nil {
		t.Fatalf("list spans: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}

	query := matcher.queries[0]
	if !containsAll(query, "trace_id = $2", "message_id = $3") {
		t.Fatalf("expected trace-scoped message query, got %s", query)
	}
	if containsAll(query, "gen_ai.system", "agyn.tool.name") {
		t.Fatalf("expected trace-scoped message query to avoid app-span preference, got %s", query)
	}
}

func TestGetTraceSummaryIncludesCategoryCounts(t *testing.T) {
	matcher := newQueryRecorder(t)
	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatalf("create pgx mock: %v", err)
	}
	defer mock.Close()

	traceID := []byte("trace-id-0000000")
	mock.ExpectQuery("trace summary names").
		WithArgs(traceID).
		WillReturnRows(pgxmock.NewRows([]string{
			"name",
			"name_count",
			"first_start",
			"last_start",
			"last_end",
			"running_count",
			"ok_count",
			"error_count",
		}).AddRow("chat.completions", int64(2), int64(10), int64(20), int64(30), int64(0), int64(2), int64(0)))
	mock.ExpectQuery("trace summary categories").
		WithArgs(traceID).
		WillReturnRows(pgxmock.NewRows([]string{
			"message_count",
			"llm_count",
			"tool_count",
		}).AddRow(int64(1), int64(2), int64(1)))

	summary, err := newStoreWithQuerier(mock).GetTraceSummary(context.Background(), traceID)
	if err != nil {
		t.Fatalf("get trace summary: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}

	if summary.CategoryCounts[SpanCategoryMessage] != 1 {
		t.Fatalf("expected message count 1, got %d", summary.CategoryCounts[SpanCategoryMessage])
	}
	if summary.CategoryCounts[SpanCategoryLLM] != 2 {
		t.Fatalf("expected llm count 2, got %d", summary.CategoryCounts[SpanCategoryLLM])
	}
	if summary.CategoryCounts[SpanCategoryTool] != 1 {
		t.Fatalf("expected tool count 1, got %d", summary.CategoryCounts[SpanCategoryTool])
	}

	categoryQuery := matcher.queries[1]
	if !containsAll(categoryQuery, "gen_ai.system", "gen_ai.request.model", "agyn.tool.name", "agyn.message.text") {
		t.Fatalf("expected provider-specific category classification, got %s", categoryQuery)
	}
}

func emptySpanRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"trace_id",
		"span_id",
		"trace_state",
		"parent_span_id",
		"flags",
		"name",
		"kind",
		"start_time_unix_nano",
		"end_time_unix_nano",
		"attributes",
		"dropped_attributes_count",
		"events",
		"dropped_events_count",
		"links",
		"dropped_links_count",
		"status_code",
		"status_message",
		"resource",
		"instrumentation_scope",
	})
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !regexp.MustCompile(regexp.QuoteMeta(part)).MatchString(value) {
			return false
		}
	}
	return true
}

type queryRecorder struct {
	t       *testing.T
	queries []string
}

func newQueryRecorder(t *testing.T) *queryRecorder {
	t.Helper()
	return &queryRecorder{t: t}
}

func (r *queryRecorder) Match(expectedSQL, actualSQL string) error {
	if strings.TrimSpace(actualSQL) == "" {
		return errors.New("query is empty")
	}
	r.queries = append(r.queries, actualSQL)
	return nil
}

var _ pgxmock.QueryMatcher = (*queryRecorder)(nil)

var _ querier = (pgxmock.PgxPoolIface)(nil)
var _ querier = (*pgxpool.Pool)(nil)
var _ = pgconn.CommandTag{}
