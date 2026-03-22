package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const spanColumns = "trace_id, span_id, trace_state, parent_span_id, flags, name, kind, start_time_unix_nano, end_time_unix_nano, attributes, dropped_attributes_count, events, dropped_events_count, links, dropped_links_count, status_code, status_message, resource, instrumentation_scope"

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) UpsertSpan(ctx context.Context, span SpanRow) (bool, error) {
	query := `INSERT INTO spans (
        trace_id,
        span_id,
        trace_state,
        parent_span_id,
        flags,
        name,
        kind,
        start_time_unix_nano,
        end_time_unix_nano,
        attributes,
        dropped_attributes_count,
        events,
        dropped_events_count,
        links,
        dropped_links_count,
        status_code,
        status_message,
        resource,
        instrumentation_scope
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
    ) ON CONFLICT (trace_id, span_id) DO UPDATE SET
        trace_state = EXCLUDED.trace_state,
        parent_span_id = EXCLUDED.parent_span_id,
        flags = EXCLUDED.flags,
        name = EXCLUDED.name,
        kind = EXCLUDED.kind,
        start_time_unix_nano = EXCLUDED.start_time_unix_nano,
        end_time_unix_nano = EXCLUDED.end_time_unix_nano,
        attributes = EXCLUDED.attributes,
        dropped_attributes_count = EXCLUDED.dropped_attributes_count,
        events = EXCLUDED.events,
        dropped_events_count = EXCLUDED.dropped_events_count,
        links = EXCLUDED.links,
        dropped_links_count = EXCLUDED.dropped_links_count,
        status_code = EXCLUDED.status_code,
        status_message = EXCLUDED.status_message,
        resource = EXCLUDED.resource,
        instrumentation_scope = EXCLUDED.instrumentation_scope
    RETURNING (xmax = 0) AS is_new`
	var isNew bool
	if err := s.pool.QueryRow(
		ctx,
		query,
		span.TraceID,
		span.SpanID,
		span.TraceState,
		span.ParentSpanID,
		span.Flags,
		span.Name,
		span.Kind,
		span.StartTimeUnixNano,
		span.EndTimeUnixNano,
		string(span.Attributes),
		span.DroppedAttributesCount,
		string(span.Events),
		span.DroppedEventsCount,
		string(span.Links),
		span.DroppedLinksCount,
		span.StatusCode,
		span.StatusMessage,
		nullableJSONText(span.Resource),
		nullableJSONText(span.InstrumentationScope),
	).Scan(&isNew); err != nil {
		return false, err
	}
	return isNew, nil
}

func (s *Store) GetSpan(ctx context.Context, traceID, spanID []byte) (SpanRow, error) {
	query := fmt.Sprintf("SELECT %s FROM spans WHERE trace_id = $1 AND span_id = $2", spanColumns)
	span, err := scanSpanRow(s.pool.QueryRow(ctx, query, traceID, spanID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SpanRow{}, ErrSpanNotFound
		}
		return SpanRow{}, err
	}
	return span, nil
}

func (s *Store) GetTrace(ctx context.Context, traceID []byte) ([]SpanRow, error) {
	query := fmt.Sprintf("SELECT %s FROM spans WHERE trace_id = $1 ORDER BY start_time_unix_nano ASC, span_id ASC", spanColumns)
	rows, err := s.pool.Query(ctx, query, traceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	spans := []SpanRow{}
	for rows.Next() {
		span, err := scanSpanRow(rows)
		if err != nil {
			return nil, err
		}
		spans = append(spans, span)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(spans) == 0 {
		return nil, ErrTraceNotFound
	}
	return spans, nil
}

func (s *Store) ListSpans(ctx context.Context, filter SpanFilter, pageSize int32, cursor *SpanCursor, orderBy OrderBy) (SpanListResult, error) {
	limit := normalizePageSize(pageSize)
	orderBy = normalizeOrderBy(orderBy)

	query := strings.Builder{}
	query.WriteString("SELECT ")
	query.WriteString(spanColumns)
	query.WriteString(" FROM spans")

	args := []any{}
	conditions := []string{}
	paramIndex := 1

	if len(filter.TraceID) > 0 {
		conditions = append(conditions, fmt.Sprintf("trace_id = $%d", paramIndex))
		args = append(args, filter.TraceID)
		paramIndex++
	}
	if len(filter.ParentSpanID) > 0 {
		conditions = append(conditions, fmt.Sprintf("parent_span_id = $%d", paramIndex))
		args = append(args, filter.ParentSpanID)
		paramIndex++
	}
	if filter.Name != "" {
		conditions = append(conditions, fmt.Sprintf("name = $%d", paramIndex))
		args = append(args, filter.Name)
		paramIndex++
	}
	if filter.Kind != 0 {
		conditions = append(conditions, fmt.Sprintf("kind = $%d", paramIndex))
		args = append(args, filter.Kind)
		paramIndex++
	}
	if filter.StartTimeMin != 0 {
		conditions = append(conditions, fmt.Sprintf("start_time_unix_nano >= $%d", paramIndex))
		args = append(args, filter.StartTimeMin)
		paramIndex++
	}
	if filter.StartTimeMax != 0 {
		conditions = append(conditions, fmt.Sprintf("start_time_unix_nano <= $%d", paramIndex))
		args = append(args, filter.StartTimeMax)
		paramIndex++
	}
	if filter.InProgress != nil {
		if *filter.InProgress {
			conditions = append(conditions, "end_time_unix_nano = 0")
		} else {
			conditions = append(conditions, "end_time_unix_nano <> 0")
		}
	}

	comparison := "<"
	orderDirection := "DESC"
	if orderBy == OrderByStartTimeAsc {
		comparison = ">"
		orderDirection = "ASC"
	}
	if cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(start_time_unix_nano, trace_id, span_id) %s ($%d, $%d, $%d)", comparison, paramIndex, paramIndex+1, paramIndex+2))
		args = append(args, cursor.StartTimeUnixNano, cursor.TraceID, cursor.SpanID)
		paramIndex += 3
	}
	if len(conditions) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(conditions, " AND "))
	}
	query.WriteString(fmt.Sprintf(" ORDER BY start_time_unix_nano %s, trace_id %s, span_id %s", orderDirection, orderDirection, orderDirection))
	query.WriteString(fmt.Sprintf(" LIMIT $%d", paramIndex))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return SpanListResult{}, err
	}
	defer rows.Close()

	spans := make([]SpanRow, 0, limit)
	var (
		nextCursor *SpanCursor
		lastSpan   SpanRow
		hasMore    bool
	)
	for rows.Next() {
		span, err := scanSpanRow(rows)
		if err != nil {
			return SpanListResult{}, err
		}
		if int32(len(spans)) == limit {
			hasMore = true
			break
		}
		spans = append(spans, span)
		lastSpan = span
	}
	if err := rows.Err(); err != nil {
		return SpanListResult{}, err
	}
	if hasMore {
		nextCursor = &SpanCursor{
			StartTimeUnixNano: lastSpan.StartTimeUnixNano,
			TraceID:           lastSpan.TraceID,
			SpanID:            lastSpan.SpanID,
		}
	}
	return SpanListResult{Spans: spans, NextCursor: nextCursor}, nil
}

type spanScanner interface {
	Scan(...any) error
}

func scanSpanRow(scanner spanScanner) (SpanRow, error) {
	var span SpanRow
	if err := scanner.Scan(
		&span.TraceID,
		&span.SpanID,
		&span.TraceState,
		&span.ParentSpanID,
		&span.Flags,
		&span.Name,
		&span.Kind,
		&span.StartTimeUnixNano,
		&span.EndTimeUnixNano,
		&span.Attributes,
		&span.DroppedAttributesCount,
		&span.Events,
		&span.DroppedEventsCount,
		&span.Links,
		&span.DroppedLinksCount,
		&span.StatusCode,
		&span.StatusMessage,
		&span.Resource,
		&span.InstrumentationScope,
	); err != nil {
		return SpanRow{}, err
	}
	return span, nil
}

func nullableJSONText(data []byte) any {
	if data == nil {
		return nil
	}
	return string(data)
}
