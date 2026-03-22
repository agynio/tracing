CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE spans (
    trace_id BYTEA NOT NULL,
    span_id BYTEA NOT NULL,
    trace_state TEXT NOT NULL DEFAULT '',
    parent_span_id BYTEA NOT NULL DEFAULT '',
    flags INTEGER NOT NULL DEFAULT 0,
    name TEXT NOT NULL DEFAULT '',
    kind SMALLINT NOT NULL DEFAULT 0,
    start_time_unix_nano BIGINT NOT NULL DEFAULT 0,
    end_time_unix_nano BIGINT NOT NULL DEFAULT 0,
    attributes JSONB NOT NULL DEFAULT '[]',
    dropped_attributes_count INTEGER NOT NULL DEFAULT 0,
    events JSONB NOT NULL DEFAULT '[]',
    dropped_events_count INTEGER NOT NULL DEFAULT 0,
    links JSONB NOT NULL DEFAULT '[]',
    dropped_links_count INTEGER NOT NULL DEFAULT 0,
    status_code SMALLINT NOT NULL DEFAULT 0,
    status_message TEXT NOT NULL DEFAULT '',
    resource JSONB,
    instrumentation_scope JSONB,
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX idx_spans_trace_id ON spans (trace_id);
CREATE INDEX idx_spans_start_time ON spans (start_time_unix_nano);
CREATE INDEX idx_spans_parent_span_id ON spans (parent_span_id);
