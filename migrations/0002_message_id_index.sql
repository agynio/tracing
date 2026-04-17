ALTER TABLE spans
    ADD COLUMN message_id TEXT GENERATED ALWAYS AS (
        jsonb_path_query_first(
            COALESCE(resource, '{}'::jsonb),
            '$.attributes[*] ? (@.key == "agyn.thread.message.id").value.stringValue'
        ) #>> '{}'
    ) STORED;

CREATE INDEX idx_spans_message_id ON spans (message_id) WHERE message_id IS NOT NULL;
