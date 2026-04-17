ALTER TABLE spans
    ADD COLUMN organization_id TEXT GENERATED ALWAYS AS (
        jsonb_path_query_first(
            COALESCE(resource, '{}'::jsonb),
            '$.attributes[*] ? (@.key == "agyn.organization.id").value.stringValue'
        ) #>> '{}'
    ) STORED;

CREATE INDEX idx_spans_organization_id ON spans (organization_id) WHERE organization_id IS NOT NULL;
