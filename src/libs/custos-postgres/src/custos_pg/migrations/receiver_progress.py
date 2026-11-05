"""Additive metadata revision16 for receiver discovery and fenced fair scan continuations."""

from custos_pg.migrations import Revision

RECEIVER_WORKSPACE_TABLES = (
    "subscription",
    "resume_subscription",
    "schedule",
    "pending_schedule_fire",
    "dead_letter",
    "bulk_replay_command",
    "source_delivery_selection",
    "delivery_receipt",
    "connector_pull_control",
    "connector_cursor",
)

METADATA_REV16 = Revision(
    number=16,
    statements=(
        *(
            f"CREATE INDEX IF NOT EXISTS receiver_workspace_{table} "
            f'ON custos_state.{table}(workspace_id COLLATE "C")'
            for table in RECEIVER_WORKSPACE_TABLES
        ),
        "CREATE SEQUENCE IF NOT EXISTS custos_state.receiver_service_ticket AS BIGINT NO CYCLE",
        """
        CREATE TABLE IF NOT EXISTS custos_state.receiver_discovery (
            deployment_id TEXT PRIMARY KEY,
            incarnation TEXT NOT NULL,
            generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
            revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
            cursor TEXT,
            sweep BIGINT NOT NULL DEFAULT 1 CHECK (sweep > 0),
            completed_sweep BIGINT NOT NULL DEFAULT 0 CHECK (completed_sweep >= 0),
            holder TEXT,
            token TEXT,
            expires_at TIMESTAMPTZ,
            CHECK (completed_sweep < sweep),
            CHECK ((holder IS NULL AND token IS NULL AND expires_at IS NULL)
                OR (holder IS NOT NULL AND token IS NOT NULL AND expires_at IS NOT NULL))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS custos_state.receiver_progress (
            deployment_id TEXT NOT NULL REFERENCES custos_state.receiver_discovery(deployment_id),
            workspace_id TEXT NOT NULL,
            operation TEXT NOT NULL CHECK (operation IN
                ('scheduled','delivery','pull-demand','retention')),
            incarnation TEXT NOT NULL,
            generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
            revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
            continuation TEXT NOT NULL CHECK (
                octet_length(continuation) <= 65536
                AND jsonb_typeof(continuation::jsonb) = 'object'
                AND continuation::jsonb ?& ARRAY['version','operation','cursor','dueCutoff']
                AND continuation::jsonb - 'version' - 'operation' - 'cursor' - 'dueCutoff'
                    = '{}'::jsonb
                AND continuation::jsonb->'version' = '1'::jsonb
                AND continuation::jsonb->>'operation' = operation
            ),
            service_ticket BIGINT NOT NULL DEFAULT
                nextval('custos_state.receiver_service_ticket'),
            next_eligible_at TIMESTAMPTZ NOT NULL,
            holder TEXT,
            token TEXT,
            expires_at TIMESTAMPTZ,
            last_seen_sweep BIGINT NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (deployment_id, workspace_id, operation),
            CHECK ((holder IS NULL AND token IS NULL AND expires_at IS NULL)
                OR (holder IS NOT NULL AND token IS NOT NULL AND expires_at IS NOT NULL))
        )
        """,
        "CREATE INDEX IF NOT EXISTS receiver_progress_service_order "
        "ON custos_state.receiver_progress(deployment_id,operation,service_ticket) "
        "INCLUDE(next_eligible_at,expires_at)",
        "CREATE INDEX IF NOT EXISTS receiver_progress_retention "
        "ON custos_state.receiver_progress(deployment_id,updated_at,workspace_id,operation) "
        "WHERE continuation::jsonb->>'cursor' IS NULL",
    ),
)
