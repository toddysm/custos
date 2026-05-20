"""Revision-1 DDL for `MetadataStoreProvider` — runtime/trigger/cursors slice.

This revision owns the tables in the `custos_state` schema covered by
SPL-013 (#127): runtime execution state, Trigger Service state,
connector pull cursors (with the lease-primitive columns), and the
artifact-use backref. Gateway short-lived state (#128) and the audit
outbox tables (#129) live in later revisions.

Append-only contracts are enforced by primary keys: a re-insert of the
same key surfaces as `23505` and the adapter maps that to
`ImmutableViolation`. The connector-cursor lease uses two columns
(`lease_holder`, `lease_expires_at`) and is read under
`SELECT … FOR UPDATE NOWAIT` per § Lease Primitive Abstraction in the
design — the abstract contract is "CAS with fencing token", not "row
lock", so future backends may implement it differently.

REV2 adds idempotency records for API-gateway deduplication (in_progress
→ completed; expired rows reaped by the gateway's maintenance task).

REV3 adds device-code session storage for OIDC device-code flow (pending
→ user_authorized → completed; expired rows reaped).
"""

from __future__ import annotations

from custos_pg.migrations import Revision

METADATA_REV1 = Revision(
    number=1,
    statements=(
        "CREATE SCHEMA IF NOT EXISTS custos_state",
        # ----- Runtime execution state -----
        """
        CREATE TABLE IF NOT EXISTS custos_state.run (
            workspace_id     TEXT        NOT NULL,
            run_id           TEXT        NOT NULL,
            workflow_id      TEXT        NOT NULL,
            workflow_version TEXT        NOT NULL,
            status           TEXT        NOT NULL,
            reason           TEXT,
            started_at       TIMESTAMPTZ NOT NULL,
            updated_at       TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, run_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS run_by_started_at
            ON custos_state.run (workspace_id, started_at DESC, run_id DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS custos_state.step (
            workspace_id TEXT        NOT NULL,
            run_id       TEXT        NOT NULL,
            step_id      TEXT        NOT NULL,
            name         TEXT        NOT NULL,
            status       TEXT        NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, run_id, step_id),
            FOREIGN KEY (workspace_id, run_id)
                REFERENCES custos_state.run (workspace_id, run_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS custos_state.step_attempt (
            workspace_id TEXT        NOT NULL,
            run_id       TEXT        NOT NULL,
            step_id      TEXT        NOT NULL,
            attempt      INTEGER     NOT NULL,
            status       TEXT        NOT NULL,
            started_at   TIMESTAMPTZ NOT NULL,
            finished_at  TIMESTAMPTZ,
            error        JSONB,
            PRIMARY KEY (workspace_id, run_id, step_id, attempt),
            FOREIGN KEY (workspace_id, run_id, step_id)
                REFERENCES custos_state.step (workspace_id, run_id, step_id)
        )
        """,
        # ----- Trigger Service state -----
        """
        CREATE TABLE IF NOT EXISTS custos_state.subscription (
            workspace_id    TEXT        NOT NULL,
            subscription_id TEXT        NOT NULL,
            workflow_id     TEXT        NOT NULL,
            state           TEXT        NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL,
            updated_at      TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, subscription_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS custos_state.subscription_selector (
            workspace_id    TEXT        NOT NULL,
            subscription_id TEXT        NOT NULL,
            seq             BIGSERIAL,
            selector        JSONB       NOT NULL,
            added_at        TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, subscription_id, seq),
            FOREIGN KEY (workspace_id, subscription_id)
                REFERENCES custos_state.subscription (workspace_id, subscription_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS custos_state.resume_subscription (
            workspace_id TEXT        NOT NULL,
            resume_id    TEXT        NOT NULL,
            run_id       TEXT        NOT NULL,
            step_id      TEXT        NOT NULL,
            expires_at   TIMESTAMPTZ NOT NULL,
            payload      JSONB       NOT NULL,
            PRIMARY KEY (workspace_id, resume_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS resume_subscription_by_expires_at
            ON custos_state.resume_subscription (expires_at)
        """,
        """
        CREATE TABLE IF NOT EXISTS custos_state.dedup_key (
            workspace_id TEXT        NOT NULL,
            key          TEXT        NOT NULL,
            expires_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, key)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS dedup_key_by_expires_at
            ON custos_state.dedup_key (expires_at)
        """,
        """
        CREATE TABLE IF NOT EXISTS custos_state.schedule (
            workspace_id TEXT        NOT NULL,
            schedule_id  TEXT        NOT NULL,
            workflow_id  TEXT        NOT NULL,
            cron         TEXT        NOT NULL,
            next_fire_at TIMESTAMPTZ NOT NULL,
            enabled      BOOLEAN     NOT NULL,
            PRIMARY KEY (workspace_id, schedule_id)
        )
        """,
        # ----- Connector pull cursors -----
        # `lease_holder` + `lease_expires_at` together implement the
        # single-writer lease. The row is the source of truth for the
        # CAS-with-fencing-token primitive — adapters read it under
        # `SELECT … FOR UPDATE NOWAIT` so a busy lease shows up as a
        # `55P03` lock-not-available, which the adapter classifies into
        # `LeaseBusy`.
        """
        CREATE TABLE IF NOT EXISTS custos_state.connector_cursor (
            workspace_id     TEXT        NOT NULL,
            instance_id      TEXT        NOT NULL,
            value            TEXT        NOT NULL,
            advanced_at      TIMESTAMPTZ NOT NULL,
            lease_holder     TEXT,
            lease_expires_at TIMESTAMPTZ,
            PRIMARY KEY (workspace_id, instance_id)
        )
        """,
        # ----- Artifact backrefs -----
        # `seq` provides total ordering for keyset pagination and lets
        # the same artifact be referenced by multiple steps in the
        # same run without collision on the primary key.
        """
        CREATE TABLE IF NOT EXISTS custos_state.artifact_use (
            workspace_id TEXT        NOT NULL,
            seq          BIGSERIAL,
            run_id       TEXT        NOT NULL,
            step_id      TEXT        NOT NULL,
            artifact_id  TEXT        NOT NULL,
            name         TEXT        NOT NULL,
            recorded_at  TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, seq)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS artifact_use_by_artifact
            ON custos_state.artifact_use (workspace_id, artifact_id, seq DESC)
        """,
    ),
)


METADATA_REV2 = Revision(
    number=2,
    statements=(
        # ----- Gateway short-lived state: idempotency records -----
        # `status` tracks the state machine: in_progress → completed.
        # Expired rows (expires_at <= now()) are reaped by the gateway's
        # background cleanup job (`delete_expired_idempotency_records`).
        """
        CREATE TABLE IF NOT EXISTS custos_state.idempotency_record (
            workspace_id    TEXT        NOT NULL,
            principal_id    TEXT        NOT NULL,
            route           TEXT        NOT NULL,
            idempotency_key TEXT        NOT NULL,
            request_hash    TEXT        NOT NULL,
            status          TEXT        NOT NULL,
            response_snapshot JSONB,
            reserved_at     TIMESTAMPTZ NOT NULL,
            expires_at      TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, principal_id, route, idempotency_key)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idempotency_record_by_expires_at
            ON custos_state.idempotency_record (expires_at)
        """,
    ),
)


METADATA_REV3 = Revision(
    number=3,
    statements=(
        # ----- Gateway short-lived state: device-code sessions -----
        # Device-code flow: client polls with user_code, backend polls
        # with device_code. Status tracks: pending → user_authorized →
        # completed (on token grant); also expires_at for cleanup.
        # Unique index on (workspace_id, user_code) for polling by user.
        """
        CREATE TABLE IF NOT EXISTS custos_state.device_code_session (
            workspace_id TEXT        NOT NULL,
            device_code  TEXT        NOT NULL,
            user_code    TEXT        NOT NULL,
            issuer_alias TEXT        NOT NULL,
            status       TEXT        NOT NULL,
            token_bundle JSONB,
            created_at   TIMESTAMPTZ NOT NULL,
            expires_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, device_code)
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS device_code_session_by_user_code
            ON custos_state.device_code_session (workspace_id, user_code)
        """,
        """
        CREATE INDEX IF NOT EXISTS device_code_session_by_expires_at
            ON custos_state.device_code_session (expires_at)
        """,
    ),
)


METADATA_REV4 = Revision(
    number=4,
    statements=(
        # ----- Audit writer (event table) -----
        # Append-only audit events written by `append_audit` (potentially
        # within a caller's transaction). No direct update/delete; rows
        # persist forever (observability archive). Primary key enforces
        # write-once per (workspace, event_id) pair.
        """
        CREATE TABLE IF NOT EXISTS custos_state.audit_event (
            workspace_id TEXT        NOT NULL,
            event_id     TEXT        NOT NULL,
            event_type   TEXT        NOT NULL,
            actor        TEXT        NOT NULL,
            subject      JSONB       NOT NULL,
            payload      JSONB       NOT NULL,
            occurred_at  TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id, event_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS audit_event_by_occurred_at
            ON custos_state.audit_event (workspace_id, occurred_at DESC)
        """,
        # ----- Audit outbox (drain buffer) -----
        # Transient: each row read by a drain pipeline and marked as
        # drained via `commit_audit_outbox_cursor`. Auto-incrementing `id`
        # provides total ordering for streaming + pagination. Observability
        # Service (cross-workspace) reads these; no workspace_id scoping
        # here (outbox is global). Expired rows are eventually cleaned up
        # by the drain pipeline's retention policy.
        """
        CREATE TABLE IF NOT EXISTS custos_state.audit_outbox (
            id           BIGSERIAL,
            workspace_id TEXT        NOT NULL,
            event_id     TEXT        NOT NULL,
            event_type   TEXT        NOT NULL,
            payload      JSONB       NOT NULL,
            enqueued_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (id)
        )
        """,
        # ----- Audit outbox drain cursors -----
        # Per-pipeline high-water mark: each pipeline (`audit-store`,
        # `audit-alert`, …) tracks its own cursor so slow consumers don't
        # block fast ones. Implicitly created on first `commit_audit_outbox_cursor`
        # call; tracks the last `id` successfully persisted downstream.
        """
        CREATE TABLE IF NOT EXISTS custos_state.audit_outbox_cursor (
            pipeline_id TEXT    NOT NULL,
            cursor      BIGINT  NOT NULL,
            updated_at  TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (pipeline_id)
        )
        """,
    ),
)


__all__ = ["METADATA_REV1", "METADATA_REV2", "METADATA_REV3", "METADATA_REV4"]
