"""Read a bounded private status snapshot using only runtime database credentials."""

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Never
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.agent.models import QuestionDispatch, QuestionJob
from app.connectors.wecom.persistence import WeComOutbox, WeComSyncState
from app.core.config import Settings, get_settings
from app.core.health import InfrastructureHealthProbe
from app.core.logging import configure_logging
from app.core.runtime import infrastructure
from app.db.models import IngestionJob, Source, User
from app.db.session import corporate_session, tenant_session
from app.ingestion.models import IngestionDispatch

DEFAULT_LIMIT = 20
MAX_LIMIT = 100
TOTAL_TIMEOUT_SECONDS = 20
STATEMENT_TIMEOUT_MS = 5000
LOCK_TIMEOUT_MS = 1000

# A lexical regex alone would disclose arbitrary lowercase secrets. Unknown legacy
# errors are deliberately replaced, even if they resemble a valid identifier.
_SAFE_ERRORS = frozenset(
    {
        "agent_action_denied",
        "agent_action_expired",
        "agent_evidence_changed",
        "agent_evidence_invalid",
        "agent_lease_expired",
        "agent_lease_lost",
        "agent_processing_failed",
        "agent_reply_invalid",
        "agent_reply_superseded",
        "agent_timeout",
        "agent_turn_budget_exceeded",
        "allowlist_revoked",
        "channel_supplement_not_video",
        "channel_supplement_target_invalid",
        "cursor_did_not_advance",
        "database_sync_failure",
        "identity_conflict",
        "job_lease_expired",
        "job_lease_lost",
        "job_processing_failed",
        "job_timeout",
        "knowledge_document_changed",
        "knowledge_file_conflict",
        "knowledge_index_failed",
        "knowledge_index_timeout",
        "knowledge_source_unavailable",
        "knowledge_store_conflict",
        "knowledge_store_outcome_unknown",
        "knowledge_upload_outcome_unknown",
        "media_parser_unavailable",
        "media_processing_timeout",
        "media_transport_failed",
        "media_truncated",
        "openai_api_error",
        "openai_transport_failed",
        "openai_vector_api_error",
        "openai_vector_transport_failed",
        "openai_responses_api_error",
        "openai_responses_transport_failed",
        "parser_timeout",
        "parser_process_failed",
        "reply_id_mismatch",
        "reply_quota_exhausted",
        "reply_window_closed",
        "s3_delete_failed",
        "s3_list_failed",
        "s3_unavailable",
        "s3_upload_failed",
        "source_unavailable",
        "wecom_connection_failed",
        "wecom_http_error",
        "wecom_invalid_response",
        "wecom_response_too_large",
        "wecom_token_cache_unavailable",
        "wecom_token_http_error",
        "wecom_token_invalid_cache",
        "wecom_token_invalid_response",
        "wecom_token_refresh_busy",
        "wecom_token_refresh_lease_lost",
        "wecom_token_transport_failed",
        "wecom_transport_failed",
        "web_address_blocked",
        "web_timeout",
        "web_unavailable",
    }
)
_DYNAMIC_ERROR = re.compile(r"(?:wecom_(?:-1|[0-9]{1,6})|delivery_failure_[0-9]{1,3})\Z")


class StatusError(RuntimeError):
    """Internal fixed diagnostics; caller never renders raw exception text."""


def safe_error(value: str | None) -> str | None:
    if value is None:
        return None
    if value in _SAFE_ERRORS or _DYNAMIC_ERROR.fullmatch(value):
        return value
    return "unrecognized_error"


def timestamp(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def age_seconds(value: datetime | None, now: datetime) -> int | None:
    return max(0, int((now - value).total_seconds())) if value is not None else None


async def _readonly(session: AsyncSession) -> None:
    # The identity helper has set only transaction-local GUCs before this point.
    # PostgreSQL enforces read-only access even if a later implementation regresses.
    await session.execute(text("SET TRANSACTION READ ONLY"))
    await session.execute(
        text("SELECT set_config('statement_timeout', :timeout, true)"),
        {"timeout": str(STATEMENT_TIMEOUT_MS)},
    )
    await session.execute(
        text("SELECT set_config('lock_timeout', :timeout, true)"),
        {"timeout": str(LOCK_TIMEOUT_MS)},
    )


async def _sync_summary(
    session: AsyncSession, configured: frozenset[str], now: datetime
) -> dict[str, JsonValue]:
    states: dict[str, JsonValue] = {"ready": 0, "retry": 0, "failed": 0}
    for state, count in await session.execute(
        select(WeComSyncState.status, func.count()).group_by(WeComSyncState.status)
    ):
        states[state] = count
    total, missing_sync, oldest = (
        await session.execute(
            select(
                func.count(),
                func.count().filter(WeComSyncState.last_synced_at.is_(None)),
                func.min(WeComSyncState.last_synced_at),
            ).select_from(WeComSyncState)
        )
    ).one()
    configured_present = await session.scalar(
        select(func.count())
        .select_from(WeComSyncState)
        .where(WeComSyncState.open_kfid.in_(configured))
    )
    return {
        "account_count": total,
        "states": states,
        "never_synced_count": missing_sync,
        "oldest_last_sync_age_seconds": age_seconds(oldest, now),
        "configured_account_count": len(configured),
        "missing_configured_account_count": len(configured) - (configured_present or 0),
    }


async def _outbox_summary(session: AsyncSession, now: datetime, limit: int) -> dict[str, JsonValue]:
    states: dict[str, JsonValue] = dict.fromkeys(
        ("queued", "sent", "deferred", "failed", "uncertain"), 0
    )
    for state, count in await session.execute(
        select(WeComOutbox.status, func.count()).group_by(WeComOutbox.status)
    ):
        states[state] = count
    records: list[JsonValue] = []
    # Inspect unresolved items first, without selecting reply content or identities.
    for row in await session.execute(
        select(
            WeComOutbox.id,
            WeComOutbox.status,
            WeComOutbox.attempts,
            WeComOutbox.error_message,
            WeComOutbox.next_attempt_at,
            WeComOutbox.created_at,
        )
        .where(WeComOutbox.status != "sent")
        .order_by(WeComOutbox.created_at, WeComOutbox.id)
        .limit(limit)
    ):
        records.append(
            {
                "outbox_id": str(row.id),
                "status": row.status,
                "attempts": row.attempts,
                "error": safe_error(row.error_message),
                "next_attempt_at": timestamp(row.next_attempt_at),
                "age_seconds": age_seconds(row.created_at, now),
            }
        )
    return {"states": states, "unresolved_records": records, "record_limit": limit}


async def _dispatch_summary(
    session: AsyncSession,
    model: type[IngestionDispatch] | type[QuestionDispatch],
    now: datetime,
    limit: int,
) -> dict[str, JsonValue]:
    unfinished, due, oldest, oldest_due = (
        await session.execute(
            select(
                func.count(),
                func.count().filter(model.available_at <= now),
                func.min(model.created_at),
                func.min(model.available_at).filter(model.available_at <= now),
            )
            .select_from(model)
            .where(model.finished_at.is_(None))
        )
    ).one()
    records: list[JsonValue] = []
    for row in await session.execute(
        select(
            model.id, model.created_at, model.available_at, model.published_at, model.finished_at
        )
        .where(model.finished_at.is_(None))
        .order_by(model.available_at, model.id)
        .limit(limit)
    ):
        records.append(
            {
                "dispatch_id": str(row.id),
                "age_seconds": age_seconds(row.created_at, now),
                "available_at": timestamp(row.available_at),
                "published_at": timestamp(row.published_at),
                "finished_at": timestamp(row.finished_at),
            }
        )
    return {
        "unfinished_count": unfinished,
        "due_count": due,
        "oldest_unfinished_age_seconds": age_seconds(oldest, now),
        "oldest_due_age_seconds": age_seconds(oldest_due, now),
        "unfinished_records": records,
        "record_limit": limit,
    }


async def _job_status(
    factory: async_sessionmaker[AsyncSession],
    corp_id: str,
    kind: str,
    user_id: UUID,
    job_id: UUID,
    now: datetime,
) -> dict[str, JsonValue]:
    async with tenant_session(factory, user_id) as session:
        await _readonly(session)
        # The corporate dispatch supplies the UUID; recheck its database identity
        # in the tenant transaction rather than accepting a CLI-provided tenant.
        if not await session.scalar(
            select(User.id).where(User.id == user_id, User.wecom_corp_id == corp_id)
        ):
            raise StatusError("dispatch_unavailable")
        model = IngestionJob if kind == "ingestion" else QuestionJob
        row = (
            await session.execute(
                select(
                    model.status,
                    model.attempts,
                    model.max_attempts,
                    model.lease_expires_at,
                    model.next_retry_at,
                    model.error_message,
                ).where(model.id == job_id, model.user_id == user_id)
            )
        ).one_or_none()
        if row is None:
            raise StatusError("dispatch_unavailable")
        result: dict[str, JsonValue] = {
            "kind": kind,
            "status": str(row.status),
            "attempts": row.attempts,
            "max_attempts": row.max_attempts,
            "lease_expires_at": timestamp(row.lease_expires_at),
            "lease_expired": row.lease_expires_at <= now if row.lease_expires_at else None,
            "next_retry_at": timestamp(row.next_retry_at),
            "error": safe_error(row.error_message),
        }
        if kind == "ingestion":
            source_status = await session.scalar(
                select(Source.status)
                .join(
                    IngestionJob,
                    (
                        (IngestionJob.source_id == Source.id)
                        & (IngestionJob.user_id == Source.user_id)
                    ),
                )
                .where(IngestionJob.id == job_id, IngestionJob.user_id == user_id)
            )
            result["source_status"] = str(source_status) if source_status is not None else None
        return result


async def snapshot(
    tenant_factory: async_sessionmaker[AsyncSession],
    connector_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    limit: int = DEFAULT_LIMIT,
    dispatch_id: UUID | None = None,
) -> dict[str, JsonValue]:
    if not settings.wecom_corp_id or type(limit) is not int or not 1 <= limit <= MAX_LIMIT:
        raise StatusError("invalid_configuration")
    if dispatch_id is not None and not isinstance(dispatch_id, UUID):
        raise StatusError("invalid_arguments")
    now = datetime.now(UTC)
    lookup: tuple[str, UUID, UUID] | None = None
    dispatch_record: dict[str, JsonValue] | None = None
    async with corporate_session(connector_factory, settings.wecom_corp_id) as session:
        await _readonly(session)
        result: dict[str, JsonValue] = {
            "snapshot_at": timestamp(now),
            "business_health": "not_assessed",
            "sync": await _sync_summary(session, settings.wecom_open_kfids, now),
            "outbox": await _outbox_summary(session, now, limit),
            "ingestion": await _dispatch_summary(session, IngestionDispatch, now, limit),
            "questions": await _dispatch_summary(session, QuestionDispatch, now, limit),
        }
        if dispatch_id is not None:
            for kind, model in (("ingestion", IngestionDispatch), ("question", QuestionDispatch)):
                row = (
                    await session.execute(
                        select(
                            model.user_id,
                            model.job_id,
                            model.available_at,
                            model.published_at,
                            model.finished_at,
                        ).where(model.id == dispatch_id, model.corp_id == settings.wecom_corp_id)
                    )
                ).one_or_none()
                if row is not None:
                    if lookup is not None:
                        raise StatusError("dispatch_unavailable")
                    lookup = kind, row.user_id, row.job_id
                    dispatch_record = {
                        "dispatch_id": str(dispatch_id),
                        "available_at": timestamp(row.available_at),
                        "published_at": timestamp(row.published_at),
                        "finished_at": timestamp(row.finished_at),
                    }
            if lookup is None:
                raise StatusError("dispatch_unavailable")
    if lookup is not None and dispatch_record is not None:
        dispatch_record["job"] = await _job_status(
            tenant_factory, settings.wecom_corp_id, *lookup, now
        )
        result["dispatch"] = dispatch_record
    return result


async def run(
    settings: Settings, *, limit: int = DEFAULT_LIMIT, dispatch_id: UUID | None = None
) -> dict[str, JsonValue]:
    if settings.connector_database_url is None or not settings.wecom_corp_id:
        raise StatusError("invalid_configuration")
    async with asyncio.timeout(TOTAL_TIMEOUT_SECONDS), infrastructure(settings) as resources:
        connector = create_async_engine(
            settings.connector_database_url.get_secret_value(),
            pool_pre_ping=True,
            pool_size=1,
            max_overflow=0,
            pool_timeout=settings.health_timeout_seconds,
            hide_parameters=True,
            connect_args={"timeout": settings.health_timeout_seconds},
        )
        try:
            await resources.database()
            await InfrastructureHealthProbe(connector, resources.redis_client).database()
            async with connector.connect() as connection:
                if await connection.scalar(
                    text(
                        "SELECT has_table_privilege(current_user, 'public.sources', 'SELECT') "
                        "OR has_any_column_privilege(current_user, 'public.sources', 'SELECT')"
                    )
                ):
                    raise StatusError("unsafe_database_role")
            return await snapshot(
                async_sessionmaker(resources.engine, expire_on_commit=False),
                async_sessionmaker(connector, expire_on_commit=False),
                settings,
                limit=limit,
                dispatch_id=dispatch_id,
            )
        finally:
            await connector.dispose()


class _SafeParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise StatusError("invalid_arguments")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _SafeParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--dispatch", type=UUID, help="Diagnose one current-corporation dispatch")
    configure_logging("WARNING")
    try:
        args = parser.parse_args(argv)
        if not 1 <= args.limit <= MAX_LIMIT:
            raise StatusError("invalid_arguments")
        result = asyncio.run(run(get_settings(), limit=args.limit, dispatch_id=args.dispatch))
    except StatusError as error:
        code = str(error)
        if code not in {
            "invalid_arguments",
            "invalid_configuration",
            "dispatch_unavailable",
            "unsafe_database_role",
        }:
            code = "status_unavailable"
        print(json.dumps({"error": code}), file=sys.stderr)
        return 2 if code == "invalid_arguments" else 1
    except (Exception, KeyboardInterrupt):
        print(json.dumps({"error": "status_unavailable"}), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
