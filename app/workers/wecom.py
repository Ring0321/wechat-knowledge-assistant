"""Run with python -m app.workers.wecom; runtime credentials only, never migration DSNs."""

import argparse
import asyncio
import logging
import signal
import socket
from uuid import UUID

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.connectors.wecom.agent import WeComQuestionBridge
from app.connectors.wecom.api import HttpWeComAPI
from app.connectors.wecom.contracts import WeComError
from app.connectors.wecom.ingestion import WeComIngestionBridge
from app.connectors.wecom.queue import RedisNotificationQueue
from app.connectors.wecom.replies import ReplyService
from app.connectors.wecom.store import WeComStore
from app.connectors.wecom.sync import MessageSyncService
from app.connectors.wecom.tokens import RedisTokenProvider
from app.connectors.wecom.worker import WeComWorker
from app.core.config import Settings, get_settings
from app.core.health import InfrastructureHealthProbe
from app.core.logging import configure_logging
from app.core.runtime import infrastructure

logger = logging.getLogger(__name__)


async def run(settings: Settings, args: argparse.Namespace) -> int:
    if not settings.wecom_enabled or settings.connector_database_url is None:
        raise WeComError("worker_configuration_missing")
    async with infrastructure(settings) as resources:
        queue = RedisNotificationQueue(resources.redis_client, settings.wecom_corp_id)
        heartbeat_key = queue.prefix + ":heartbeat:" + socket.gethostname()
        if args.health:
            return 0 if await resources.redis_client.exists(heartbeat_key) else 1
        connector_engine = create_async_engine(
            settings.connector_database_url.get_secret_value(),
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=2,
            hide_parameters=True,
            connect_args={"timeout": settings.health_timeout_seconds},
        )
        try:
            # Reject accidental privileged DSNs or a coordinator allowed to read knowledge.
            await resources.database()
            await resources.redis()
            await InfrastructureHealthProbe(connector_engine, resources.redis_client).database()
            async with connector_engine.connect() as connection:
                if await connection.scalar(
                    text("SELECT has_table_privilege(current_user, 'public.sources', 'SELECT')")
                ):
                    raise WeComError("connector_role_too_privileged")
            store = WeComStore(
                async_sessionmaker(resources.engine, expire_on_commit=False),
                async_sessionmaker(connector_engine, expire_on_commit=False),
                settings.wecom_corp_id,
                admission=(
                    WeComQuestionBridge(settings)
                    if settings.agent_enabled
                    else WeComIngestionBridge(settings)
                    if settings.ingestion_enabled
                    else None
                ),
            )
            if args.retry_account:
                if args.retry_account not in settings.wecom_open_kfids:
                    raise WeComError("unknown_customer_account")
                await store.retry_account(args.retry_account)
                return 0
            if args.retry_outbox:
                return 0 if await store.retry_reply(args.retry_outbox) else 1
            assert settings.wecom_secret is not None
            async with httpx.AsyncClient(
                timeout=settings.wecom_http_timeout_seconds,
                follow_redirects=False,
                trust_env=False,
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            ) as http:
                tokens = RedisTokenProvider(
                    resources.redis_client,
                    http,
                    settings.wecom_corp_id,
                    settings.wecom_secret.get_secret_value(),
                )
                api = HttpWeComAPI(
                    http, tokens, timeout_seconds=settings.wecom_http_timeout_seconds
                )
                worker = WeComWorker(
                    queue,
                    MessageSyncService(api, store, settings),
                    ReplyService(api, store, settings),
                    settings,
                )
                stop = asyncio.Event()
                loop = asyncio.get_running_loop()
                for signum in (signal.SIGINT, signal.SIGTERM):
                    try:
                        loop.add_signal_handler(signum, stop.set)
                    except NotImplementedError:
                        pass  # Windows developer runner uses KeyboardInterrupt instead.

                async def heartbeat() -> None:
                    while not stop.is_set():
                        await resources.redis_client.set(heartbeat_key, "alive", ex=30)
                        try:
                            await asyncio.wait_for(stop.wait(), timeout=10)
                        except TimeoutError:
                            pass

                try:
                    async with asyncio.TaskGroup() as group:
                        group.create_task(worker.run(stop))
                        group.create_task(heartbeat())
                finally:
                    await resources.redis_client.delete(heartbeat_key)
        finally:
            await connector_engine.dispose()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--health", action="store_true")
    actions.add_argument("--retry-account")
    actions.add_argument("--retry-outbox", type=UUID)
    args = parser.parse_args()
    configure_logging("INFO")
    try:
        settings = get_settings()
        configure_logging(settings.log_level)
        return asyncio.run(run(settings, args))
    except KeyboardInterrupt:
        return 0
    except Exception:
        logger.error("wecom_worker_failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
