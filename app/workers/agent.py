"""Run background questions with explicit model/key and non-owner database roles."""

import argparse
import asyncio
import logging
import signal
import socket
from uuid import UUID

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.openai_responses import OpenAIResponsesAdapter
from app.adapters.openai_vector import OpenAIVectorAdapter
from app.agent.engine import QuestionAgent
from app.agent.queue import QuestionQueue
from app.agent.repository import QuestionRepository
from app.agent.worker import QuestionWorker
from app.connectors.wecom.agent import WeComQuestionBridge
from app.core.config import Settings, get_settings
from app.core.health import InfrastructureHealthProbe
from app.core.logging import configure_logging
from app.core.runtime import infrastructure
from app.domain.artifacts import ArtifactError
from app.knowledge.service import KnowledgeService

logger = logging.getLogger(__name__)


async def run(settings: Settings, args: argparse.Namespace) -> int:
    if (
        not settings.agent_enabled
        or not settings.knowledge_enabled
        or not settings.openai_api_key
        or settings.connector_database_url is None
    ):
        raise ArtifactError("agent_configuration_missing")
    async with infrastructure(settings) as resources:
        queue = QuestionQueue(
            resources.redis_client,
            settings.wecom_corp_id,
            reclaim_ms=settings.agent_lease_seconds * 1000,
        )
        heartbeat_key = queue.prefix + ":heartbeat:" + socket.gethostname()
        if args.health:
            return 0 if await resources.redis_client.exists(heartbeat_key) else 1
        connector = create_async_engine(
            settings.connector_database_url.get_secret_value(),
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=2,
            hide_parameters=True,
        )
        try:
            await resources.database()
            await resources.redis()
            await InfrastructureHealthProbe(connector, resources.redis_client).database()
            async with connector.connect() as connection:
                if await connection.scalar(
                    text("SELECT has_table_privilege(current_user, 'public.sources', 'SELECT')")
                ):
                    raise ArtifactError("connector_role_too_privileged")
            tenant_factory = async_sessionmaker(resources.engine, expire_on_commit=False)
            repository = QuestionRepository(
                tenant_factory,
                async_sessionmaker(connector, expire_on_commit=False),
                settings,
                WeComQuestionBridge(settings),
            )
            if args.retry:
                return 0 if await repository.retry(args.retry) else 1
            async with httpx.AsyncClient(
                follow_redirects=False, trust_env=False, timeout=30
            ) as http:
                key = settings.openai_api_key.get_secret_value()
                knowledge = KnowledgeService(
                    tenant_factory,
                    settings,
                    OpenAIVectorAdapter(
                        http, key, timeout_seconds=settings.knowledge_http_timeout_seconds
                    ),
                )
                provider = OpenAIResponsesAdapter(
                    http,
                    key,
                    model=settings.openai_agent_model,
                    timeout_seconds=settings.agent_http_timeout_seconds,
                    max_output_tokens=settings.agent_max_output_tokens,
                )
                worker = QuestionWorker(
                    queue, repository, QuestionAgent(settings, knowledge, provider)
                )
                stop = asyncio.Event()
                loop = asyncio.get_running_loop()
                for signum in (signal.SIGINT, signal.SIGTERM):
                    try:
                        loop.add_signal_handler(signum, stop.set)
                    except NotImplementedError:
                        pass

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
            await connector.dispose()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--health", action="store_true")
    actions.add_argument("--retry", type=UUID, help="Retry a failed question dispatch UUID")
    args = parser.parse_args()
    configure_logging("INFO")
    try:
        settings = get_settings()
        configure_logging(settings.log_level)
        return asyncio.run(run(settings, args))
    except KeyboardInterrupt:
        return 0
    except Exception:
        logger.error("agent_worker_failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
