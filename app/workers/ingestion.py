"""Isolated ingestion runtime; uses only non-owner tenant and coordinator credentials."""

import argparse
import asyncio
import logging
import signal
import socket
from contextlib import AsyncExitStack
from uuid import UUID

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.browser import GuardedBrowserRenderer
from app.adapters.ffmpeg import FFmpegExtractor
from app.adapters.openai_media import OpenAIMediaAdapter
from app.adapters.openai_vector import OpenAIVectorAdapter
from app.adapters.s3 import S3ObjectStore
from app.adapters.web_http import SafeWebFetcher
from app.connectors.wecom.ingestion import WeComIngestionBridge
from app.connectors.wecom.media import HttpWeComMedia
from app.connectors.wecom.tokens import RedisTokenProvider
from app.core.config import Settings, get_settings
from app.core.health import InfrastructureHealthProbe
from app.core.logging import configure_logging
from app.core.runtime import infrastructure
from app.domain.artifacts import ArtifactError
from app.domain.media import MediaLimits
from app.domain.parsing import ParseLimits
from app.ingestion.pipeline import IngestionPipeline
from app.ingestion.queue import IngestionQueue
from app.ingestion.repository import JobRepository
from app.ingestion.worker import IngestionWorker
from app.knowledge.backfill import backfill
from app.knowledge.processor import KnowledgeProcessor
from app.knowledge.recovery import resolve_unknown
from app.parsers.local import LocalFileParser
from app.parsers.media import AudioVideoParser
from app.parsers.web import WebPageParser

logger = logging.getLogger(__name__)


async def run(settings: Settings, args: argparse.Namespace) -> int:
    if not settings.ingestion_enabled or settings.connector_database_url is None:
        raise ArtifactError("ingestion_configuration_missing")
    async with infrastructure(settings) as resources:
        queue = IngestionQueue(
            resources.redis_client,
            settings.wecom_corp_id,
            reclaim_ms=settings.ingestion_lease_seconds * 1000,
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
            repository = JobRepository(
                async_sessionmaker(resources.engine, expire_on_commit=False),
                async_sessionmaker(connector, expire_on_commit=False),
                settings,
                WeComIngestionBridge(settings),
            )
            if args.retry:
                return 0 if await repository.retry(args.retry) else 1
            if getattr(args, "backfill", False):
                count = await backfill(
                    repository.tenant_factory, repository.connector_factory, settings
                )
                logger.info("knowledge_backfill_queued", extra={"count": count})
                return 0
            assert (
                settings.wecom_secret
                and settings.s3_access_key_id
                and settings.s3_secret_access_key
            )
            objects = S3ObjectStore(
                settings.s3_endpoint_url,
                settings.s3_access_key_id.get_secret_value(),
                settings.s3_secret_access_key.get_secret_value(),
                settings.s3_bucket,
                settings.s3_region,
            )
            try:
                async with (
                    AsyncExitStack() as stack,
                    httpx.AsyncClient(follow_redirects=False, trust_env=False, timeout=30) as http,
                ):
                    tokens = RedisTokenProvider(
                        resources.redis_client,
                        http,
                        settings.wecom_corp_id,
                        settings.wecom_secret.get_secret_value(),
                    )
                    if getattr(args, "resolve_unknown", None):
                        assert settings.openai_api_key
                        recovered = await resolve_unknown(
                            repository,
                            OpenAIVectorAdapter(http, settings.openai_api_key.get_secret_value()),
                            args.resolve_unknown,
                            confirmed_remote_absence=args.confirm_remote_absence,
                        )
                        return 0 if recovered else 1
                    media = HttpWeComMedia(http, tokens)
                    knowledge = None
                    if settings.knowledge_enabled:
                        assert settings.openai_api_key
                        knowledge = KnowledgeProcessor(
                            repository,
                            OpenAIVectorAdapter(
                                http,
                                settings.openai_api_key.get_secret_value(),
                                timeout_seconds=settings.knowledge_http_timeout_seconds,
                                max_content_bytes=settings.knowledge_max_document_bytes,
                            ),
                            objects,
                        )
                    parser, webpages = None, None
                    media_parser = None
                    if settings.media_parsing_enabled:
                        assert settings.openai_api_key
                        provider = OpenAIMediaAdapter(
                            http,
                            settings.openai_api_key.get_secret_value(),
                            vision_model=settings.openai_vision_model,
                        )
                        media_limits = MediaLimits(
                            max_input_bytes=settings.ingestion_max_file_bytes,
                            max_text_chars=settings.parser_max_text_chars,
                            total_timeout_seconds=settings.media_timeout_seconds,
                            max_duration_seconds=settings.media_max_duration_seconds,
                            chunk_seconds=settings.media_chunk_seconds,
                            max_frames=settings.media_max_frames,
                            frame_interval_seconds=settings.media_frame_interval_seconds,
                            scene_threshold=settings.media_scene_threshold,
                        )
                        media_parser = AudioVideoParser(
                            FFmpegExtractor(media_limits),
                            provider,
                            provider,
                            media_limits,
                            vision_model=settings.openai_vision_model,
                        )
                    if settings.parsing_enabled:
                        limits = ParseLimits(
                            max_input_bytes=settings.ingestion_max_file_bytes,
                            max_text_chars=settings.parser_max_text_chars,
                            max_pages=settings.parser_max_pages,
                            max_cells=settings.parser_max_cells,
                            timeout_seconds=settings.parser_timeout_seconds,
                        )
                        parser = LocalFileParser(
                            limits,
                            ocr_command=settings.ocr_command,
                            ocr_languages=settings.ocr_languages,
                        )
                        fetcher = SafeWebFetcher()
                        stack.push_async_callback(fetcher.aclose)
                        webpages = WebPageParser(
                            fetcher,
                            GuardedBrowserRenderer(fetcher, settings.browser_ws_url),
                            limits,
                        )
                    worker = IngestionWorker(
                        queue,
                        repository,
                        IngestionPipeline(
                            repository,
                            media,
                            objects,
                            parser=parser,
                            webpages=webpages,
                            media_parser=media_parser,
                        ),
                        knowledge=knowledge,
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
                await objects.aclose()
        finally:
            await connector.dispose()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--health", action="store_true")
    actions.add_argument("--retry", type=UUID, help="Retry a failed dispatch UUID")
    actions.add_argument(
        "--backfill", action="store_true", help="Queue up to 1000 stored sources for indexing"
    )
    actions.add_argument(
        "--resolve-unknown",
        type=UUID,
        help="Recover a verified ambiguous mutation after stopping workers",
    )
    parser.add_argument(
        "--confirm-remote-absence",
        action="store_true",
        help="Operator confirms no in-flight requests or unobserved remote objects",
    )
    args = parser.parse_args()
    configure_logging("INFO")
    try:
        settings = get_settings()
        configure_logging(settings.log_level)
        return asyncio.run(run(settings, args))
    except KeyboardInterrupt:
        return 0
    except Exception:
        logger.error("ingestion_worker_failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
