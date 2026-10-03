import asyncio
import logging
from typing import Protocol
from uuid import UUID

from app.domain.artifacts import ArtifactError
from app.ingestion.contracts import WorkItem
from app.ingestion.pipeline import IngestionPipeline
from app.ingestion.queue import IngestionQueue
from app.ingestion.repository import JobRepository
from app.knowledge.jobs import DELETE, INDEX

logger = logging.getLogger(__name__)


class KnowledgeProcessorPort(Protocol):
    async def process(self, work: WorkItem) -> None: ...


class IngestionWorker:
    def __init__(
        self,
        queue: IngestionQueue,
        repository: JobRepository,
        pipeline: IngestionPipeline,
        *,
        knowledge: KnowledgeProcessorPort | None = None,
    ) -> None:
        self.queue, self.repository, self.pipeline = queue, repository, pipeline
        self.knowledge = knowledge

    async def once(self) -> bool:
        entry = await self.queue.read_entry()
        if entry is None:
            return False
        entry_id, values = entry
        try:
            dispatch_id = UUID(values["dispatch_id"])
        except (KeyError, ValueError):
            logger.warning("ingestion_invalid_dispatch")
            await self.queue.acknowledge(entry_id)
            return True
        work = await self.repository.claim(dispatch_id)
        if work is not None:
            try:
                async with asyncio.timeout(self.repository.settings.ingestion_job_timeout_seconds):
                    if work.input_data.get("kind") in (INDEX, DELETE):
                        if self.knowledge is None:
                            raise ArtifactError("knowledge_disabled", retryable=True)
                        await self.knowledge.process(work)
                    else:
                        await self.pipeline.process(work)
            except ArtifactError as error:
                await self.repository.fail(work, error)
            except TimeoutError:
                await self.repository.fail(work, ArtifactError("job_timeout", retryable=True))
            except Exception:
                await self.repository.fail(
                    work, ArtifactError("job_processing_failed", retryable=True)
                )
            else:
                logger.info("ingestion_completed")
        await self.queue.acknowledge(entry_id)
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.repository.publish_due(self.queue)
            except Exception:
                # A full stream must keep draining even when no more work can be published.
                logger.warning("ingestion_publish_retry")
            if stop.is_set():
                return
            try:
                await self.once()
            except Exception:
                # No ACK on database/transport failure. Lease and dispatch ledger recover it.
                logger.warning("ingestion_worker_retry")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=2)
                except TimeoutError:
                    pass
