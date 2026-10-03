import asyncio
import logging
from uuid import UUID

from app.agent.engine import QuestionAgent
from app.agent.queue import QuestionQueue
from app.agent.repository import QuestionRepository
from app.domain.artifacts import ArtifactError

logger = logging.getLogger(__name__)


class QuestionWorker:
    def __init__(
        self, queue: QuestionQueue, repository: QuestionRepository, agent: QuestionAgent
    ) -> None:
        self.queue, self.repository, self.agent = queue, repository, agent

    async def once(self) -> bool:
        entry = await self.queue.read_entry()
        if entry is None:
            return False
        entry_id, values = entry
        try:
            dispatch_id = UUID(values["dispatch_id"])
        except (KeyError, ValueError):
            logger.warning("agent_invalid_dispatch")
            await self.queue.acknowledge(entry_id)
            return True
        work = await self.repository.claim(dispatch_id)
        if work is not None:
            try:
                async with asyncio.timeout(self.repository.settings.agent_job_timeout_seconds):
                    if not await self.repository.confirmation(work):
                        outcome = await self.agent.answer(work)
                        await self.repository.complete(work, outcome)
            except ArtifactError as error:
                await self.repository.fail(work, error)
            except TimeoutError:
                await self.repository.fail(work, ArtifactError("agent_timeout", retryable=True))
            except Exception:
                await self.repository.fail(
                    work, ArtifactError("agent_processing_failed", retryable=True)
                )
            else:
                logger.info("agent_completed")
        await self.queue.acknowledge(entry_id)
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.repository.publish_due(self.queue)
            except Exception:
                logger.warning("agent_publish_retry")
            if stop.is_set():
                return
            try:
                await self.once()
            except Exception:
                logger.warning("agent_worker_retry")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=2)
                except TimeoutError:
                    pass
