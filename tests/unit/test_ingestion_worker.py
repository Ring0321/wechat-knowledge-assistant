import asyncio
from unittest.mock import AsyncMock, Mock

from app.core.streams import StreamError
from app.ingestion.worker import IngestionWorker


async def test_publisher_failure_does_not_starve_existing_entries() -> None:
    stop = asyncio.Event()
    repository = Mock()
    repository.publish_due = AsyncMock(side_effect=StreamError("queue_unavailable"))
    worker = IngestionWorker(Mock(), repository, Mock())

    async def consume() -> bool:
        stop.set()
        return True

    worker.once = AsyncMock(side_effect=consume)
    await asyncio.wait_for(worker.run(stop), timeout=0.5)
    worker.once.assert_awaited_once()
    repository.publish_due.assert_awaited_once()
