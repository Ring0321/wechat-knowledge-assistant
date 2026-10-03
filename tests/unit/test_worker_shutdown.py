"""A cooperative stop finishes current work but never starts another queued task."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from app.agent.worker import QuestionWorker
from app.connectors.wecom.worker import WeComWorker
from app.core.config import Settings
from app.ingestion.worker import IngestionWorker


@pytest.mark.parametrize("kind", ["ingestion", "agent", "wecom"])
async def test_stop_during_current_work_drains_one_then_exits(kind: str) -> None:
    stop, entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    repository = Mock(publish_due=AsyncMock())
    worker = (
        IngestionWorker(Mock(), repository, Mock())
        if kind == "ingestion"
        else QuestionWorker(Mock(), repository, Mock())
        if kind == "agent"
        else WeComWorker(Mock(), Mock(), Mock(), Mock(spec=Settings))
    )

    async def current() -> bool:
        entered.set()
        await release.wait()
        return True

    action = AsyncMock(side_effect=current)
    setattr(worker, "tick" if kind == "wecom" else "once", action)
    task = asyncio.create_task(worker.run(stop))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        stop.set()
        assert not task.done()
    finally:
        release.set()
        await asyncio.wait_for(task, timeout=1)
    action.assert_awaited_once()


@pytest.mark.parametrize("kind", ["ingestion", "agent"])
async def test_stop_during_publish_does_not_claim_new_work(kind: str) -> None:
    stop, entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def publish(*args: object) -> int:
        entered.set()
        await release.wait()
        return 1

    repository = Mock(publish_due=AsyncMock(side_effect=publish))
    worker = (
        IngestionWorker(Mock(), repository, Mock())
        if kind == "ingestion"
        else QuestionWorker(Mock(), repository, Mock())
    )
    action = AsyncMock(return_value=True)
    worker.once = action
    task = asyncio.create_task(worker.run(stop))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        stop.set()
    finally:
        release.set()
        await asyncio.wait_for(task, timeout=1)
    action.assert_not_awaited()
