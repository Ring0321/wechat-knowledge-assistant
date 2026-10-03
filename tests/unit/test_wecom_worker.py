import asyncio
from time import time
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from app.connectors.wecom.contracts import CallbackNotification, WeComError
from app.connectors.wecom.worker import WeComWorker
from app.core.config import Settings


def config() -> Settings:
    return Settings.model_validate(
        {
            "database_url": "postgresql+asyncpg://test:synthetic@localhost/test",
            "redis_url": "redis://localhost/0",
            "wecom_corp_id": "synthetic-corp",
            "wecom_open_kfids": ["synthetic-kf"],
        }
    )


def worker(*, age: int = 0) -> tuple[WeComWorker, AsyncMock, AsyncMock, AsyncMock]:
    queue, sync, replies = AsyncMock(), AsyncMock(), AsyncMock()
    queue.receive.return_value = (
        "1-0",
        CallbackNotification(
            "synthetic-corp",
            "synthetic-kf",
            "synthetic-token",
            int(time()) - age,
        ),
    )
    replies.dispatch_one.return_value = False
    result = WeComWorker(queue, sync, replies, config())
    result.next_poll = float("inf")
    return result, queue, sync, replies


@pytest.mark.parametrize("age,token", [(0, "synthetic-token"), (600, None), (-60, None)])
async def test_old_or_future_notification_uses_cursor_without_token(
    age: int, token: str | None
) -> None:
    runner, queue, sync, _ = worker(age=age)
    await runner.tick()
    sync.sync_account.assert_awaited_once_with("synthetic-kf", token)
    queue.acknowledge.assert_awaited_once_with("1-0")


async def test_database_failure_leaves_notification_pending() -> None:
    runner, queue, sync, replies = worker()
    sync.sync_account.side_effect = SQLAlchemyError("synthetic database outage")
    with pytest.raises(SQLAlchemyError):
        await runner.tick()
    queue.acknowledge.assert_not_awaited()
    replies.dispatch_one.assert_not_awaited()


async def test_recorded_sync_failure_can_ack_and_resume_independent_replies() -> None:
    runner, queue, sync, replies = worker()
    sync.sync_account.side_effect = WeComError("persisted_failure")
    await runner.tick()
    queue.acknowledge.assert_awaited_once()
    replies.dispatch_one.assert_awaited_once()


async def test_foreign_corporate_queue_entry_never_calls_official_api() -> None:
    runner, queue, sync, _ = worker()
    queue.receive.return_value = (
        "1-0",
        CallbackNotification(
            "foreign-corp",
            "synthetic-kf",
            "token",
            int(time()),
        ),
    )
    await runner.tick()
    sync.sync_account.assert_not_awaited()


async def test_clean_stop_does_not_consume_another_notification() -> None:
    runner, queue, _, _ = worker()
    stop = asyncio.Event()
    stop.set()
    await runner.run(stop)
    queue.receive.assert_not_awaited()
