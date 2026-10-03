"""Seed/check synthetic tenants around a real pg_dump/pg_restore in the full gate."""

import asyncio
import hashlib
import os
import sys
from pathlib import Path
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[1]


async def verify(*, prepare: bool) -> None:
    # Test images intentionally use --no-install-project. Direct script execution
    # must resolve the adjacent, copied application rather than an installed wheel.
    sys.path.insert(0, str(ROOT))
    from app.core.health import InfrastructureHealthProbe
    from app.db.models import Source, User
    from app.db.session import tenant_session
    from app.domain.enums import SourceStatus, SourceType

    url = make_url(os.environ["TEST_DATABASE_URL"])
    database = os.environ["TEST_RESTORE_DATABASE"]
    if (
        not url.database
        or not url.database.endswith("_test")
        or not database.startswith("restore_")
        or not database.endswith("_test")
    ):
        raise RuntimeError("disposable_test_database_required")
    if not prepare:
        url = url.set(database=database)
    engine = create_async_engine(url, hide_parameters=True)
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"])
    users = [UUID(os.environ["TEST_RESTORE_USER_A"]), UUID(os.environ["TEST_RESTORE_USER_B"])]
    try:
        await InfrastructureHealthProbe(engine, redis).database()
        factory = async_sessionmaker(engine, expire_on_commit=False)
        for index, user_id in enumerate(users):
            async with tenant_session(factory, user_id) as session:
                body = f"synthetic restore content {index}"
                if prepare:
                    session.add(
                        User(
                            id=user_id,
                            wecom_corp_id="restore-fixture",
                            wecom_external_user_id=str(user_id),
                        )
                    )
                    await session.flush()
                    session.add(
                        Source(
                            user_id=user_id,
                            source_type=SourceType.NOTE,
                            title="restore fixture",
                            text=body,
                            status=SourceStatus.STORED,
                            sha256=hashlib.sha256(body.encode()).hexdigest(),
                        )
                    )
                else:
                    sources = list((await session.scalars(select(Source))).all())
                    assert len(sources) == 1 and sources[0].user_id == user_id
                    assert sources[0].text == body
                    assert sources[0].sha256 == hashlib.sha256(body.encode()).hexdigest()
                    assert await session.get(User, users[1 - index]) is None
        if not prepare:
            async with factory() as session:
                assert (await session.scalars(select(Source))).first() is None
    finally:
        await redis.aclose()
        await engine.dispose()
    print(
        "Restore fixture prepared."
        if prepare
        else "PostgreSQL logical restore: data, hashes, grants and tenant isolation passed."
    )


if __name__ == "__main__":
    if sys.argv[1:] not in (["prepare"], ["check"]):
        raise SystemExit("Expected prepare or check")
    asyncio.run(verify(prepare=sys.argv[1] == "prepare"))
