"""Synthetic remote vector service; database, queue and object storage stay real."""

import asyncio
from collections import Counter
from uuid import UUID, uuid4

from pydantic import JsonValue

from app.domain.artifacts import ArtifactError
from app.domain.knowledge import VectorFileStatus, VectorHit, VectorRequestRejected


class FakeVector:
    """Expose remote side effects separately from their possibly lost acknowledgements."""

    def __init__(self) -> None:
        self.namespace = uuid4().hex
        self.calls: Counter[str] = Counter()
        self.stores: dict[UUID, str] = {}
        self.files: dict[str, str] = {}
        self.contents: dict[str, bytes] = {}
        self.statuses: dict[tuple[str, str], VectorFileStatus] = {}
        self.attributes: dict[tuple[str, str], dict[str, JsonValue]] = {}
        self.attach_status: VectorFileStatus = "completed"
        self.uncertain_after: set[str] = set()
        self.hidden: set[str] = set()
        self.failures: Counter[str] = Counter()
        self.rejections: Counter[str] = Counter()
        self.entered: dict[str, asyncio.Event] = {}
        self.release: dict[str, asyncio.Event] = {}
        self.extra_hits: tuple[VectorHit, ...] = ()
        self.search_calls: list[tuple[str, UUID]] = []

    def pause(self, operation: str) -> None:
        self.entered[operation] = asyncio.Event()
        self.release[operation] = asyncio.Event()

    async def _start(self, operation: str) -> None:
        self.calls[operation] += 1
        if self.rejections[operation]:
            self.rejections[operation] -= 1
            raise VectorRequestRejected("synthetic_vector_rejected", retryable=True)
        if self.failures[operation]:
            self.failures[operation] -= 1
            raise ArtifactError("synthetic_vector_unavailable", retryable=True)
        if operation in self.entered:
            self.entered[operation].set()
            await self.release[operation].wait()

    def _acknowledge(self, operation: str) -> None:
        if operation in self.uncertain_after:
            self.uncertain_after.remove(operation)
            raise ArtifactError("synthetic_vector_ack_lost", retryable=True)

    async def find_store(self, user_id: UUID) -> str | None:
        await self._start("find_store")
        return None if "find_store" in self.hidden else self.stores.get(user_id)

    async def create_store(self, user_id: UUID) -> str:
        await self._start("create_store")
        store_id = f"vs_synthetic_{self.namespace}_{self.calls['create_store']}"
        self.stores[user_id] = store_id
        self._acknowledge("create_store")
        return store_id

    async def find_file(self, filename: str) -> str | None:
        await self._start("find_file")
        return None if "find_file" in self.hidden else self.files.get(filename)

    async def upload(self, filename: str, content: bytes) -> str:
        await self._start("upload")
        file_id = f"file-synthetic-{self.namespace}-{self.calls['upload']}"
        self.files[filename], self.contents[file_id] = file_id, content
        self._acknowledge("upload")
        return file_id

    async def file_status(self, store_id: str, file_id: str) -> VectorFileStatus | None:
        await self._start("file_status")
        return self.statuses.get((store_id, file_id))

    async def attach(
        self, store_id: str, file_id: str, attributes: dict[str, JsonValue]
    ) -> VectorFileStatus:
        await self._start("attach")
        assert file_id in self.contents
        self.attributes[store_id, file_id] = dict(attributes)
        self.statuses[store_id, file_id] = self.attach_status
        self._acknowledge("attach")
        return self.attach_status

    async def detach(self, store_id: str, file_id: str) -> None:
        await self._start("detach")
        self.statuses.pop((store_id, file_id), None)
        self.attributes.pop((store_id, file_id), None)
        self._acknowledge("detach")

    async def delete_file(self, file_id: str) -> None:
        await self._start("delete_file")
        self.contents.pop(file_id, None)
        self.files = {name: value for name, value in self.files.items() if value != file_id}
        self._acknowledge("delete_file")

    async def search(
        self, store_id: str, query: str, *, user_id: UUID, limit: int
    ) -> tuple[VectorHit, ...]:
        await self._start("search")
        self.search_calls.append((store_id, user_id))
        return (
            self.extra_hits
            + tuple(
                VectorHit(
                    file_id, 0.9, self.contents[file_id].decode(), self.attributes[store, file_id]
                )
                for (store, file_id), status in self.statuses.items()
                if store == store_id and status == "completed" and file_id in self.contents
            )[:limit]
        )
