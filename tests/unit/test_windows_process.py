"""Exercise actual Windows job ownership with disposable synthetic processes."""

import asyncio
import ctypes
import gc
import json
import subprocess
import sys
from collections.abc import AsyncIterator
from ctypes import wintypes
from pathlib import Path
from typing import Any

import pytest

from app.adapters import ffmpeg, windows_process
from app.adapters.ffmpeg import FFmpegExtractor
from app.adapters.windows_process import WindowsJob
from app.domain.artifacts import ArtifactError
from app.domain.media import MediaLimits

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Windows Job Object API")


@pytest.fixture
async def jobs(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[list[WindowsJob]]:
    owned: list[WindowsJob] = []

    class TrackedJob(WindowsJob):
        def __init__(self) -> None:
            super().__init__()
            owned.append(self)

    monkeypatch.setattr(ffmpeg, "WindowsJob", TrackedJob)
    yield owned
    for job in owned:
        job.close()


def _handle_count() -> int:
    kernel = windows_process._kernel()
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.GetProcessHandleCount.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetProcessHandleCount.restype = wintypes.BOOL
    count = wintypes.DWORD()
    assert kernel.GetProcessHandleCount(kernel.GetCurrentProcess(), ctypes.byref(count))
    return count.value


def _alive(pid: int) -> bool:
    kernel = windows_process._kernel()
    kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetExitCodeProcess.restype = wintypes.BOOL
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        return False
    try:
        code = wintypes.DWORD()
        return bool(kernel.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259
    finally:
        assert kernel.CloseHandle(handle)


def _in_job(pid: int, job: WindowsJob) -> bool:
    kernel = windows_process._kernel()
    kernel.IsProcessInJob.argtypes = [
        wintypes.HANDLE,
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.BOOL),
    ]
    kernel.IsProcessInJob.restype = wintypes.BOOL
    handle = kernel.OpenProcess(0x1000, False, pid)
    assert handle
    try:
        result = wintypes.BOOL()
        assert kernel.IsProcessInJob(handle, job._handle, ctypes.byref(result))
        return bool(result.value)
    finally:
        assert kernel.CloseHandle(handle)


async def _ready(directory: Path) -> list[int]:
    path = directory / "ready.json"
    async with asyncio.timeout(5):
        while not path.exists():  # noqa: ASYNC110 - external synthetic process
            await asyncio.sleep(0.01)
    return list(json.loads(path.read_text()))


def _run(directory: Path, *, exit_parent: bool = False) -> asyncio.Task[tuple[bytes, bytes]]:
    script = (
        "import json,os,pathlib,subprocess,sys,time;"
        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(40)']);"
        "pathlib.Path('ready.tmp').write_text(json.dumps([os.getpid(),p.pid]));"
        "pathlib.Path('ready.tmp').replace('ready.json');"
        + ("sys.exit(0)" if exit_parent else "time.sleep(40)")
    )
    return asyncio.create_task(
        FFmpegExtractor(MediaLimits())._run(
            [sys.executable, "-c", script], directory, output_limit=100
        )
    )


async def _stop(task: asyncio.Task[Any], jobs: list[WindowsJob]) -> None:
    # Native ownership also makes failed-test cleanup independent of PID reuse.
    for job in jobs:
        job.close()
    task.cancel()
    await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)


@pytest.mark.parametrize("taskkill_behavior", ["slow", "failed"])
async def test_job_cancellation_is_fast_without_taskkill(
    tmp_path: Path,
    jobs: list[WindowsJob],
    monkeypatch: pytest.MonkeyPatch,
    taskkill_behavior: str,
) -> None:
    original = asyncio.create_subprocess_exec
    calls: list[str] = []

    async def record(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        if Path(args[0]).name.lower() == "taskkill.exe":
            calls.append("taskkill")
            if taskkill_behavior == "failed":
                raise OSError("helper unavailable")
            await asyncio.sleep(60)
        assert args[1:4] == ("-I", "-S", "-c")
        assert kwargs["creationflags"] & subprocess.CREATE_NO_WINDOW
        return await original(*args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", record)
    task = _run(tmp_path)
    pids: list[int] = []
    try:
        pids = await _ready(tmp_path)
        assert len(jobs) == 1
        assert all(_alive(pid) and _in_job(pid, jobs[0]) for pid in pids)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert not any(_alive(pid) for pid in pids)
        assert jobs[0]._handle is None
        assert not calls
    finally:
        await _stop(task, jobs)


async def test_parent_exit_closes_job_even_when_descendant_holds_pipes(
    tmp_path: Path, jobs: list[WindowsJob]
) -> None:
    task = _run(tmp_path, exit_parent=True)
    pids: list[int] = []
    try:
        pids = await _ready(tmp_path)
        assert await asyncio.wait_for(task, 5) == (b"", b"")
        assert not any(_alive(pid) for pid in pids)
        assert jobs[0]._handle is None
    finally:
        await _stop(task, jobs)


async def test_spawn_handshake_and_repeated_cancellation_do_not_leak(
    tmp_path: Path, jobs: list[WindowsJob], monkeypatch: pytest.MonkeyPatch
) -> None:
    original = asyncio.create_subprocess_exec
    entered, release = asyncio.Event(), asyncio.Event()
    processes: list[asyncio.subprocess.Process] = []

    async def delayed(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        process = await original(*args, **kwargs)
        processes.append(process)
        entered.set()
        await release.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed)
    task = _run(tmp_path)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await asyncio.sleep(0.1)
        assert not (tmp_path / "ready.json").exists()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert jobs[0]._handle is None
        assert all(process.returncode is not None for process in processes)
        if (tmp_path / "ready.json").exists():
            assert not any(_alive(pid) for pid in await _ready(tmp_path))
    finally:
        release.set()
        await _stop(task, jobs)


async def test_repeated_cancellation_waits_for_owned_cleanup(
    tmp_path: Path, jobs: list[WindowsJob], monkeypatch: pytest.MonkeyPatch
) -> None:
    original = ffmpeg._terminate
    entered, release = asyncio.Event(), asyncio.Event()

    async def delayed(process: asyncio.subprocess.Process, job: WindowsJob | None = None) -> None:
        entered.set()
        await release.wait()
        await original(process, job)

    monkeypatch.setattr(ffmpeg, "_terminate", delayed)
    task = _run(tmp_path)
    pids: list[int] = []
    try:
        pids = await _ready(tmp_path)
        task.cancel()
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert jobs[0]._handle is None
        assert not any(_alive(pid) for pid in pids)
    finally:
        release.set()
        await _stop(task, jobs)


@pytest.mark.parametrize("failure", ["create_process", "assignment", "decoder_start"])
async def test_failed_startup_closes_job_and_reaps_launcher(
    tmp_path: Path, jobs: list[WindowsJob], monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    original = asyncio.create_subprocess_exec
    processes: list[asyncio.subprocess.Process] = []

    async def record(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        if failure == "create_process":
            raise OSError("synthetic spawn failure")
        process = await original(*args, **kwargs)
        processes.append(process)
        return process

    def reject(*args: Any) -> int:
        assert not (tmp_path / "ready.json").exists()
        return 0

    monkeypatch.setattr(asyncio, "create_subprocess_exec", record)
    if failure == "assignment":
        kernel = windows_process._kernel()
        monkeypatch.setattr(kernel, "AssignProcessToJobObject", reject)
        monkeypatch.setattr(windows_process, "_kernel", lambda: kernel)
    if failure == "decoder_start":
        monkeypatch.setattr(ffmpeg, "_executable", lambda command: str(tmp_path / "missing.exe"))
    # Repeating each failure catches accumulating native job/process handles.
    baseline = _handle_count()
    for _ in range(5):
        with pytest.raises(ArtifactError) as error:
            await _run(tmp_path)
        assert error.value.code == "ffmpeg_unavailable" and error.value.retryable
        assert all(job._handle is None for job in jobs)
        assert all(process.returncode is not None for process in processes)
        assert not (tmp_path / "ready.json").exists()
    processes.clear()
    gc.collect()
    await asyncio.sleep(0)
    assert _handle_count() <= baseline + 2


def test_job_configuration_failure_closes_native_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    kernel = windows_process._kernel()
    monkeypatch.setattr(kernel, "SetInformationJobObject", lambda *args: 0)
    monkeypatch.setattr(windows_process, "_kernel", lambda: kernel)
    baseline = _handle_count()
    for _ in range(10):
        with pytest.raises(OSError, match="windows_process_unavailable"):
            WindowsJob()
    assert _handle_count() == baseline


async def test_cancelled_spawn_failure_keeps_cancellation_and_closes_job(
    tmp_path: Path, jobs: list[WindowsJob], monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    async def failed(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        entered.set()
        await release.wait()
        raise OSError("synthetic delayed spawn failure")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", failed)
    task = _run(tmp_path)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert jobs[0]._handle is None
    finally:
        release.set()
        await _stop(task, jobs)
