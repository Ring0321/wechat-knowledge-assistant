"""Own one Windows decoder process tree, without a taskkill subprocess.

Only the isolated Python launcher runs before Job Object assignment. It blocks on
stdin until the parent has assigned it to the job; the decoder and its descendants
therefore cannot race assignment. No breakaway or inheritable job handle is used.
This is lifetime management, not an operating-system security sandbox.
"""

import asyncio
import ctypes
import sys
from ctypes import wintypes
from typing import Any

# -I and -S keep the launcher's imports independent of the media directory and
# Python startup hooks. Its stdin is a one-byte control pipe, never decoder input.
LAUNCHER = """
import subprocess
import sys
if sys.stdin.buffer.read(1) != b'G':
    sys.exit(125)
try:
    child = subprocess.Popen(sys.argv[1:], stdin=subprocess.DEVNULL,
                             creationflags=subprocess.CREATE_NO_WINDOW)
except OSError:
    sys.exit(125)
sys.exit(child.wait())
"""
LAUNCH_FAILURE = 125


class _BasicLimits(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit", ctypes.c_longlong),
        ("LimitFlags", wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", wintypes.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", wintypes.DWORD),
        ("SchedulingClass", wintypes.DWORD),
    ]


class _IOCounters(ctypes.Structure):
    _fields_ = [
        (name, ctypes.c_ulonglong)
        for name in (
            "ReadOperationCount",
            "WriteOperationCount",
            "OtherOperationCount",
            "ReadTransferCount",
            "WriteTransferCount",
            "OtherTransferCount",
        )
    ]


class _ExtendedLimits(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimits),
        ("IoInfo", _IOCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


def _kernel() -> Any:
    # Lazy loading keeps imports safe on the production Linux worker.
    if sys.platform != "win32":
        raise OSError("windows_process_unavailable")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel.CreateJobObjectW.restype = wintypes.HANDLE
    kernel.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    kernel.SetInformationJobObject.restype = wintypes.BOOL
    kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    return kernel


def _check(result: object) -> None:
    if not result:
        raise OSError("windows_process_unavailable")


class WindowsJob:
    """A non-inheritable, unnamed kill-on-close job owned by this invocation."""

    def __init__(self) -> None:
        self._kernel = _kernel()
        self._handle: int | None = self._kernel.CreateJobObjectW(None, None)
        _check(self._handle)
        limits = _ExtendedLimits()
        limits.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        try:
            _check(
                self._kernel.SetInformationJobObject(
                    self._handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)
                )
            )
        except BaseException:
            self.close()
            raise

    def assign(self, pid: int) -> None:
        # PROCESS_SET_QUOTA | PROCESS_TERMINATE; no broader process access needed.
        handle = self._kernel.OpenProcess(0x0100 | 0x0001, False, pid)
        _check(handle)
        try:
            _check(self._kernel.AssignProcessToJobObject(self._handle, handle))
        finally:
            _check(self._kernel.CloseHandle(handle))

    def close(self) -> None:
        if self._handle is not None:
            _check(self._kernel.CloseHandle(self._handle))
            self._handle = None

    async def close_after_exit(self, process: asyncio.subprocess.Process) -> None:
        # Process.wait can wait for pipe EOF after the parent exits. Descendants
        # can retain those pipes, so observe the exit code instead, then kill the
        # remaining job members before waiting for EOF/reaping in the caller.
        try:
            while process.returncode is None:  # noqa: ASYNC110 - external process
                await asyncio.sleep(0.01)
        finally:
            self.close()
