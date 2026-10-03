"""Bounded local media decoding; this adapter is not an operating-system sandbox.

Use a patched FFmpeg in a restricted worker/container. Protocol/demuxer allowlists,
disabled MOV external tracks, clean environment and byte/time/thread/pixel limits
reduce exposure but cannot contain a decoder exploit or impose a hard RSS quota.
The caller owns the temporary directory and returned derivatives. No input, decoder
diagnostics or provider credentials are logged or included in public exceptions.
"""

import asyncio
import json
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import wave
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from app.adapters.windows_process import LAUNCH_FAILURE, LAUNCHER, WindowsJob
from app.domain.artifacts import ArtifactError
from app.domain.media import AudioChunk, ExtractedMedia, MediaInfo, MediaLimits, VideoFrame

_DEMUXERS = "mov,matroska,webm,avi,wav,mp3,aac,amr,ogg,flac"
_FORMAT_NAMES = frozenset(_DEMUXERS.split(",")) | {
    "mp4",
    "m4a",
    "3gp",
    "3g2",
    "mj2",
}
_SAMPLE_RATE = 16_000
_PCM_BYTES_PER_SECOND = _SAMPLE_RATE * 2
_LOG_LIMIT = 256 * 1024
_PROBE_LIMIT = 128 * 1024
_JPEG_LIMIT = 4 * 1024 * 1024
_PTS = re.compile(rb"\[Parsed_showinfo_\d+[^\r\n]*?\bn:\s*\d+\s+pts:\s*\S+\s+pts_time:(\S+)")


def _number(value: object, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ArtifactError("media_invalid_metadata")
    try:
        result = float(value)
    except (ValueError, OverflowError):
        raise ArtifactError("media_invalid_metadata") from None
    if not math.isfinite(result) or (result <= 0 if positive else result < 0):
        raise ArtifactError("media_invalid_metadata")
    return result


def _integer(value: object) -> int:
    result = _number(value, positive=True)
    if not result.is_integer():
        raise ArtifactError("media_invalid_metadata")
    return int(result)


def _parse_info(payload: bytes, limits: MediaLimits) -> MediaInfo:
    try:
        data = json.loads(payload)
    except (ValueError, UnicodeError, RecursionError):
        raise ArtifactError("media_invalid_metadata") from None
    if not isinstance(data, dict) or not isinstance(data.get("format"), dict):
        raise ArtifactError("media_invalid_metadata")
    container = data["format"]
    name = container.get("format_name")
    if not isinstance(name, str) or not name or not set(name.split(",")) <= _FORMAT_NAMES:
        raise ArtifactError("media_unsupported")
    duration = _number(container.get("duration"), positive=True)
    if duration > limits.max_duration_seconds:
        raise ArtifactError("media_duration_limit")
    streams = data.get("streams")
    if not isinstance(streams, list) or not streams or len(streams) > 32:
        raise ArtifactError("media_invalid_metadata")
    has_audio, has_video, width, height = False, False, 0, 0
    for stream in streams:
        if not isinstance(stream, dict):
            raise ArtifactError("media_invalid_metadata")
        kind = stream.get("codec_type")
        if not isinstance(kind, str) or kind not in {
            "audio",
            "video",
            "subtitle",
            "data",
            "attachment",
        }:
            raise ArtifactError("media_unsupported")
        if kind not in {"audio", "video"}:
            continue
        codec = stream.get("codec_name")
        if not isinstance(codec, str) or not codec or codec == "unknown":
            raise ArtifactError("media_unsupported")
        if "duration" in stream:
            stream_duration = _number(stream["duration"], positive=True)
            if stream_duration > limits.max_duration_seconds:
                raise ArtifactError("media_duration_limit")
        if kind == "audio":
            if (
                _integer(stream.get("sample_rate")) > 384_000
                or _integer(stream.get("channels")) > 32
            ):
                raise ArtifactError("media_audio_limit")
            has_audio = True
        else:
            current_width = _integer(stream.get("width"))
            current_height = _integer(stream.get("height"))
            if current_width * current_height > limits.max_pixels:
                raise ArtifactError("media_pixel_limit")
            # An album cover is not a temporal video stream. Map V:0 consistently.
            disposition = stream.get("disposition", {})
            if not isinstance(disposition, dict):
                raise ArtifactError("media_invalid_metadata")
            attached = disposition.get("attached_pic", 0)
            if type(attached) is not int or attached not in (0, 1):
                raise ArtifactError("media_invalid_metadata")
            if attached:
                continue
            if not has_video:
                width, height = current_width, current_height
            has_video = True
    if not has_audio and not has_video:
        raise ArtifactError("media_unsupported")
    return MediaInfo(duration, has_audio, has_video, width, height)


def _periodic_times(duration: float, limits: MediaLimits) -> list[float]:
    count = min(limits.max_frames, max(1, math.ceil(duration / limits.frame_interval_seconds)))
    if count == 1:
        return [0.0]
    last = (math.ceil(duration / limits.frame_interval_seconds) - 1) * limits.frame_interval_seconds
    # Redistribute when capped so a long recording is not represented only by its start.
    return [index * last / (count - 1) for index in range(count)]


def _select_times(duration: float, scenes: Sequence[float], limits: MediaLimits) -> list[float]:
    periodic = _periodic_times(duration, limits)
    # Reserve roughly half the capacity for periodic coverage, including both endpoints.
    coverage = min(len(periodic), max(1, (limits.max_frames + 1) // 2))
    anchors = (
        [periodic[round(index * (len(periodic) - 1) / (coverage - 1))] for index in range(coverage)]
        if coverage > 1
        else [0.0]
    )
    chosen: list[float] = []

    def add(candidate: float) -> None:
        if (
            len(chosen) < limits.max_frames
            and math.isfinite(candidate)
            and 0 <= candidate < duration
            and all(abs(candidate - item) >= limits.min_frame_gap_seconds for item in chosen)
        ):
            chosen.append(candidate)

    for value in anchors:
        add(value)
    # Farthest-point selection preserves temporal spread even with many early scenes.
    remaining = sorted(set(scenes))
    while remaining and len(chosen) < limits.max_frames:
        value = max(remaining, key=lambda item: min(abs(item - picked) for picked in chosen))
        remaining.remove(value)
        add(value)
    for value in periodic:
        add(value)
    return sorted(chosen)


def _timestamps(diagnostics: bytes, duration: float, maximum: int) -> list[float]:
    matches = _PTS.findall(diagnostics)
    if len(matches) > maximum:
        raise ArtifactError("media_frame_limit")
    values: list[float] = []
    for raw in matches:
        try:
            value = _number(raw.decode("ascii"))
        except UnicodeError:
            raise ArtifactError("media_invalid_metadata") from None
        if value > duration + 0.001 or (values and value < values[-1]):
            raise ArtifactError("media_invalid_metadata")
        values.append(min(value, duration))
    return values


def _safe_path(path: Path, *, directory: bool = False) -> Path:
    absolute = path.absolute()
    if str(absolute).startswith(("\\\\", "//")):
        raise ArtifactError("media_unsupported_path")
    try:
        for component in (absolute, *absolute.parents):
            if component.is_symlink() or component.is_junction():
                raise ArtifactError("media_unsupported_path")
        info = absolute.stat()
        if not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
            raise ArtifactError("media_unreadable")
        return absolute.resolve(strict=True)
    except OSError:
        raise ArtifactError("media_unreadable") from None


def _environment(directory: Path) -> dict[str, str]:
    environment = {"LANG": "C", "LC_ALL": "C", "OMP_NUM_THREADS": "1"}
    if os.name == "nt":
        for key in ("SystemRoot", "WINDIR"):
            if value := os.environ.get(key):
                environment[key] = value
    environment.update(TMP=str(directory), TEMP=str(directory), TMPDIR=str(directory))
    return environment


def _cleanup(directory: Path) -> None:
    for generated in directory.iterdir():
        if generated.is_file() and not generated.is_symlink():
            generated.unlink(missing_ok=True)
    directory.rmdir()


def _executable(command: str) -> str:
    executable = shutil.which(command)
    if executable is None or (os.name == "nt" and Path(executable).suffix.lower() != ".exe"):
        raise ArtifactError("ffmpeg_unavailable", retryable=True)
    return os.path.abspath(executable)


async def _finish[T](task: asyncio.Task[T]) -> T:
    """Finish owned cleanup even if cancellation is requested more than once."""
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


async def _terminate(process: asyncio.subprocess.Process, job: WindowsJob | None = None) -> None:
    """Kill the owned POSIX session or Windows job, then reap the direct child."""
    if sys.platform == "win32":
        assert job is not None
        job.close()
        # Assignment failure leaves only the blocked, controlled launcher alive.
        try:
            if process.returncode is None:
                process.kill()
        except ProcessLookupError:
            pass
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    async def discard(stream: asyncio.StreamReader | None) -> None:
        if stream is not None:
            while await stream.read(64 * 1024):
                pass

    # A full pipe may pause its transport. Drain after killing so both pipe handles
    # close and Process.wait cannot hang behind unread output on either platform.
    await asyncio.gather(process.wait(), discard(process.stdout), discard(process.stderr))


class FFmpegExtractor:
    def __init__(
        self,
        limits: MediaLimits,
        *,
        ffmpeg_command: str = "ffmpeg",
        ffprobe_command: str = "ffprobe",
    ) -> None:
        self.limits = limits
        self.ffmpeg_command = ffmpeg_command
        self.ffprobe_command = ffprobe_command

    async def _run(
        self, command: list[str], directory: Path, *, output_limit: int
    ) -> tuple[bytes, bytes]:
        executable = _executable(command[0])
        kwargs: dict[str, Any]
        job: WindowsJob | None = None
        monitor: asyncio.Task[None] | None = None
        if sys.platform == "win32":
            kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW}
            launch_command = [sys.executable, "-I", "-S", "-c", LAUNCHER, executable, *command[1:]]
        else:
            kwargs = {"start_new_session": True}
            launch_command = [executable, *command[1:]]

        async def start() -> asyncio.subprocess.Process:
            nonlocal job, monitor
            process: asyncio.subprocess.Process | None = None
            try:
                if sys.platform == "win32":
                    job = WindowsJob()
                process = await asyncio.create_subprocess_exec(
                    *launch_command,
                    stdin=asyncio.subprocess.PIPE if job else asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=directory,
                    env=_environment(directory),
                    limit=64 * 1024,
                    **kwargs,
                )
                if job is not None:
                    if process.returncode is not None:
                        raise OSError("windows_process_unavailable")
                    job.assign(process.pid)
                    monitor = asyncio.create_task(job.close_after_exit(process))
                    assert process.stdin is not None
                    process.stdin.write(b"G")
                    process.stdin.close()
                return process
            except BaseException:
                if process is not None:
                    await _finish(asyncio.create_task(_terminate(process, job)))
                elif job is not None:
                    job.close()
                if monitor is not None:
                    await _finish(monitor)
                raise

        spawn = asyncio.create_task(start())
        try:
            process = await asyncio.shield(spawn)
        except asyncio.CancelledError:
            try:
                process = await _finish(spawn)
            except OSError:
                raise asyncio.CancelledError from None
            await _finish(asyncio.create_task(_terminate(process, job)))
            if monitor is not None:
                await _finish(monitor)
            raise
        except OSError:
            raise ArtifactError("ffmpeg_unavailable", retryable=True) from None

        async def read(stream: asyncio.StreamReader | None, limit: int, code: str) -> bytes:
            assert stream is not None
            result = bytearray()
            while block := await stream.read(64 * 1024):
                if len(result) + len(block) > limit:
                    raise ArtifactError(code)
                result.extend(block)
            return bytes(result)

        stdout = asyncio.create_task(read(process.stdout, output_limit, "media_output_limit"))
        stderr = asyncio.create_task(read(process.stderr, _LOG_LIMIT, "media_log_limit"))
        completion = asyncio.create_task(process.wait())
        tasks = (stdout, stderr, completion, *((monitor,) if monitor is not None else ()))

        async def cleanup() -> None:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await _terminate(process, job)

        try:
            async with asyncio.timeout(self.limits.process_timeout_seconds):
                await asyncio.gather(*tasks)
        except BaseException as error:
            await _finish(asyncio.create_task(cleanup()))
            if isinstance(error, TimeoutError):
                raise ArtifactError("media_process_timeout") from None
            raise
        if process.returncode:
            if job is not None and process.returncode == LAUNCH_FAILURE:
                raise ArtifactError("ffmpeg_unavailable", retryable=True)
            configuration_errors = (
                b"Unrecognized option",
                b"Option not found",
                b"No such filter",
                b"Unknown encoder",
            )
            if any(marker in stderr.result() for marker in configuration_errors):
                raise ArtifactError("ffmpeg_unavailable", retryable=True)
            raise ArtifactError("media_unreadable")
        return stdout.result(), stderr.result()

    def _input_options(self, path: Path, *, mov: bool = True) -> list[str]:
        return [
            "-threads",
            "1",
            "-max_alloc",
            "67108864",
            "-protocol_whitelist",
            "file",
            "-format_whitelist",
            _DEMUXERS,
            *(["-enable_drefs", "0", "-use_absolute_path", "0"] if mov else []),
            "-max_pixels",
            str(self.limits.max_pixels),
            "-probesize",
            "5000000",
            "-analyzeduration",
            "5000000",
            "-max_probe_packets",
            "2500",
            "-max_streams",
            "32",
            "-i",
            str(path),
        ]

    def _decode_command(
        self, path: Path, *, info_log: bool = False, mov: bool = False
    ) -> list[str]:
        return [
            self.ffmpeg_command,
            "-hide_banner",
            "-nostdin",
            "-nostats",
            "-loglevel",
            "info" if info_log else "error",
            "-xerror",
            "-filter_threads",
            "1",
            "-filter_complex_threads",
            "1",
            "-copyts",
            "-start_at_zero",
            *self._input_options(path, mov=mov),
            "-map_metadata",
            "-1",
            "-map_chapters",
            "-1",
            "-sn",
            "-dn",
        ]

    async def extract(self, path: Path, directory: Path) -> ExtractedMedia:
        source = _safe_path(path)
        destination = _safe_path(directory, directory=True)
        try:
            size = source.stat().st_size
            if size <= 0:
                raise ArtifactError("media_unreadable")
            if size > self.limits.max_input_bytes:
                raise ArtifactError("media_input_limit")
            output = Path(tempfile.mkdtemp(prefix="media-", dir=destination))
        except OSError:
            raise ArtifactError("media_unreadable") from None
        try:
            async with asyncio.timeout(self.limits.total_timeout_seconds):
                return await self._extract(source, output)
        except BaseException as error:
            # Only this invocation's generated, fixed-name files are removed.
            _cleanup(output)
            if isinstance(error, TimeoutError):
                raise ArtifactError("media_total_timeout") from None
            if isinstance(error, OSError):
                raise ArtifactError("media_unreadable") from None
            raise

    async def _extract(self, path: Path, directory: Path) -> ExtractedMedia:
        raw_info, _ = await self._run(
            [
                self.ffprobe_command,
                "-v",
                "error",
                *self._input_options(path),
                "-show_entries",
                "format=format_name,duration:stream=codec_type,codec_name,width,height,duration,sample_rate,channels:stream_disposition=attached_pic",
                "-of",
                "json",
            ],
            directory,
            output_limit=_PROBE_LIMIT,
        )
        info = _parse_info(raw_info, self.limits)
        mov = "mov" in json.loads(raw_info)["format"]["format_name"].split(",")
        audio: list[AudioChunk] = []
        frames: list[VideoFrame] = []
        used = 0
        if info.has_audio:
            pcm, _ = await self._run(
                [
                    *self._decode_command(path, mov=mov),
                    "-map",
                    "0:a:0",
                    "-vn",
                    "-af",
                    "aresample=16000:async=1:first_pts=0",
                    "-t",
                    f"{info.duration:.9f}",
                    "-ac",
                    "1",
                    "-ar",
                    "16000",
                    "-c:a",
                    "pcm_s16le",
                    "-threads",
                    "1",
                    "-f",
                    "s16le",
                    "pipe:1",
                ],
                directory,
                output_limit=min(
                    self.limits.max_output_bytes,
                    math.ceil(info.duration * _SAMPLE_RATE) * 2 + 2,
                ),
            )
            if not pcm or len(pcm) % 2:
                raise ArtifactError("media_unreadable")
            step = self.limits.chunk_seconds * _PCM_BYTES_PER_SECOND
            for index, offset in enumerate(range(0, len(pcm), step)):
                piece = memoryview(pcm)[offset : offset + step]
                used += len(piece) + 44
                if used > self.limits.max_output_bytes:
                    raise ArtifactError("media_output_limit")
                chunk = directory / f"audio-{index:04d}.wav"
                with chunk.open("xb") as stream, wave.open(stream, "wb") as wav:
                    wav.setnchannels(1)
                    wav.setsampwidth(2)
                    wav.setframerate(_SAMPLE_RATE)
                    wav.writeframes(piece)
                audio.append(
                    AudioChunk(
                        chunk,
                        offset / _PCM_BYTES_PER_SECOND,
                        (offset + len(piece)) / _PCM_BYTES_PER_SECOND,
                    )
                )
            del pcm
        if info.has_video:
            maximum = self.limits.max_frames * 4
            bucket = max(self.limits.min_frame_gap_seconds, info.duration / maximum)
            selection = (
                f"gt(scene,{self.limits.scene_threshold:.9f})*"
                f"(isnan(prev_selected_t)+gte(t-prev_selected_t,{self.limits.min_frame_gap_seconds:.9f})*"
                f"gt(floor(t/{bucket:.9f}),floor(prev_selected_t/{bucket:.9f})))"
            )
            _, diagnostics = await self._run(
                [
                    *self._decode_command(path, info_log=True, mov=mov),
                    "-map",
                    "0:V:0",
                    "-an",
                    "-vf",
                    f"scale=320:320:force_original_aspect_ratio=decrease,select='{selection}',showinfo",
                    "-t",
                    f"{info.duration:.9f}",
                    "-frames:v",
                    str(maximum),
                    "-fps_mode",
                    "vfr",
                    "-threads",
                    "1",
                    "-f",
                    "null",
                    "pipe:1",
                ],
                directory,
                output_limit=1024,
            )
            candidates = _timestamps(diagnostics, info.duration, maximum)
            for index, target in enumerate(_select_times(info.duration, candidates, self.limits)):
                # Decode from the beginning: output seeking can relabel a preceding frame.
                # showinfo after select records the actual chosen presentation timestamp.
                image, diagnostics = await self._run(
                    [
                        *self._decode_command(path, info_log=True, mov=mov),
                        "-map",
                        "0:V:0",
                        "-an",
                        "-vf",
                        f"select='gte(t,{target:.9f})',scale={self.limits.frame_width}:{self.limits.frame_width}:"
                        "force_original_aspect_ratio=decrease:force_divisible_by=2,setsar=1,showinfo",
                        "-t",
                        f"{info.duration:.9f}",
                        "-frames:v",
                        "1",
                        "-fps_mode",
                        "vfr",
                        "-c:v",
                        "mjpeg",
                        "-q:v",
                        "4",
                        "-threads",
                        "1",
                        "-f",
                        "image2pipe",
                        "pipe:1",
                    ],
                    directory,
                    output_limit=min(_JPEG_LIMIT, self.limits.max_output_bytes - used),
                )
                timestamps = _timestamps(diagnostics, info.duration, 2)
                # FFmpeg may feed a lookahead frame to the filter before -frames:v stops.
                if not image:
                    continue
                if (
                    not timestamps
                    or not image.startswith(b"\xff\xd8")
                    or not image.endswith(b"\xff\xd9")
                ):
                    raise ArtifactError("media_unreadable")
                timestamp = timestamps[0]
                if frames and timestamp - frames[-1].timestamp < self.limits.min_frame_gap_seconds:
                    continue
                used += len(image)
                frame = directory / f"frame-{index:04d}.jpg"
                with frame.open("xb") as stream:
                    stream.write(image)
                frames.append(VideoFrame(frame, timestamp))
            if not frames:
                raise ArtifactError("media_unreadable")
        return ExtractedMedia(info, tuple(audio), tuple(frames))
