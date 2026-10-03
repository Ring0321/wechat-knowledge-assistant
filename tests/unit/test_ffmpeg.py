"""Bounded media decoding, with disposable synthetic inputs and no provider calls."""

import asyncio
import json
import os
import shutil
import subprocess
import sys
import wave
from array import array
from pathlib import Path
from typing import Any

import pytest

from app.adapters import ffmpeg
from app.adapters.ffmpeg import FFmpegExtractor
from app.domain.artifacts import ArtifactError
from app.domain.media import MediaLimits


def _metadata(**changes: Any) -> bytes:
    data: dict[str, Any] = {
        "format": {"format_name": "wav", "duration": "3.25"},
        "streams": [
            {
                "codec_type": "audio",
                "codec_name": "pcm_s16le",
                "sample_rate": "16000",
                "channels": 1,
            }
        ],
    }
    data.update(changes)
    return json.dumps(data).encode()


def _video(**changes: Any) -> dict[str, Any]:
    return {"codec_type": "video", "codec_name": "h264", "width": 320, "height": 180, **changes}


def _showinfo(*values: str | float) -> bytes:
    return b"\n".join(
        f"[Parsed_showinfo_2 @ 0x1234] n: {index:3} pts: {index} pts_time:{value} pos: -1".encode()
        for index, value in enumerate(values)
    )


def _wav(path: Path, samples: int, rate: int = 16000) -> bytes:
    # A reproducible signal also lets the split test prove that samples were not lost.
    pcm = b"\x01\x00\xff\xff" * (samples // 2) + b"\x01\x00" * (samples % 2)
    with wave.open(str(path), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(rate)
        wav.writeframes(pcm)
    return pcm


def _assert_error(error: pytest.ExceptionInfo[ArtifactError], code: str) -> None:
    assert error.value.code == code
    assert str(error.value) == code
    assert not error.value.retryable


def _assert_cleaned(source: Path) -> None:
    assert list(source.parent.iterdir()) == [source]


@pytest.mark.parametrize("raw", [b"", b"[]", b"{}", b"null", b"\xff", b"{bad", b"[" * 2000])
def test_malformed_probe_is_classified(raw: bytes) -> None:
    with pytest.raises(ArtifactError) as error:
        ffmpeg._parse_info(raw, MediaLimits())
    _assert_error(error, "media_invalid_metadata")


@pytest.mark.parametrize("duration", [None, True, [], "NaN", "inf", "-inf", "N/A", "0", "-1"])
def test_duration_must_be_finite_and_known(duration: object) -> None:
    with pytest.raises(ArtifactError) as error:
        ffmpeg._parse_info(
            _metadata(format={"format_name": "wav", "duration": duration}), MediaLimits()
        )
    _assert_error(error, "media_invalid_metadata")


@pytest.mark.parametrize("name", [None, [], "", "hls", "concat", "mov,hls", "image2"])
def test_unknown_demuxers_are_rejected(name: object) -> None:
    with pytest.raises(ArtifactError) as error:
        ffmpeg._parse_info(_metadata(format={"format_name": name, "duration": "3"}), MediaLimits())
    _assert_error(error, "media_unsupported")


@pytest.mark.parametrize("streams", [None, {}, [], [None], [_video()] * 33])
def test_invalid_stream_arrays(streams: object) -> None:
    with pytest.raises(ArtifactError) as error:
        ffmpeg._parse_info(_metadata(streams=streams), MediaLimits())
    _assert_error(error, "media_invalid_metadata")


@pytest.mark.parametrize(
    ("stream", "code"),
    [
        ({"codec_type": []}, "media_unsupported"),
        ({"codec_type": "unknown"}, "media_unsupported"),
        ({"codec_type": "audio", "codec_name": "unknown"}, "media_unsupported"),
        (_video(width="N/A"), "media_invalid_metadata"),
        (_video(width=1.5), "media_invalid_metadata"),
        (_video(width=-1), "media_invalid_metadata"),
        (_video(width=4000, height=4000), "media_pixel_limit"),
        (_video(disposition=[]), "media_invalid_metadata"),
        (_video(disposition={"attached_pic": "0"}), "media_invalid_metadata"),
        (_video(duration="NaN"), "media_invalid_metadata"),
        (_video(duration=901), "media_duration_limit"),
        ({"codec_type": "subtitle"}, "media_unsupported"),
        (
            {"codec_type": "audio", "codec_name": "aac", "channels": 33, "sample_rate": 48000},
            "media_audio_limit",
        ),
        (
            {"codec_type": "audio", "codec_name": "aac", "channels": 1, "sample_rate": 384001},
            "media_audio_limit",
        ),
    ],
)
def test_unsafe_or_unknown_streams(stream: object, code: str) -> None:
    with pytest.raises(ArtifactError) as error:
        ffmpeg._parse_info(_metadata(streams=[stream]), MediaLimits())
    _assert_error(error, code)


def test_duration_limit() -> None:
    with pytest.raises(ArtifactError) as error:
        ffmpeg._parse_info(_metadata(), MediaLimits(max_duration_seconds=3))
    _assert_error(error, "media_duration_limit")


def test_audio_cover_is_not_temporal_video() -> None:
    audio = json.loads(_metadata())["streams"][0]
    info = ffmpeg._parse_info(
        _metadata(streams=[_video(disposition={"attached_pic": 1}), audio]), MediaLimits()
    )
    assert info.has_audio and not info.has_video
    assert (info.width, info.height) == (0, 0)


def test_silent_video_is_supported() -> None:
    info = ffmpeg._parse_info(_metadata(streams=[_video()]), MediaLimits())
    assert info.has_video and not info.has_audio
    assert (info.width, info.height) == (320, 180)


def test_periodic_and_scene_selection_keeps_coverage_and_deduplicates() -> None:
    limits = MediaLimits(max_frames=6, frame_interval_seconds=10, min_frame_gap_seconds=2)
    values = ffmpeg._select_times(120, [3, 3, 3.5, 4, 11, 31, 62, 81, 103, 119], limits)
    assert len(values) == 6
    assert 0 in values and 110 in values
    assert any(value in {31, 62, 81, 103, 119} for value in values)
    assert all(right - left >= 2 for left, right in zip(values, values[1:], strict=False))


def test_periodic_selection_is_sparse_not_per_second() -> None:
    values = ffmpeg._select_times(900, [], MediaLimits())
    assert len(values) == 12
    assert values[0] == 0 and values[-1] == 870
    assert all(right - left > 30 for left, right in zip(values, values[1:], strict=False))
    assert ffmpeg._select_times(0.1, [], MediaLimits(max_frames=1)) == [0]


@pytest.mark.parametrize("value", ["nan", "inf", "-1", "secret", "12"])
def test_invalid_actual_frame_timestamps(value: str) -> None:
    with pytest.raises(ArtifactError) as error:
        ffmpeg._timestamps(_showinfo(value), 10, 2)
    _assert_error(error, "media_invalid_metadata")


def test_actual_timestamps_preserve_precision_and_order() -> None:
    assert ffmpeg._timestamps(_showinfo(0, 2.375), 10, 2) == [0, 2.375]
    with pytest.raises(ArtifactError) as error:
        ffmpeg._timestamps(_showinfo(3, 2), 10, 2)
    _assert_error(error, "media_invalid_metadata")
    with pytest.raises(ArtifactError) as error:
        ffmpeg._timestamps(_showinfo(0, 1, 2), 10, 2)
    _assert_error(error, "media_frame_limit")


def test_process_environment_does_not_inherit_secrets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for key in ("OPENAI_API_KEY", "AWS_SECRET_ACCESS_KEY", "HTTP_PROXY", "FFREPORT", "LD_PRELOAD"):
        monkeypatch.setenv(key, "sensitive-do-not-inherit")
    environment = ffmpeg._environment(tmp_path)
    assert "sensitive-do-not-inherit" not in environment.values()
    assert environment["OMP_NUM_THREADS"] == "1"
    assert environment["TMP"] == str(tmp_path)


def test_commands_restrict_nested_reads_and_write_only_stdout(tmp_path: Path) -> None:
    extractor = FFmpegExtractor(MediaLimits())
    command = extractor._decode_command(tmp_path / "input.mp4", mov=True)
    assert command[command.index("-protocol_whitelist") + 1] == "file"
    assert command[command.index("-enable_drefs") + 1] == "0"
    assert command[command.index("-use_absolute_path") + 1] == "0"
    formats = set(command[command.index("-format_whitelist") + 1].split(","))
    assert not {"concat", "hls", "dash", "image2", "sdp", "aviSynth"} & formats
    assert command.count("-i") == 1 and "-nostdin" in command


@pytest.mark.parametrize(("size", "code"), [(0, "media_unreadable"), (1025, "media_input_limit")])
async def test_input_limits_before_spawning(tmp_path: Path, size: int, code: str) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"x" * size)
    extractor = FFmpegExtractor(MediaLimits(max_input_bytes=1024), ffprobe_command="does-not-exist")
    with pytest.raises(ArtifactError) as error:
        await extractor.extract(source, tmp_path)
    _assert_error(error, code)
    _assert_cleaned(source)


async def test_missing_binary_is_retryable_and_cleans_output(tmp_path: Path) -> None:
    source = tmp_path / "source.wav"
    _wav(source, 100)
    with pytest.raises(ArtifactError) as error:
        await FFmpegExtractor(MediaLimits(), ffprobe_command="absent-ffprobe-12345").extract(
            source, tmp_path
        )
    assert error.value.code == "ffmpeg_unavailable" and error.value.retryable
    _assert_cleaned(source)


async def test_process_failure_suppresses_raw_diagnostics(tmp_path: Path) -> None:
    with pytest.raises(ArtifactError) as error:
        await FFmpegExtractor(MediaLimits())._run(
            [
                sys.executable,
                "-c",
                "import sys;sys.stderr.write('sensitive-input-and-URL');sys.exit(1)",
            ],
            tmp_path,
            output_limit=100,
        )
    _assert_error(error, "media_unreadable")


@pytest.mark.parametrize(
    ("stream", "code"), [("stdout", "media_output_limit"), ("stderr", "media_log_limit")]
)
async def test_child_output_is_bounded(tmp_path: Path, stream: str, code: str) -> None:
    with pytest.raises(ArtifactError) as error:
        await FFmpegExtractor(MediaLimits())._run(
            [sys.executable, "-c", f"import sys;sys.{stream}.buffer.write(b'x' * 300000)"],
            tmp_path,
            output_limit=1024,
        )
    _assert_error(error, code)


async def test_process_timeout(tmp_path: Path) -> None:
    with pytest.raises(ArtifactError) as error:
        await FFmpegExtractor(MediaLimits(process_timeout_seconds=0.15))._run(
            [sys.executable, "-c", "import time;time.sleep(30)"],
            tmp_path,
            output_limit=100,
        )
    _assert_error(error, "media_process_timeout")


async def test_cancellation_terminates_and_reaps_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started = asyncio.Event()
    processes: list[asyncio.subprocess.Process] = []
    original = asyncio.create_subprocess_exec

    async def record(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        process = await original(*args, **kwargs)
        # Includes the controlled Windows launcher; all direct children must finish.
        processes.append(process)
        started.set()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", record)
    task = asyncio.create_task(
        FFmpegExtractor(MediaLimits())._run(
            [sys.executable, "-c", "import time;time.sleep(30)"],
            tmp_path,
            output_limit=100,
        )
    )
    await asyncio.wait_for(started.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    assert processes and all(process.returncode is not None for process in processes)


async def test_cancellation_during_spawn_does_not_orphan_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spawned = asyncio.Event()
    release = asyncio.Event()
    processes: list[asyncio.subprocess.Process] = []
    original = asyncio.create_subprocess_exec

    async def delayed(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        process = await original(*args, **kwargs)
        processes.append(process)
        if args[0] == sys.executable:
            spawned.set()
            await release.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed)
    task = asyncio.create_task(
        FFmpegExtractor(MediaLimits())._run(
            [sys.executable, "-c", "import time;time.sleep(30)"],
            tmp_path,
            output_limit=100,
        )
    )
    await asyncio.wait_for(spawned.wait(), 5)
    task.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    assert processes and all(process.returncode is not None for process in processes)


def _pid_exists(pid: int) -> bool:
    if sys.platform == "win32":
        import ctypes

        kernel = ctypes.windll.kernel32
        kernel.OpenProcess.restype = ctypes.c_void_p
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        code = ctypes.c_ulong()
        try:
            return (
                bool(kernel.GetExitCodeProcess(ctypes.c_void_p(handle), ctypes.byref(code)))
                and code.value == 259
            )
        finally:
            kernel.CloseHandle(ctypes.c_void_p(handle))
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A killed orphan can briefly remain a zombie until its init process reaps it.
    status = Path(f"/proc/{pid}/status")
    return not (status.exists() and "State:\tZ" in status.read_text())


async def test_cancellation_kills_descendant_process(tmp_path: Path) -> None:
    ready = tmp_path / "child.pid"
    script = (
        "import pathlib,subprocess,sys,time;"
        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);"
        "pathlib.Path('child.pid').write_text(str(p.pid));time.sleep(30)"
    )
    task = asyncio.create_task(
        FFmpegExtractor(MediaLimits())._run(
            [sys.executable, "-c", script],
            tmp_path,
            output_limit=100,
        )
    )
    child_pid: int | None = None
    try:
        async with asyncio.timeout(5):
            while not await asyncio.to_thread(ready.exists):  # noqa: ASYNC110 - external process
                await asyncio.sleep(0.02)
        child_pid = int(ready.read_text())
        assert _pid_exists(child_pid)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        async with asyncio.timeout(5):
            while _pid_exists(child_pid):  # noqa: ASYNC110 - external process
                await asyncio.sleep(0.02)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if child_pid is not None and _pid_exists(child_pid):
            os.kill(child_pid, 9)


@pytest.fixture
def real_ffmpeg() -> str:
    executable = shutil.which("ffmpeg")
    if executable is None or shutil.which("ffprobe") is None:
        pytest.skip("Real ffmpeg/ffprobe required for synthetic media verification")
    return executable


async def test_real_audio_chunks_have_exact_samples_and_offsets(
    tmp_path: Path, real_ffmpeg: str
) -> None:
    source = tmp_path / "source.wav"
    original = _wav(source, 3 * 16000 + 123)
    result = await FFmpegExtractor(
        MediaLimits(chunk_seconds=1), ffmpeg_command=real_ffmpeg
    ).extract(source, tmp_path)
    assert result.info.has_audio and not result.frames
    assert [chunk.start for chunk in result.audio] == [0, 1, 2, 3]
    assert [chunk.end for chunk in result.audio] == [1, 2, 3, len(original) / 32000]
    reconstructed = bytearray()
    for chunk in result.audio:
        with wave.open(str(chunk.path), "rb") as wav:
            assert (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) == (1, 2, 16000)
            assert wav.getnframes() == round((chunk.end - chunk.start) * 16000)
            reconstructed.extend(wav.readframes(wav.getnframes()))
    assert reconstructed == original


async def test_real_stereo_resampled_to_16k_mono(tmp_path: Path, real_ffmpeg: str) -> None:
    source = tmp_path / "stereo.wav"
    with wave.open(str(source), "wb") as wav:
        wav.setnchannels(2)
        wav.setsampwidth(2)
        wav.setframerate(48000)
        wav.writeframes(b"\x01\x00\x03\x00" * 48000)
    result = await FFmpegExtractor(MediaLimits()).extract(source, tmp_path)
    assert len(result.audio) == 1
    with wave.open(str(result.audio[0].path), "rb") as wav:
        assert (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) == (1, 2, 16000)
        assert wav.getnframes() == 16000
    assert (result.audio[0].start, result.audio[0].end) == (0, 1)


async def _make_video(
    executable: str, output: Path, *, sound: bool = False, scenes: bool = True
) -> None:
    # Four visible color changes, including one off the periodic five-second grid.
    command = [
        executable,
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-f",
        "lavfi",
        "-i",
        "color=red:s=160x96:r=8:d=3[color1];color=blue:s=160x96:r=8:d=3[color2];"
        "color=white:s=160x96:r=8:d=3[color3];color=black:s=160x96:r=8:d=3[color4];"
        "[color1][color2][color3][color4]concat=n=4:v=1:a=0",
    ]
    if not scenes:
        command[-1] = "color=red:s=160x96:r=8:d=12"
    if sound:
        command += [
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=16000:duration=12",
            "-c:a",
            "aac",
        ]
    command += ["-c:v", "mpeg4", "-threads", "1", "-y", str(output)]
    process = await asyncio.create_subprocess_exec(
        *command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )
    _, diagnostics = await asyncio.wait_for(process.communicate(), 30)
    assert process.returncode == 0, diagnostics.decode(errors="replace")


@pytest.mark.parametrize("sound", [False, True])
async def test_real_video_scene_and_periodic_frames(
    tmp_path: Path, real_ffmpeg: str, sound: bool
) -> None:
    source = tmp_path / "source.mp4"
    await _make_video(real_ffmpeg, source, sound=sound)
    limits = MediaLimits(max_frames=4, frame_interval_seconds=5, min_frame_gap_seconds=2)
    result = await FFmpegExtractor(limits, ffmpeg_command=real_ffmpeg).extract(source, tmp_path)
    timestamps = [frame.timestamp for frame in result.frames]
    assert result.info.has_video and result.info.has_audio is sound
    assert bool(result.audio) is sound
    assert 2 <= len(timestamps) <= 4
    assert timestamps[0] == 0 and timestamps[-1] == 10
    assert any(timestamp in (3, 6, 9) for timestamp in timestamps)
    assert all(right - left >= 2 for left, right in zip(timestamps, timestamps[1:], strict=False))
    for frame in result.frames:
        assert frame.path.read_bytes().startswith(b"\xff\xd8")
        assert 0 <= frame.timestamp < result.info.duration


async def test_real_frame_timestamp_is_actual_pts(tmp_path: Path, real_ffmpeg: str) -> None:
    source = tmp_path / "source.mp4"
    await _make_video(real_ffmpeg, source, scenes=False)
    result = await FFmpegExtractor(MediaLimits(max_frames=2, frame_interval_seconds=5.1)).extract(
        source, tmp_path
    )
    assert [frame.timestamp for frame in result.frames] == [0, 10.25]


@pytest.mark.parametrize(
    ("limits", "code"),
    [
        (MediaLimits(max_duration_seconds=2), "media_duration_limit"),
        (MediaLimits(max_output_bytes=1024), "media_output_limit"),
    ],
)
async def test_real_extraction_limits_cleanup(
    tmp_path: Path, real_ffmpeg: str, limits: MediaLimits, code: str
) -> None:
    source = tmp_path / "source.wav"
    _wav(source, 3 * 16000)
    with pytest.raises(ArtifactError) as error:
        await FFmpegExtractor(limits).extract(source, tmp_path)
    _assert_error(error, code)
    _assert_cleaned(source)


@pytest.mark.parametrize(
    "content",
    [
        b"not-media-with-secret-query",
        b"ffconcat version 1.0\nfile 'sensitive.wav'\n",
        b"#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXTINF:10,\nhttp://127.0.0.1:1/private\n#EXT-X-ENDLIST\n",
    ],
)
async def test_real_unknown_and_nested_formats_fail_closed(
    tmp_path: Path, real_ffmpeg: str, content: bytes
) -> None:
    source = tmp_path / "source"
    source.write_bytes(content)
    with pytest.raises(ArtifactError) as error:
        await FFmpegExtractor(MediaLimits()).extract(source, tmp_path)
    _assert_error(error, "media_unreadable")
    _assert_cleaned(source)


async def test_real_playlist_never_connects_to_nested_url(tmp_path: Path, real_ffmpeg: str) -> None:
    connections: list[bool] = []

    async def connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connections.append(True)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(connected, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        source = tmp_path / "input.m3u8"
        source.write_text(
            "#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXTINF:10,\n"
            f"http://127.0.0.1:{port}/private?token=never-requested\n#EXT-X-ENDLIST\n"
        )
        with pytest.raises(ArtifactError) as error:
            await FFmpegExtractor(MediaLimits()).extract(source, tmp_path)
        _assert_error(error, "media_unreadable")
        assert not connections
        _assert_cleaned(source)
    finally:
        server.close()
        await server.wait_closed()


async def test_real_audio_byte_budget_includes_wav_headers(
    tmp_path: Path, real_ffmpeg: str
) -> None:
    source = tmp_path / "source.wav"
    _wav(source, 16000)
    with pytest.raises(ArtifactError) as error:
        await FFmpegExtractor(MediaLimits(max_output_bytes=32043)).extract(source, tmp_path)
    _assert_error(error, "media_output_limit")
    _assert_cleaned(source)


async def test_real_delayed_audio_keeps_video_timeline(tmp_path: Path, real_ffmpeg: str) -> None:
    source = tmp_path / "delayed.mp4"
    process = await asyncio.create_subprocess_exec(
        real_ffmpeg,
        "-v",
        "error",
        "-nostdin",
        "-f",
        "lavfi",
        "-i",
        "color=black:s=160x96:r=8:d=4",
        "-itsoffset",
        "2",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=16000:duration=2",
        "-c:v",
        "mpeg4",
        "-c:a",
        "aac",
        "-threads",
        "1",
        "-y",
        str(source),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert await asyncio.wait_for(process.wait(), 20) == 0
    result = await FFmpegExtractor(MediaLimits(chunk_seconds=1, max_frames=1)).extract(
        source, tmp_path
    )
    assert result.audio[0].start == 0
    samples: list[int] = []
    for chunk in result.audio:
        with wave.open(str(chunk.path), "rb") as wav:
            values = array("h", wav.readframes(wav.getnframes()))
            if sys.byteorder != "little":
                values.byteswap()
            samples.extend(values)
    first_audible = next(index for index, value in enumerate(samples) if abs(value) > 500)
    assert 1.9 <= first_audible / 16000 <= 2.1
    assert 3.9 <= result.audio[-1].end <= 4.1
