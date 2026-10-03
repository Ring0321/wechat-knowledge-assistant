"""Process isolation and resource limits for untrusted local documents."""

import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceType
from app.domain.parsing import ParsedContent, ParseLimits

SUFFIXES = {
    ".pdf": SourceType.PDF,
    ".docx": SourceType.WORD,
    ".xlsx": SourceType.EXCEL,
    ".pptx": SourceType.PPT,
    ".png": SourceType.IMAGE,
    ".jpg": SourceType.IMAGE,
    ".jpeg": SourceType.IMAGE,
    ".webp": SourceType.IMAGE,
    ".tif": SourceType.IMAGE,
    ".tiff": SourceType.IMAGE,
    ".bmp": SourceType.IMAGE,
}
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class LocalFileParser:
    def __init__(
        self,
        limits: ParseLimits,
        *,
        ocr_command: str = "tesseract",
        ocr_languages: str = "chi_sim+eng",
    ) -> None:
        self.limits, self.ocr_command, self.ocr_languages = limits, ocr_command, ocr_languages

    async def parse(
        self, path: Path, *, filename: str | None, source_type: SourceType, url: str | None = None
    ) -> ParsedContent:
        suffix = Path(filename or "").suffix.lower()
        if suffix in (".doc", ".xls", ".ppt", ".docm", ".xlsm", ".pptm"):
            raise ArtifactError("legacy_or_macro_format_unsupported")
        kind = SUFFIXES.get(suffix, source_type)
        if kind not in (
            SourceType.PDF,
            SourceType.WORD,
            SourceType.EXCEL,
            SourceType.PPT,
            SourceType.IMAGE,
            SourceType.WEB_PAGE,
            SourceType.WECHAT_ARTICLE,
        ):
            raise ArtifactError("unsupported_format")
        # Explicit environment allowlist: parser/OCR cannot inherit application credentials.
        environment = {
            key: os.environ[key]
            for key in ("PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP")
            if key in os.environ
        }
        environment.update(
            PYTHONPATH=str(PROJECT_ROOT),
            PYTHONNOUSERSITE="1",
            PYTHONIOENCODING="utf-8",
            OMP_THREAD_LIMIT="1",
        )
        with TemporaryDirectory(prefix="pkb-parser-") as temp:
            request, result = Path(temp) / "request.json", Path(temp) / "result.json"
            payload = {
                "path": str(path),
                "source_type": kind.value,
                "limits": self.limits.model_dump(),
                "ocr_command": self.ocr_command,
                "ocr_languages": self.ocr_languages,
                "url": url,
            }
            await asyncio.to_thread(request.write_text, json.dumps(payload), encoding="utf-8")
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "app.parsers.runner",
                str(request),
                str(result),
                cwd=temp,
                env=environment,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=sys.platform != "win32",
            )
            try:
                async with asyncio.timeout(self.limits.timeout_seconds):
                    await process.wait()
            except (TimeoutError, asyncio.CancelledError) as error:
                if process.returncode is None:
                    if sys.platform == "win32":
                        # taskkill /T kills the OCR descendant as well; no shell or user args.
                        killer = await asyncio.create_subprocess_exec(
                            "taskkill",
                            "/PID",
                            str(process.pid),
                            "/T",
                            "/F",
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL,
                        )
                        await killer.wait()
                    else:
                        os.killpg(process.pid, signal.SIGKILL)
                    await process.wait()
                if isinstance(error, asyncio.CancelledError):
                    raise
                raise ArtifactError("parser_timeout") from None
            if process.returncode != 0:
                raise ArtifactError("parser_process_failed")
            try:
                if (
                    await asyncio.to_thread(result.stat)
                ).st_size > self.limits.max_text_chars * 16 + 1024 * 1024:
                    raise ArtifactError("parse_output_limit")
                response = json.loads(await asyncio.to_thread(result.read_text, encoding="utf-8"))
                if "error" in response:
                    raise ArtifactError(str(response["error"]))
                parsed = ParsedContent.model_validate(response["ok"])
                if len(parsed.text) > self.limits.max_text_chars:
                    raise ArtifactError("parse_text_limit")
                return parsed
            except (OSError, ValueError, KeyError):
                raise ArtifactError("parser_output_invalid") from None
