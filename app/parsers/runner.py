"""Bounded parser child with a sanitized environment, not an OS privilege sandbox."""

import json
import math
import sys
from pathlib import Path

from pydantic import JsonValue

from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceType
from app.domain.parsing import ParseLimits


def main() -> int:
    response: dict[str, JsonValue]
    request_path, response_path = map(Path, sys.argv[1:3])
    request = json.loads(request_path.read_text(encoding="utf-8"))
    limits = ParseLimits.model_validate(request["limits"])
    if sys.platform != "win32":
        import resource

        resource.setrlimit(resource.RLIMIT_AS, (768 * 1024 * 1024, 768 * 1024 * 1024))
        cpu = math.ceil(limits.timeout_seconds) + 1
        resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))
        resource.setrlimit(resource.RLIMIT_FSIZE, (128 * 1024 * 1024, 128 * 1024 * 1024))
    try:
        path = Path(request["path"])
        if not path.is_file() or path.stat().st_size > limits.max_input_bytes:
            raise ArtifactError("parse_input_limit")
        kind = SourceType(request["source_type"])
        if kind in (SourceType.WEB_PAGE, SourceType.WECHAT_ARTICLE):
            from app.parsers.web import WeChatArticleParser, html_content

            result = (
                WeChatArticleParser.extract if kind == SourceType.WECHAT_ARTICLE else html_content
            )(path.read_bytes(), request["url"], limits)
        elif kind in (SourceType.PDF, SourceType.IMAGE):
            from app.parsers.pdf_image import parse_image, parse_pdf

            parser = parse_pdf if kind == SourceType.PDF else parse_image
            result = parser(
                path, limits, command=request["ocr_command"], languages=request["ocr_languages"]
            )
        else:
            from app.parsers.office import parse_office

            result = parse_office(path, kind, limits)
        response = {"ok": result.model_dump(mode="json")}
    except ArtifactError as error:
        response = {"error": error.code}
    except Exception:
        response = {"error": "document_invalid"}
    response_path.write_text(json.dumps(response, ensure_ascii=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
