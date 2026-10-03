"""PDF text and local OCR. Called only inside the bounded parser subprocess."""

import math
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pypdfium2 as pdfium  # type: ignore[import-untyped]
from PIL import Image, ImageOps, UnidentifiedImageError
from pypdf import PdfReader

from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceType
from app.domain.parsing import ParsedContent, ParseLimits


def clean(value: str) -> str:
    return "".join(char for char in value if char in "\n\t" or ord(char) >= 32).strip()


def ocr(image: Image.Image, limits: ParseLimits, *, command: str, languages: str) -> str:
    if image.width * image.height > limits.max_pixels:
        raise ArtifactError("image_pixel_limit")
    with TemporaryDirectory(prefix="ocr-") as temp:
        source = Path(temp) / "input.png"
        output = Path(temp) / "result"
        ImageOps.exif_transpose(image).convert("RGB").save(source)
        try:
            result = subprocess.run(
                [command, str(source), str(output), "-l", languages, "--psm", "3"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=min(limits.timeout_seconds, 20),
                check=False,
            )
        except FileNotFoundError:
            raise ArtifactError("ocr_unavailable") from None
        except subprocess.TimeoutExpired:
            raise ArtifactError("ocr_timeout") from None
        if result.returncode != 0:
            raise ArtifactError("ocr_failed")
        path = output.with_suffix(".txt")
        if path.stat().st_size > limits.max_text_chars * 4:
            raise ArtifactError("parse_text_limit")
        return clean(path.read_text(encoding="utf-8"))


def parse_image(
    path: Path, limits: ParseLimits, *, command: str = "tesseract", languages: str = "chi_sim+eng"
) -> ParsedContent:
    try:
        with Image.open(path, formats=["PNG", "JPEG", "WEBP", "TIFF", "BMP"]) as image:
            if image.width * image.height > limits.max_pixels:
                raise ArtifactError("image_pixel_limit")
            if getattr(image, "n_frames", 1) != 1:
                raise ArtifactError("animated_image_unsupported")
            text = ocr(image, limits, command=command, languages=languages)
            if len(text) > limits.max_text_chars:
                raise ArtifactError("parse_text_limit")
            return ParsedContent(
                source_type=SourceType.IMAGE,
                text=text,
                segments=[{"text": text, "locator": {"image": 1}, "method": "ocr"}] if text else [],
                metadata={
                    "parser": "tesseract",
                    "parse_status": "normalized" if text else "empty",
                    "ocr": True,
                    "ocr_languages": languages,
                    "width": image.width,
                    "height": image.height,
                },
            )
    except (
        UnidentifiedImageError,
        OSError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
    ):
        raise ArtifactError("image_invalid") from None


def parse_pdf(
    path: Path, limits: ParseLimits, *, command: str = "tesseract", languages: str = "chi_sim+eng"
) -> ParsedContent:
    reader = PdfReader(path, strict=True)
    if reader.is_encrypted:
        raise ArtifactError("pdf_encrypted")
    if len(reader.pages) > limits.max_pages:
        raise ArtifactError("parse_page_limit")
    output = ParsedContent(
        source_type=SourceType.PDF,
        metadata={"parser": "pypdf+tesseract", "page_count": len(reader.pages), "ocr": False},
    )
    if reader.metadata and reader.metadata.title:
        output.title = clean(str(reader.metadata.title))[:512] or None
    parts: list[str] = []
    size = 0
    document: Any = None
    try:
        for index, page in enumerate(reader.pages):
            body = clean(page.extract_text() or "")
            method = "embedded_text"
            if not body:
                if document is None:
                    document = pdfium.PdfDocument(str(path))
                rendered_page = document[index]
                try:
                    width, height = rendered_page.get_size()
                    scale = min(2.0, math.sqrt(limits.max_pixels / max(width * height, 1)))
                    if scale < 0.5:
                        raise ArtifactError("image_pixel_limit")
                    bitmap = rendered_page.render(scale=scale)
                    try:
                        image = bitmap.to_pil()
                        try:
                            body = ocr(image, limits, command=command, languages=languages)
                        finally:
                            image.close()
                    finally:
                        bitmap.close()
                finally:
                    rendered_page.close()
                method = "ocr"
                output.metadata["ocr"] = True
            section = f"[第 {index + 1} 页]\n{body}"
            size += len(section) + 2
            if size > limits.max_text_chars:
                raise ArtifactError("parse_text_limit")
            if body:
                parts.append(section)
                output.segments.append(
                    {"text": body, "locator": {"page": index + 1}, "method": method}
                )
        output.text = "\n\n".join(parts)
        output.metadata["parse_status"] = "normalized" if output.text else "empty"
        return output
    finally:
        if document is not None:
            document.close()
