"""Read bounded OOXML text without Office, formulas, macros or network access.

This module runs synchronously inside the caller's restricted parser process.
Only package XML is interpreted; embedded images/objects are not executed.
"""

import posixpath
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit
from xml.etree.ElementTree import Element
from zipfile import ZIP_DEFLATED, ZIP_STORED, BadZipFile, ZipFile
from zlib import error as ZlibError

from defusedxml.ElementTree import fromstring  # type: ignore[import-untyped]
from pydantic import JsonValue

from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceType
from app.domain.parsing import ParsedContent, ParseLimits

_VERSION = "ooxml-1"
_MAX_RATIO = 100
_MAX_DEPTH = 128
_CELL = re.compile(r"([A-Z]{1,3})([1-9][0-9]{0,6})\Z")
_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_STRICT_REL = "http://purl.oclc.org/ooxml/officeDocument/relationships"
_WORD_NS = {
    "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "http://purl.oclc.org/ooxml/wordprocessingml/main",
}
_SHEET_NS = {
    "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "http://purl.oclc.org/ooxml/spreadsheetml/main",
}
_SLIDE_NS = {
    "http://schemas.openxmlformats.org/presentationml/2006/main",
    "http://purl.oclc.org/ooxml/presentationml/main",
}
_DRAW_NS = {
    "http://schemas.openxmlformats.org/drawingml/2006/main",
    "http://purl.oclc.org/ooxml/drawingml/main",
}


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _namespace(tag: str) -> str:
    return tag[1:].split("}", 1)[0] if tag.startswith("{") else ""


def _children(element: Element, local: str) -> list[Element]:
    return [child for child in element if _local(child.tag) == local]


def _child(element: Element, local: str) -> Element | None:
    found = _children(element, local)
    if len(found) > 1:
        raise ArtifactError("office_invalid_structure")
    return found[0] if found else None


def _attribute(element: Element, name: str) -> str | None:
    return next((value for key, value in element.attrib.items() if _local(key) == name), None)


def _require_root(root: Element, name: str, namespaces: set[str]) -> None:
    if _local(root.tag) != name or _namespace(root.tag) not in namespaces:
        raise ArtifactError("office_invalid_structure")


def _text(element: Element, namespaces: set[str]) -> str:
    result: list[str] = []
    pending = [element]
    while pending:
        node = pending.pop()
        # Spreadsheet phonetic annotations are pronunciation aids, not cell content.
        if _namespace(node.tag) in _SHEET_NS and _local(node.tag) == "rPh":
            continue
        pending.extend(reversed(list(node)))
        if _namespace(node.tag) not in namespaces:
            continue
        local = _local(node.tag)
        if local == "t":
            result.append(node.text or "")
        elif local == "tab":
            result.append("\t")
        elif local in {"br", "cr"}:
            result.append("\n")
    return "".join(result).strip()


@dataclass(frozen=True)
class _Relationship:
    kind: str
    target: str | None


class _Package:
    def __init__(self, archive: ZipFile, limits: ParseLimits) -> None:
        self.archive, self.limits = archive, limits
        self.names: set[str] = set()
        self.external_relationships = 0
        self.relationships: dict[str, dict[str, _Relationship]] = {}
        entries = archive.infolist()
        if len(entries) > limits.max_archive_entries:
            raise ArtifactError("office_archive_entries_exceeded")
        total = 0
        for entry in entries:
            name = entry.filename
            if (
                not name
                or name.startswith("/")
                or "\\" in name
                or "\\" in entry.orig_filename
                or ":" in name
                or "\x00" in entry.orig_filename
                or any(part in {".", ".."} for part in name.split("/"))
                or name in self.names
                or stat.S_ISLNK(entry.external_attr >> 16)
            ):
                raise ArtifactError("office_invalid_archive_path")
            self.names.add(name)
            if entry.flag_bits & 1:
                raise ArtifactError("office_encrypted")
            if entry.compress_type not in {ZIP_STORED, ZIP_DEFLATED}:
                raise ArtifactError("office_unsupported_compression")
            total += entry.file_size
            if total > limits.max_archive_bytes:
                raise ArtifactError("office_archive_bytes_exceeded")
            if entry.file_size > max(entry.compress_size, 1) * _MAX_RATIO:
                raise ArtifactError("office_compression_ratio_exceeded")
            if name.lower().endswith("vbaproject.bin"):
                raise ArtifactError("office_macros_not_supported")
        types = self.xml("[Content_Types].xml")
        if _local(types.tag) != "Types":
            raise ArtifactError("office_invalid_structure")
        for item in types:
            value = item.get("ContentType", "").lower()
            if "macroenabled" in value or "vbaproject" in value:
                raise ArtifactError("office_macros_not_supported")
        for name in sorted(self.names):
            if name.endswith(".rels"):
                self._read_relationships(name)

    def xml(self, name: str) -> Element:
        if name not in self.names:
            raise ArtifactError("office_missing_part")
        info = self.archive.getinfo(name)
        with self.archive.open(info) as stream:
            data = stream.read(min(info.file_size, self.limits.max_archive_bytes) + 1)
        if len(data) != info.file_size:
            raise ArtifactError("office_invalid_archive")
        try:
            root: Element = fromstring(
                data, forbid_dtd=True, forbid_entities=True, forbid_external=True
            )
        except Exception:
            raise ArtifactError("office_invalid_xml") from None
        pending = [(root, 1)]
        nodes = 0
        # The byte bound limits XML size; this also bounds sparse/empty node graphs.
        while pending:
            element, depth = pending.pop()
            nodes += 1
            if depth > _MAX_DEPTH or nodes > self.limits.max_cells * 20 + 1000:
                raise ArtifactError("office_structure_limit_exceeded")
            pending.extend((child, depth + 1) for child in element)
        return root

    def _read_relationships(self, name: str) -> None:
        root = self.xml(name)
        if _local(root.tag) != "Relationships":
            raise ArtifactError("office_invalid_structure")
        if name == "_rels/.rels":
            owner = ""
        else:
            directory, basename = posixpath.split(name)
            if not directory.endswith("/_rels"):
                raise ArtifactError("office_invalid_structure")
            owner = posixpath.join(directory[:-6], basename[:-5])
        records: dict[str, _Relationship] = {}
        for relation in root:
            identifier = relation.get("Id", "")
            kind = relation.get("Type", "")
            target = relation.get("Target", "")
            mode = relation.get("TargetMode", "Internal")
            if not identifier or identifier in records or not kind or not target:
                raise ArtifactError("office_invalid_relationship")
            if mode == "External":
                self.external_relationships += 1
                records[identifier] = _Relationship(kind, None)
                continue
            if mode != "Internal":
                raise ArtifactError("office_invalid_relationship")
            decoded = unquote(target)
            parts = urlsplit(decoded)
            if (
                parts.scheme
                or parts.netloc
                or parts.query
                or parts.fragment
                or "\\" in decoded
                or "\x00" in decoded
            ):
                raise ArtifactError("office_invalid_relationship")
            resolved = posixpath.normpath(
                decoded.lstrip("/")
                if decoded.startswith("/")
                else posixpath.join(posixpath.dirname(owner), decoded)
            )
            if resolved == ".." or resolved.startswith("../") or resolved.startswith("/"):
                raise ArtifactError("office_invalid_relationship")
            records[identifier] = _Relationship(kind, resolved)
        self.relationships[owner] = records

    def target(self, owner: str, identifier: str, kind: str) -> str:
        relation = self.relationships.get(owner, {}).get(identifier)
        if relation is None or relation.kind not in {f"{_REL}/{kind}", f"{_STRICT_REL}/{kind}"}:
            raise ArtifactError("office_invalid_relationship")
        if relation.target is None:
            raise ArtifactError("office_external_required_part")
        return relation.target

    def kind_targets(self, owner: str, kind: str) -> list[str]:
        return [
            self.target(owner, identifier, kind)
            for identifier, relation in self.relationships.get(owner, {}).items()
            if relation.kind in {f"{_REL}/{kind}", f"{_STRICT_REL}/{kind}"}
        ]

    def title(self) -> str | None:
        if "docProps/core.xml" not in self.names:
            return None
        root = self.xml("docProps/core.xml")
        titles = [
            element.text or ""
            for element in root
            if element.tag == "{http://purl.org/dc/elements/1.1/}title"
        ]
        title = " ".join(titles).strip()
        if len(title) > self.limits.max_text_chars:
            raise ArtifactError("office_text_limit_exceeded")
        return title or None


class _Output:
    def __init__(self, source_type: SourceType, limits: ParseLimits) -> None:
        self.source_type, self.limits = source_type, limits
        self.segments: list[dict[str, JsonValue]] = []
        self.parts: list[str] = []
        self.characters = 0

    def append(self, content: str, locator: dict[str, JsonValue], **metadata: JsonValue) -> None:
        if not content.strip():
            return
        size = len(content) + (2 if self.parts else 0)
        if self.characters + size > self.limits.max_text_chars:
            raise ArtifactError("office_text_limit_exceeded")
        if len(self.segments) >= self.limits.max_cells:
            raise ArtifactError("office_structure_limit_exceeded")
        self.characters += size
        self.parts.append(content)
        self.segments.append({"text": content, "locator": locator, **metadata})

    def finish(self, package: _Package, **metadata: JsonValue) -> ParsedContent:
        return ParsedContent(
            text="\n\n".join(self.parts),
            title=package.title(),
            source_type=self.source_type,
            segments=self.segments,
            metadata={
                "parser": _VERSION,
                "parse_status": "normalized" if self.parts else "empty",
                "external_relationships_ignored": package.external_relationships,
                "embedded_objects_parsed": False,
                **metadata,
            },
        )


def _word(package: _Package, main: str, output: _Output) -> ParsedContent:
    root = package.xml(main)
    _require_root(root, "document", _WORD_NS)
    body = _child(root, "body")
    if body is None:
        raise ArtifactError("office_invalid_structure")
    paragraph = 0
    table = 0
    headings: dict[int, str] = {}
    pending: list[tuple[Element, dict[str, JsonValue]]] = [(body, {})]
    while pending:
        element, context = pending.pop()
        name = _local(element.tag)
        if name == "p":
            paragraph += 1
            if paragraph > package.limits.max_cells:
                raise ArtifactError("office_structure_limit_exceeded")
            properties = _child(element, "pPr")
            style = _child(properties, "pStyle") if properties is not None else None
            style_name = _attribute(style, "val") if style is not None else None
            extra: dict[str, JsonValue] = {}
            if style_name:
                extra["paragraph_style"] = style_name[:128]
            content = _text(element, _WORD_NS)
            outline = _child(properties, "outlineLvl") if properties is not None else None
            outline_value = _attribute(outline, "val") if outline is not None else None
            heading = re.fullmatch(r"Heading([1-9])", style_name or "", re.IGNORECASE)
            level = int(heading[1]) if heading else None
            if outline_value in {str(value) for value in range(9)}:
                level = int(outline_value) + 1
            if level is not None and content:
                headings = {key: value for key, value in headings.items() if key < level}
                headings[level] = content[:256]
                extra["heading_level"] = level
            if headings:
                extra["heading_path"] = [headings[key] for key in sorted(headings)]
            output.append(
                content,
                {"kind": "paragraph", "paragraph": paragraph, **context},
                **extra,
            )
            continue
        children = [(child, context) for child in element]
        if name == "tbl":
            table += 1
            children = [
                (row, {**context, "table": table, "row": index})
                for index, row in enumerate(_children(element, "tr"), 1)
            ]
        elif name == "tr":
            children = [
                (cell, {**context, "column": index})
                for index, cell in enumerate(_children(element, "tc"), 1)
            ]
        pending.extend(reversed(children))
    return output.finish(package, paragraph_count=paragraph, table_count=table)


def _cell_position(reference: str) -> tuple[int, int]:
    match = _CELL.fullmatch(reference)
    if match is None:
        raise ArtifactError("office_invalid_cell")
    column = 0
    for character in match[1]:
        column = column * 26 + ord(character) - ord("A") + 1
    row = int(match[2])
    if column > 16384 or row > 1048576:
        raise ArtifactError("office_invalid_cell")
    return column, row


def _column_name(column: int) -> str:
    result = ""
    while column:
        column, remainder = divmod(column - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _shared_strings(package: _Package, main: str) -> list[str]:
    paths = package.kind_targets(main, "sharedStrings")
    if len(paths) > 1:
        raise ArtifactError("office_invalid_structure")
    if not paths:
        return []
    root = package.xml(paths[0])
    _require_root(root, "sst", _SHEET_NS)
    strings: list[str] = []
    total = 0
    for item in _children(root, "si"):
        content = _text(item, _SHEET_NS)
        total += len(content)
        if len(strings) >= package.limits.max_cells or total > package.limits.max_text_chars:
            raise ArtifactError("office_shared_strings_limit_exceeded")
        strings.append(content)
    return strings


def _cell_value(cell: Element, strings: list[str]) -> tuple[str, str | None]:
    value = _child(cell, "v")
    raw = value.text or "" if value is not None else ""
    formula = _child(cell, "f")
    if formula is not None and not raw:
        return "公式（无缓存结果，未计算）", "missing"
    cell_type = cell.get("t", "n")
    if cell_type == "s":
        if not raw.isascii() or not raw.isdigit() or len(raw) > 10 or int(raw) >= len(strings):
            raise ArtifactError("office_invalid_shared_string")
        content = strings[int(raw)]
    elif cell_type == "inlineStr":
        inline = _child(cell, "is")
        content = _text(inline, _SHEET_NS) if inline is not None else ""
    elif cell_type == "b":
        if raw not in {"0", "1", ""}:
            raise ArtifactError("office_invalid_cell")
        content = {"0": "FALSE", "1": "TRUE", "": ""}[raw]
    elif cell_type in {"n", "str", "e", "d"}:
        content = raw
    else:
        raise ArtifactError("office_invalid_cell")
    if formula is not None:
        return f"公式（缓存值，未重新计算）：{content}", "cached"
    return content, None


def _excel(package: _Package, main: str, output: _Output) -> ParsedContent:
    root = package.xml(main)
    _require_root(root, "workbook", _SHEET_NS)
    sheets = _child(root, "sheets")
    if sheets is None:
        raise ArtifactError("office_invalid_structure")
    listed = _children(sheets, "sheet")
    if len(listed) > package.limits.max_pages:
        raise ArtifactError("office_page_limit_exceeded")
    strings = _shared_strings(package, main)
    cell_count = 0
    seen_names: set[str] = set()
    for index, sheet in enumerate(listed, 1):
        name = sheet.get("name", "")
        identifier = _attribute(sheet, "id")
        if not name or name in seen_names or len(name) > 128 or not identifier:
            raise ArtifactError("office_invalid_structure")
        seen_names.add(name)
        path = package.target(main, identifier, "worksheet")
        worksheet = package.xml(path)
        _require_root(worksheet, "worksheet", _SHEET_NS)
        data = _child(worksheet, "sheetData")
        if data is None:
            continue
        seen_cells: set[str] = set()
        seen_rows: set[int] = set()
        previous_row = 0
        for row in _children(data, "row"):
            row_value = row.get("r", str(previous_row + 1))
            if not row_value.isascii() or not row_value.isdigit() or len(row_value) > 7:
                raise ArtifactError("office_invalid_cell")
            row_number = int(row_value)
            if row_number in seen_rows or not 1 <= row_number <= 1048576:
                raise ArtifactError("office_invalid_cell")
            seen_rows.add(row_number)
            previous_row = row_number
            previous_column = 0
            for cell in _children(row, "c"):
                cell_count += 1
                if cell_count > package.limits.max_cells:
                    raise ArtifactError("office_cell_limit_exceeded")
                reference = cell.get("r", f"{_column_name(previous_column + 1)}{row_number}")
                column, actual_row = _cell_position(reference)
                if actual_row != row_number or reference in seen_cells:
                    raise ArtifactError("office_invalid_cell")
                seen_cells.add(reference)
                previous_column = column
                content, formula_status = _cell_value(cell, strings)
                extra: dict[str, JsonValue] = {"cell_type": cell.get("t", "n")}
                if formula_status:
                    extra["formula_result"] = formula_status
                output.append(
                    content,
                    {"kind": "cell", "sheet": name, "sheet_index": index, "cell": reference},
                    **extra,
                )
    return output.finish(
        package,
        sheet_count=len(listed),
        cell_count=cell_count,
        numeric_values="raw_unformatted",
        formulas_evaluated=False,
    )


def _slide_text(root: Element, output: _Output, slide: int, *, notes: bool = False) -> None:
    paragraph = 0
    table = 0
    pending: list[tuple[Element, dict[str, JsonValue]]] = [(root, {})]
    while pending:
        element, context = pending.pop()
        name, namespace = _local(element.tag), _namespace(element.tag)
        if name == "p" and namespace in _DRAW_NS:
            paragraph += 1
            output.append(
                _text(element, _DRAW_NS),
                {
                    "kind": "notes" if notes else "slide",
                    "slide": slide,
                    "paragraph": paragraph,
                    **context,
                },
            )
            continue
        children = [(child, context) for child in element]
        if name == "tbl" and namespace in _DRAW_NS:
            table += 1
            children = [
                (row, {**context, "table": table, "row": index})
                for index, row in enumerate(_children(element, "tr"), 1)
            ]
        elif name == "tr" and namespace in _DRAW_NS:
            children = [
                (cell, {**context, "column": index})
                for index, cell in enumerate(_children(element, "tc"), 1)
            ]
        pending.extend(reversed(children))


def _powerpoint(package: _Package, main: str, output: _Output) -> ParsedContent:
    root = package.xml(main)
    _require_root(root, "presentation", _SLIDE_NS)
    slides = _child(root, "sldIdLst")
    listed = _children(slides, "sldId") if slides is not None else []
    if len(listed) > package.limits.max_pages:
        raise ArtifactError("office_page_limit_exceeded")
    seen: set[str] = set()
    for index, slide in enumerate(listed, 1):
        identifier = slide.get(f"{{{_REL}}}id") or slide.get(f"{{{_STRICT_REL}}}id")
        if not identifier or identifier in seen:
            raise ArtifactError("office_invalid_structure")
        seen.add(identifier)
        path = package.target(main, identifier, "slide")
        slide_root = package.xml(path)
        _require_root(slide_root, "sld", _SLIDE_NS)
        _slide_text(slide_root, output, index)
        notes = package.kind_targets(path, "notesSlide")
        if len(notes) > 1:
            raise ArtifactError("office_invalid_structure")
        if notes:
            notes_root = package.xml(notes[0])
            _require_root(notes_root, "notes", _SLIDE_NS)
            _slide_text(notes_root, output, index, notes=True)
    return output.finish(package, slide_count=len(listed))


def parse_office(path: Path, source_type: SourceType, limits: ParseLimits) -> ParsedContent:
    """Return bounded text and source locators, or a stable non-sensitive error code."""
    parsers = {SourceType.WORD: _word, SourceType.EXCEL: _excel, SourceType.PPT: _powerpoint}
    if source_type not in parsers:
        raise ArtifactError("office_unsupported_type")
    try:
        if path.stat().st_size > limits.max_input_bytes:
            raise ArtifactError("office_input_limit_exceeded")
        with ZipFile(path) as archive:
            package = _Package(archive, limits)
            main = package.kind_targets("", "officeDocument")
            if len(main) != 1:
                raise ArtifactError("office_invalid_structure")
            return parsers[source_type](package, main[0], _Output(source_type, limits))
    except ArtifactError:
        raise
    except (
        BadZipFile,
        OSError,
        RuntimeError,
        NotImplementedError,
        ValueError,
        OverflowError,
        ZlibError,
    ):
        raise ArtifactError("office_invalid_archive") from None
