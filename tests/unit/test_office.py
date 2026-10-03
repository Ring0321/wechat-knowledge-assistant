"""Real, minimal OOXML packages exercise extraction and hostile package boundaries."""

import struct
import warnings
from pathlib import Path
from zipfile import ZIP_BZIP2, ZIP_DEFLATED, ZIP_STORED, ZipFile, ZipInfo

import pytest

from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceType
from app.domain.parsing import ParseLimits
from app.parsers.office import parse_office

W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
S = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
P = "http://schemas.openxmlformats.org/presentationml/2006/main"
A = "http://schemas.openxmlformats.org/drawingml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG = "http://schemas.openxmlformats.org/package/2006/relationships"
LIMITS = ParseLimits()


def rels(*items: tuple[str, str, str, str]) -> str:
    return (
        f'<Relationships xmlns="{PKG}">'
        + "".join(
            f'<Relationship Id="{identifier}" Type="{R}/{kind}" '
            f'Target="{target}" TargetMode="{mode}"/>'
            for identifier, kind, target, mode in items
        )
        + "</Relationships>"
    )


def package(main: str, body: str, **extra: str) -> dict[str, str]:
    content_type = {
        "word/document.xml": "wordprocessingml.document",
        "xl/workbook.xml": "spreadsheetml.sheet",
        "ppt/presentation.xml": "presentationml.presentation",
    }[main]
    return {
        "[Content_Types].xml": (
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.'
            'relationships+xml"/>'
            f'<Override PartName="/{main}" '
            'ContentType="application/vnd.openxmlformats-officedocument.'
            f'{content_type}.main+xml"/></Types>'
        ),
        "_rels/.rels": rels(("main", "officeDocument", main, "Internal")),
        main: body,
        **extra,
    }


def docx(content: str = "Hello") -> dict[str, str]:
    return package(
        "word/document.xml",
        f'<w:document xmlns:w="{W}"><w:body><w:p><w:r><w:t>{content}</w:t>'
        "</w:r></w:p></w:body></w:document>",
    )


def xlsx(cells: str = '<c r="A1"><v>42</v></c>', *, rows: str | None = None) -> dict[str, str]:
    body = rows if rows is not None else '<row r="1">' + cells + "</row>"
    return package(
        "xl/workbook.xml",
        f'<workbook xmlns="{S}" xmlns:r="{R}"><sheets>'
        '<sheet name="数据" sheetId="1" r:id="s1"/></sheets></workbook>',
        **{
            "xl/_rels/workbook.xml.rels": rels(
                ("s1", "worksheet", "worksheets/data.xml", "Internal")
            ),
            "xl/worksheets/data.xml": (
                f'<worksheet xmlns="{S}"><dimension ref="A1:XFD1048576"/>'
                f"<sheetData>{body}</sheetData></worksheet>"
            ),
        },
    )


def pptx() -> dict[str, str]:
    return package(
        "ppt/presentation.xml",
        f'<p:presentation xmlns:p="{P}" xmlns:r="{R}"><p:sldIdLst>'
        '<p:sldId id="256" r:id="s2"/><p:sldId id="257" r:id="s1"/>'
        "</p:sldIdLst></p:presentation>",
        **{
            "ppt/_rels/presentation.xml.rels": rels(
                ("s1", "slide", "slides/slide1.xml", "Internal"),
                ("s2", "slide", "slides/slide2.xml", "Internal"),
            ),
            "ppt/slides/slide1.xml": (
                f'<p:sld xmlns:p="{P}" xmlns:a="{A}"><p:cSld><p:spTree><p:sp>'
                "<p:txBody><a:p><a:r><a:t>第二页</a:t></a:r></a:p></p:txBody>"
                "</p:sp></p:spTree></p:cSld></p:sld>"
            ),
            "ppt/slides/slide2.xml": (
                f'<p:sld xmlns:p="{P}" xmlns:a="{A}"><p:cSld><p:spTree><p:sp>'
                "<p:txBody><a:p><a:r><a:t>第一页</a:t></a:r></a:p></p:txBody></p:sp>"
                "<p:graphicFrame><a:graphic><a:graphicData><a:tbl><a:tr><a:tc>"
                "<a:txBody><a:p><a:r><a:t>表格正文</a:t></a:r></a:p></a:txBody></a:tc>"
                "</a:tr></a:tbl></a:graphicData></a:graphic></p:graphicFrame>"
                "</p:spTree></p:cSld></p:sld>"
            ),
            "ppt/slides/_rels/slide2.xml.rels": rels(
                ("n1", "notesSlide", "../notesSlides/notesSlide9.xml", "Internal")
            ),
            "ppt/notesSlides/notesSlide9.xml": (
                f'<p:notes xmlns:p="{P}" xmlns:a="{A}"><p:cSld><p:spTree><p:sp>'
                "<p:txBody><a:p><a:r><a:t>讲者备注</a:t></a:r></a:p></p:txBody>"
                "</p:sp></p:spTree></p:cSld></p:notes>"
            ),
        },
    )


def write(tmp_path: Path, entries: dict[str, str], compression: int = ZIP_STORED) -> Path:
    path = tmp_path / "input.ooxml"
    with ZipFile(path, "w", compression=compression) as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
    return path


def assert_error(
    path: Path,
    expected: str,
    source_type: SourceType = SourceType.WORD,
    limits: ParseLimits = LIMITS,
) -> None:
    with pytest.raises(ArtifactError) as caught:
        parse_office(path, source_type, limits)
    assert caught.value.code == expected
    assert str(caught.value) == expected
    assert caught.value.retryable is False


def test_docx_text_headings_tables_and_title(tmp_path: Path) -> None:
    entries = package(
        "word/document.xml",
        f'<w:document xmlns:w="{W}"><w:body><w:p><w:pPr>'
        '<w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>章标题</w:t></w:r></w:p>'
        "<w:p><w:r><w:t>正文</w:t><w:tab/><w:t>内容</w:t><w:br/>"
        "<w:t>下一行</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r>"
        "<w:t>表格 A</w:t></w:r></w:p></w:tc><w:tc><w:p><w:r><w:t>表格 B</w:t>"
        "</w:r></w:p></w:tc></w:tr></w:tbl></w:body></w:document>",
        **{
            "docProps/core.xml": (
                '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/'
                '2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/">'
                "<dc:title>报告名称</dc:title></cp:coreProperties>"
            )
        },
    )
    result = parse_office(write(tmp_path, entries), SourceType.WORD, LIMITS)
    assert result.title == "报告名称"
    assert result.text == "章标题\n\n正文\t内容\n下一行\n\n表格 A\n\n表格 B"
    assert result.segments[0]["paragraph_style"] == "Heading1"
    assert result.segments[0]["heading_level"] == 1
    assert result.segments[1]["heading_path"] == ["章标题"]
    assert result.segments[2]["locator"] == {
        "kind": "paragraph",
        "paragraph": 3,
        "table": 1,
        "row": 1,
        "column": 1,
    }
    assert result.metadata["paragraph_count"] == 4
    assert result.metadata["parse_status"] == "normalized"
    assert result.metadata["parser"] == "ooxml-1"
    assert result.model_dump_json()


@pytest.mark.parametrize("content", ["", "   "])
def test_empty_document_is_explicit(tmp_path: Path, content: str) -> None:
    result = parse_office(write(tmp_path, docx(content)), SourceType.WORD, LIMITS)
    assert result.text == ""
    assert result.metadata["parse_status"] == "empty"
    assert result.segments == []


def test_strict_namespace_docx(tmp_path: Path) -> None:
    entries = {
        name: value.replace(W, "http://purl.oclc.org/ooxml/wordprocessingml/main").replace(
            R, "http://purl.oclc.org/ooxml/officeDocument/relationships"
        )
        for name, value in docx().items()
    }
    assert parse_office(write(tmp_path, entries), SourceType.WORD, LIMITS).text == "Hello"


def test_xlsx_shared_strings_inline_cache_and_missing_formula(tmp_path: Path) -> None:
    entries = xlsx(
        '<c r="A1" t="s"><v>0</v></c><c r="B1" t="inlineStr"><is><r><t>内联</t></r>'
        '<r><t>文本</t></r></is></c><c r="C1"><f>1+2</f><v>3</v></c>'
        '<c r="D1"><f>WEBSERVICE(&quot;http://127.0.0.1/&quot;)</f></c>'
        '<c r="E1" t="b"><v>1</v></c><c r="F1" t="e"><v>#DIV/0!</v></c>'
    )
    entries["xl/_rels/workbook.xml.rels"] = rels(
        ("s1", "worksheet", "worksheets/data.xml", "Internal"),
        ("shared", "sharedStrings", "sharedStrings.xml", "Internal"),
    )
    entries["xl/sharedStrings.xml"] = (
        f'<sst xmlns="{S}"><si><r><t>共享</t></r><r><t>内容</t></r></si></sst>'
    )
    result = parse_office(write(tmp_path, entries), SourceType.EXCEL, LIMITS)
    assert (
        result.text == "共享内容\n\n内联文本\n\n公式（缓存值，未重新计算）：3\n\n"
        "公式（无缓存结果，未计算）\n\nTRUE\n\n#DIV/0!"
    )
    assert result.segments[0]["locator"] == {
        "kind": "cell",
        "sheet": "数据",
        "sheet_index": 1,
        "cell": "A1",
    }
    assert result.segments[2]["formula_result"] == "cached"
    assert result.segments[3]["formula_result"] == "missing"
    assert result.metadata["formulas_evaluated"] is False
    assert "127.0.0.1" not in result.model_dump_json()


def test_xlsx_never_expands_declared_dimensions(tmp_path: Path) -> None:
    result = parse_office(write(tmp_path, xlsx()), SourceType.EXCEL, ParseLimits(max_cells=1))
    assert result.text == "42"
    assert result.metadata["cell_count"] == 1


def test_xlsx_infers_missing_cell_coordinates_without_expanding_sparse_cells(
    tmp_path: Path,
) -> None:
    entries = xlsx(
        rows="<row><c><v>first</v></c><c><v>second</v></c></row>"
        '<row r="1048576"><c r="XFD1048576"><v>last</v></c></row>'
    )
    result = parse_office(write(tmp_path, entries), SourceType.EXCEL, LIMITS)
    assert [segment["locator"]["cell"] for segment in result.segments] == ["A1", "B1", "XFD1048576"]


def test_pptx_respects_slide_order_tables_and_related_notes(tmp_path: Path) -> None:
    result = parse_office(write(tmp_path, pptx()), SourceType.PPT, LIMITS)
    assert result.text == "第一页\n\n表格正文\n\n讲者备注\n\n第二页"
    assert result.segments[1]["locator"] == {
        "kind": "slide",
        "slide": 1,
        "paragraph": 2,
        "table": 1,
        "row": 1,
        "column": 1,
    }
    assert result.segments[2]["locator"] == {"kind": "notes", "slide": 1, "paragraph": 1}
    assert result.metadata["slide_count"] == 2


def test_external_hyperlinks_are_ignored_without_fetch(tmp_path: Path) -> None:
    entries = docx("safe text")
    entries["word/_rels/document.xml.rels"] = rels(
        ("link", "hyperlink", "http://127.0.0.1/admin", "External")
    )
    result = parse_office(write(tmp_path, entries), SourceType.WORD, LIMITS)
    assert result.text == "safe text"
    assert result.metadata["external_relationships_ignored"] == 1
    assert "127.0.0.1" not in result.model_dump_json()


def test_external_required_part_is_rejected(tmp_path: Path) -> None:
    entries = xlsx()
    entries["xl/_rels/workbook.xml.rels"] = rels(
        ("s1", "worksheet", "http://127.0.0.1/data", "External")
    )
    assert_error(write(tmp_path, entries), "office_external_required_part", SourceType.EXCEL)


@pytest.mark.parametrize(
    "target",
    [
        "../../../secret.xml",
        "https://example.com/data.xml",
        "//127.0.0.1/data.xml",
        "a.xml?token=secret",
        "a.xml#frag",
        "..%2f..%2f..%2fsecret.xml",
        "..\\secret.xml",
    ],
)
def test_invalid_internal_relationships(tmp_path: Path, target: str) -> None:
    entries = docx()
    entries["word/_rels/document.xml.rels"] = rels(("link", "hyperlink", target, "Internal"))
    assert_error(write(tmp_path, entries), "office_invalid_relationship")


@pytest.mark.parametrize(
    "target", ["word/document.xml", "[Content_Types].xml", "_rels/.rels", "docProps/core.xml"]
)
def test_dtd_entities_are_rejected_in_interpreted_parts(tmp_path: Path, target: str) -> None:
    entries = docx()
    entries[target] = (
        '<!DOCTYPE root [<!ENTITY secret SYSTEM "file:///sensitive.txt">]><root>&secret;</root>'
    )
    assert_error(write(tmp_path, entries), "office_invalid_xml")


@pytest.mark.parametrize(
    "extra",
    [
        {"word/vbaProject.bin": "opaque macro"},
        {
            "[Content_Types].xml": (
                '<Types><Default ContentType="application/vnd.ms-word.document.'
                'macroEnabled.main+xml"/></Types>'
            )
        },
    ],
)
def test_macro_packages_are_rejected(tmp_path: Path, extra: dict[str, str]) -> None:
    assert_error(write(tmp_path, {**docx(), **extra}), "office_macros_not_supported")


@pytest.mark.parametrize(
    "name", ["../outside.xml", "/absolute.xml", "word/../outside.xml", "word\\outside.xml"]
)
def test_archive_path_attacks_are_rejected(tmp_path: Path, name: str) -> None:
    path = write(tmp_path, {**docx(), name.replace("\\", "/"): "unused"})
    if "\\" in name:
        path.write_bytes(path.read_bytes().replace(b"word/outside.xml", b"word\\outside.xml"))
    assert_error(path, "office_invalid_archive_path")
    assert not (tmp_path / "outside.xml").exists()


def test_archive_symlink_is_rejected(tmp_path: Path) -> None:
    path = write(tmp_path, docx())
    entry = ZipInfo("word/link")
    entry.create_system = 3
    entry.external_attr = 0o120777 << 16
    with ZipFile(path, "a") as archive:
        archive.writestr(entry, "target")
    assert_error(path, "office_invalid_archive_path")


def test_duplicate_archive_members_are_rejected(tmp_path: Path) -> None:
    path = write(tmp_path, docx())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with ZipFile(path, "a") as archive:
            archive.writestr("word/document.xml", "duplicate")
    assert_error(path, "office_invalid_archive_path")


def test_encrypted_archive_flag_is_rejected(tmp_path: Path) -> None:
    path = write(tmp_path, docx())
    data = bytearray(path.read_bytes())
    local = data.index(b"PK\x03\x04")
    central = data.index(b"PK\x01\x02")
    struct.pack_into("<H", data, local + 6, struct.unpack_from("<H", data, local + 6)[0] | 1)
    struct.pack_into("<H", data, central + 8, struct.unpack_from("<H", data, central + 8)[0] | 1)
    path.write_bytes(data)
    assert_error(path, "office_encrypted")


def test_input_limit(tmp_path: Path) -> None:
    path = write(tmp_path, docx("x" * 1024))
    assert_error(path, "office_input_limit_exceeded", limits=ParseLimits(max_input_bytes=1024))


def test_total_expanded_bytes_limit(tmp_path: Path) -> None:
    path = write(tmp_path, docx("x" * 1024))
    assert_error(path, "office_archive_bytes_exceeded", limits=ParseLimits(max_archive_bytes=1024))


def test_archive_member_limit(tmp_path: Path) -> None:
    assert_error(
        write(tmp_path, docx()),
        "office_archive_entries_exceeded",
        limits=ParseLimits(max_archive_entries=2),
    )


def test_compression_bomb_rejected(tmp_path: Path) -> None:
    assert_error(
        write(tmp_path, docx("x" * 200000), ZIP_DEFLATED), "office_compression_ratio_exceeded"
    )


def test_unsupported_compression(tmp_path: Path) -> None:
    assert_error(write(tmp_path, docx(), ZIP_BZIP2), "office_unsupported_compression")


def test_text_limit_rejects_instead_of_truncating(tmp_path: Path) -> None:
    assert_error(
        write(tmp_path, docx("x" * 101)),
        "office_text_limit_exceeded",
        limits=ParseLimits(max_text_chars=100),
    )


def test_text_limit_includes_join_separators(tmp_path: Path) -> None:
    entries = docx("x" * 49)
    entries["word/document.xml"] = entries["word/document.xml"].replace(
        "</w:body>", f"<w:p><w:r><w:t>{'y' * 50}</w:t></w:r></w:p></w:body>"
    )
    assert_error(
        write(tmp_path, entries),
        "office_text_limit_exceeded",
        limits=ParseLimits(max_text_chars=100),
    )


def test_cell_limit_counts_empty_cells(tmp_path: Path) -> None:
    assert_error(
        write(tmp_path, xlsx('<c r="A1"/><c r="B1"/>')),
        "office_cell_limit_exceeded",
        SourceType.EXCEL,
        ParseLimits(max_cells=1),
    )


def test_slide_limit(tmp_path: Path) -> None:
    assert_error(
        write(tmp_path, pptx()),
        "office_page_limit_exceeded",
        SourceType.PPT,
        ParseLimits(max_pages=1),
    )


@pytest.mark.parametrize(
    "cell",
    [
        '<c r="XFE1"/>',
        '<c r="A1048577"/>',
        '<c r="A0"/>',
        '<c r="a1"/>',
        '<c r="A2"/>',
        '<c r="A1"/><c r="A1"/>',
        '<c r="A1" t="s"><v>1</v></c>',
        '<c r="A1" t="s"><v>-1</v></c>',
        '<c r="A1" t="b"><v>4</v></c>',
    ],
)
def test_invalid_cells_are_not_silently_accepted(tmp_path: Path, cell: str) -> None:
    code = "office_invalid_shared_string" if 't="s"' in cell else "office_invalid_cell"
    assert_error(write(tmp_path, xlsx(cell)), code, SourceType.EXCEL)


def test_deep_xml_is_bounded(tmp_path: Path) -> None:
    entries = docx()
    entries["word/document.xml"] = "<x>" * 130 + "</x>" * 130
    assert_error(write(tmp_path, entries), "office_structure_limit_exceeded")


def test_invalid_xml(tmp_path: Path) -> None:
    entries = docx()
    entries["word/document.xml"] = "<broken>"
    assert_error(write(tmp_path, entries), "office_invalid_xml")


@pytest.mark.parametrize("body", [b"not a zip", b"PK\x03\x04", b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"])
def test_damaged_or_legacy_files_have_safe_errors(tmp_path: Path, body: bytes) -> None:
    path = tmp_path / "secret-name.docx"
    path.write_bytes(body)
    assert_error(path, "office_invalid_archive")


def test_missing_required_part(tmp_path: Path) -> None:
    entries = docx()
    del entries["word/document.xml"]
    assert_error(write(tmp_path, entries), "office_missing_part")


def test_source_type_cannot_misclassify_a_package(tmp_path: Path) -> None:
    assert_error(write(tmp_path, docx()), "office_invalid_structure", SourceType.EXCEL)


def test_unsupported_source_type(tmp_path: Path) -> None:
    assert_error(write(tmp_path, docx()), "office_unsupported_type", SourceType.OTHER)


def test_missing_input_has_safe_error(tmp_path: Path) -> None:
    assert_error(tmp_path / "private-user-filename.docx", "office_invalid_archive")


def test_spreadsheet_phonetic_annotation_is_not_cell_content(tmp_path: Path) -> None:
    entries = xlsx(
        '<c r="A1" t="inlineStr"><is><t>東京</t><rPh sb="0" eb="2"><t>とうきょう</t></rPh></is></c>'
    )
    result = parse_office(write(tmp_path, entries), SourceType.EXCEL, LIMITS)
    assert result.text == "東京"


def test_corrupt_compressed_data_has_safe_error(tmp_path: Path) -> None:
    path = write(tmp_path, docx(), ZIP_DEFLATED)
    data = bytearray(path.read_bytes())
    with ZipFile(path) as archive:
        info = archive.getinfo("word/document.xml")
        offset = info.header_offset + 30 + len(info.filename.encode()) + len(info.extra)
    data[offset] = 0xFF
    path.write_bytes(data)
    assert_error(path, "office_invalid_archive")


def test_word_outline_levels_and_heading_hierarchy(tmp_path: Path) -> None:
    entries = docx()
    entries["word/document.xml"] = (
        f'<w:document xmlns:w="{W}"><w:body>'
        '<w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>一章</w:t></w:r></w:p>'
        '<w:p><w:pPr><w:outlineLvl w:val="1"/></w:pPr><w:r><w:t>一节</w:t></w:r></w:p>'
        "<w:p><w:r><w:t>内容</w:t></w:r></w:p>"
        '<w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>二章</w:t></w:r></w:p>'
        "</w:body></w:document>"
    )
    result = parse_office(write(tmp_path, entries), SourceType.WORD, LIMITS)
    assert result.segments[2]["heading_path"] == ["一章", "一节"]
    assert result.segments[3]["heading_path"] == ["二章"]


def test_word_empty_paragraphs_are_bounded(tmp_path: Path) -> None:
    entries = docx()
    entries["word/document.xml"] = (
        f'<w:document xmlns:w="{W}"><w:body><w:p/><w:p/></w:body></w:document>'
    )
    assert_error(
        write(tmp_path, entries),
        "office_structure_limit_exceeded",
        limits=ParseLimits(max_cells=1),
    )


def test_sheet_limit_applies_before_loading_parts(tmp_path: Path) -> None:
    entries = xlsx()
    entries["xl/workbook.xml"] = entries["xl/workbook.xml"].replace(
        "</sheets>", '<sheet name="二页" sheetId="2" r:id="missing"/></sheets>'
    )
    assert_error(
        write(tmp_path, entries),
        "office_page_limit_exceeded",
        SourceType.EXCEL,
        ParseLimits(max_pages=1),
    )


def test_shared_string_expansion_is_bounded(tmp_path: Path) -> None:
    entries = xlsx('<c r="A1" t="s"><v>0</v></c>')
    entries["xl/_rels/workbook.xml.rels"] = rels(
        ("s1", "worksheet", "worksheets/data.xml", "Internal"),
        ("shared", "sharedStrings", "sharedStrings.xml", "Internal"),
    )
    entries["xl/sharedStrings.xml"] = f'<sst xmlns="{S}"><si><t>{"x" * 101}</t></si></sst>'
    assert_error(
        write(tmp_path, entries),
        "office_shared_strings_limit_exceeded",
        SourceType.EXCEL,
        ParseLimits(max_text_chars=100),
    )


def test_formula_shared_string_without_cached_value_is_explicit(tmp_path: Path) -> None:
    entries = xlsx('<c r="A1" t="s"><f>UNKNOWN()</f></c>')
    result = parse_office(write(tmp_path, entries), SourceType.EXCEL, LIMITS)
    assert result.text == "公式（无缓存结果，未计算）"
