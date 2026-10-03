"""Synthetic parser fixtures; no customer documents."""

from pathlib import Path

from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject


def text_pdf(path: Path, *, pages: int = 1) -> None:
    writer = PdfWriter()
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    for index in range(pages):
        page = writer.add_blank_page(width=600, height=800)
        page[NameObject("/Resources")] = DictionaryObject(
            {NameObject("/Font"): DictionaryObject({NameObject("/F1"): font})}
        )
        content = DecodedStreamObject()
        content.set_data(
            f"BT /F1 18 Tf 30 740 Td (Personal knowledge page {index + 1}) Tj ET".encode()
        )
        page[NameObject("/Contents")] = writer._add_object(content)
    writer.add_metadata({"/Title": "Synthetic knowledge"})
    writer.write(path)
