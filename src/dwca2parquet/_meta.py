"""
Parsing of DwC-A meta.xml descriptor files.

Produces an ArchiveDescriptor describing the structure of the archive: which
data files are present, their CSV format details, and the fields (with terms,
column indices, and optional default values) declared for each file.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field


@dataclass
class FieldDescriptor:
    """
    Description of a single field declared in meta.xml.

    Attributes
    ----------
    term : str
        Full Darwin Core term URI
        (e.g. "http://rs.tdwg.org/dwc/terms/scientificName").
    index : int or None
        0-based column index in the CSV file. None for default-only fields
        (fields that have a default value but no corresponding CSV column).
    default : str or None
        Default value to use when the field is absent or empty. None if no
        default is declared.
    """

    term: str
    index: int | None
    default: str | None


@dataclass
class FileDescriptor:
    """
    Description of a single data file (core or extension) from meta.xml.

    Attributes
    ----------
    filename : str
        Filename of the CSV data file inside the archive.
    row_type : str
        Full rowType URI identifying the type of records in this file
        (e.g. "http://rs.tdwg.org/dwc/terms/Occurrence").
    encoding : str
        Character encoding of the CSV file (e.g. "utf-8").
    fields_terminated_by : str
        Field delimiter character (e.g. "\\t" or ",").
    fields_enclosed_by : str
        Quote character used to enclose fields (empty string if none).
    lines_terminated_by : str
        Line terminator (e.g. "\\n" or "\\r\\n").
    ignore_header_lines : int
        Number of header lines to skip at the start of the CSV file.
    id_index : int or None
        Column index of the record identifier (id for core, None for extensions).
    coreid_index : int or None
        Column index of the foreign key linking to the core record
        (coreid for extensions, None for core).
    fields : list of FieldDescriptor
        All fields declared for this file, in order of declaration.
    is_core : bool
        True if this is the core file, False for extensions.
    """

    filename: str
    row_type: str
    encoding: str
    fields_terminated_by: str
    fields_enclosed_by: str
    lines_terminated_by: str
    ignore_header_lines: int
    id_index: int | None
    coreid_index: int | None
    fields: list[FieldDescriptor]
    is_core: bool


@dataclass
class ArchiveDescriptor:
    """
    Full description of a DwC-A archive, parsed from meta.xml.

    Attributes
    ----------
    core : FileDescriptor
        Descriptor for the core data file.
    extensions : list of FileDescriptor
        Descriptors for any extension data files (may be empty).
    metadata_filename : str or None
        Filename of the archive-level metadata file (e.g. "eml.xml"), or None
        if not declared in meta.xml.
    """

    core: FileDescriptor
    extensions: list[FileDescriptor] = field(default_factory=list)
    metadata_filename: str | None = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _strip_namespace(xml_text: str) -> str:
    """Remove the default XML namespace declaration so XPath queries work without prefixes."""
    return re.sub(r' xmlns="[^"]+"', "", xml_text, count=1)


def _decode_attribute(element: ET.Element, name: str, default: str, encoding: str = "utf-8") -> str:
    """
    Read an XML attribute and decode escape sequences (e.g. \\t, \\n).

    Parameters
    ----------
    element : ET.Element
        The XML element to read from.
    name : str
        Attribute name.
    default : str
        Value to return if the attribute is absent.
    encoding : str
        Encoding to use when decoding the raw attribute bytes.

    Returns
    -------
    str
        The decoded attribute value, or default if not present.
    """
    raw = element.get(name)
    if raw is None:
        return default
    return bytes(raw, encoding).decode("unicode-escape")


def _parse_file_descriptor(section: ET.Element, is_core: bool) -> FileDescriptor:
    """Parse a <core> or <extension> element into a FileDescriptor."""
    encoding = section.get("encoding", "utf-8")

    fields_terminated_by = _decode_attribute(section, "fieldsTerminatedBy", "\t", encoding)
    fields_enclosed_by = _decode_attribute(section, "fieldsEnclosedBy", "", encoding)
    lines_terminated_by = _decode_attribute(section, "linesTerminatedBy", "\n", encoding)
    ignore_header_lines = int(section.get("ignoreHeaderLines", "0"))
    row_type = section.get("rowType", "")

    location_el = section.find("location")
    filename = location_el.text.strip() if location_el is not None and location_el.text else ""

    # id (core) or coreid (extension)
    id_index: int | None = None
    coreid_index: int | None = None
    if is_core:
        id_el = section.find("id")
        if id_el is not None:
            id_index = int(id_el.get("index", "0"))
    else:
        coreid_el = section.find("coreid")
        if coreid_el is not None:
            coreid_index = int(coreid_el.get("index", "0"))

    fields = []
    for field_el in section.findall("field"):
        term = field_el.get("term", "")
        index_attr = field_el.get("index")
        index = int(index_attr) if index_attr is not None else None
        default = field_el.get("default", None)
        fields.append(FieldDescriptor(term=term, index=index, default=default))

    return FileDescriptor(
        filename=filename,
        row_type=row_type,
        encoding=encoding,
        fields_terminated_by=fields_terminated_by,
        fields_enclosed_by=fields_enclosed_by,
        lines_terminated_by=lines_terminated_by,
        ignore_header_lines=ignore_header_lines,
        id_index=id_index,
        coreid_index=coreid_index,
        fields=fields,
        is_core=is_core,
    )


def parse_meta_xml(xml_text: str) -> ArchiveDescriptor:
    """
    Parse the content of a meta.xml file into an ArchiveDescriptor.

    Parameters
    ----------
    xml_text : str
        Raw XML content of the meta.xml file.

    Returns
    -------
    ArchiveDescriptor
        Fully populated descriptor for the archive.
    """
    xml_text = _strip_namespace(xml_text)
    root = ET.fromstring(xml_text)

    metadata_filename = root.get("metadata", None)

    core_el = root.find("core")
    core = _parse_file_descriptor(core_el, is_core=True)

    extensions = [
        _parse_file_descriptor(ext_el, is_core=False)
        for ext_el in root.findall("extension")
    ]

    return ArchiveDescriptor(
        core=core,
        extensions=extensions,
        metadata_filename=metadata_filename,
    )
