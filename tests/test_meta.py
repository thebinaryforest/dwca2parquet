"""
Tests for meta.xml parsing (_meta.py).

parse_meta_xml() reads the XML descriptor found in every DwC-A archive and
returns an ArchiveDescriptor that drives the rest of the conversion pipeline.
These tests verify that all meta.xml attributes are extracted correctly,
including the many optional ones that must fall back to DwC-A spec defaults.

Test data comes from real-world DwC-A archives copied into tests/fixtures/.
Where a fixture archive does not exist for a particular edge case, a minimal
inline XML string is used instead.
"""

import zipfile
from pathlib import Path

from dwca2parquet._meta import (
    ArchiveDescriptor,
    FieldDescriptor,
    FileDescriptor,
    parse_meta_xml,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES = Path(__file__).parent / "fixtures"


def meta_xml_from_zip(name: str) -> str:
    """Return the raw meta.xml text from a fixture zip file.

    Handles archives where all files are wrapped in a single subdirectory
    (a common real-world variation).
    """
    path = FIXTURES / name
    with zipfile.ZipFile(path) as z:
        candidates = [n for n in z.namelist() if n.endswith("meta.xml")]
        assert candidates, f"No meta.xml found in {name}"
        return z.read(candidates[0]).decode("utf-8")


def meta_xml_from_dir(name: str) -> str:
    """Return the raw meta.xml text from an unzipped fixture directory."""
    path = FIXTURES / name / "meta.xml"
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Basic structure
# ---------------------------------------------------------------------------

class TestBasicStructure:
    """parse_meta_xml() returns the right top-level objects."""

    def test_returns_archive_descriptor(self):
        """Result is an ArchiveDescriptor instance."""
        xml = meta_xml_from_zip("dwca-simple-test-archive.zip")
        result = parse_meta_xml(xml)
        assert isinstance(result, ArchiveDescriptor)

    def test_core_is_file_descriptor(self):
        """The core attribute holds a FileDescriptor."""
        xml = meta_xml_from_zip("dwca-simple-test-archive.zip")
        result = parse_meta_xml(xml)
        assert isinstance(result.core, FileDescriptor)

    def test_core_is_marked_as_core(self):
        """FileDescriptor.is_core is True for the core file."""
        xml = meta_xml_from_zip("dwca-simple-test-archive.zip")
        result = parse_meta_xml(xml)
        assert result.core.is_core is True

    def test_no_extensions_when_absent(self):
        """extensions is an empty list when the archive has no <extension> elements."""
        xml = meta_xml_from_zip("dwca-simple-test-archive.zip")
        result = parse_meta_xml(xml)
        assert result.extensions == []

    def test_metadata_filename_present(self):
        """metadata_filename is read from the metadata= attribute on <archive>."""
        xml = meta_xml_from_zip("dwca-simple-test-archive.zip")
        result = parse_meta_xml(xml)
        assert result.metadata_filename == "eml.xml"

    def test_metadata_filename_absent(self):
        """metadata_filename is None when the attribute is missing."""
        xml = meta_xml_from_zip("dwca-nometadata.zip")
        result = parse_meta_xml(xml)
        assert result.metadata_filename is None

    def test_metadata_filename_non_eml(self):
        """metadata_filename reflects any value, not just 'eml.xml'.

        GBIF downloads use 'metadata.xml' rather than 'eml.xml'.
        """
        xml = meta_xml_from_zip("gbif-results.zip")
        result = parse_meta_xml(xml)
        assert result.metadata_filename == "metadata.xml"


# ---------------------------------------------------------------------------
# Core file descriptor
# ---------------------------------------------------------------------------

class TestCoreDescriptor:
    """All attributes of the core FileDescriptor are parsed correctly.

    Uses dwca-simple-test-archive.zip as the baseline fixture: a standard
    tab-delimited occurrence archive with one header line and no defaults.
    """

    def setup_method(self):
        xml = meta_xml_from_zip("dwca-simple-test-archive.zip")
        self.desc = parse_meta_xml(xml).core

    def test_filename(self):
        """CSV filename is read from <files><location>."""
        assert self.desc.filename == "occurrence.txt"

    def test_row_type(self):
        """rowType URI is read from the rowType attribute."""
        assert self.desc.row_type == "http://rs.tdwg.org/dwc/terms/Occurrence"

    def test_encoding(self):
        assert self.desc.encoding == "utf-8"

    def test_fields_terminated_by_tab(self):
        """Escape sequence \\t in the XML attribute is decoded to a real tab."""
        assert self.desc.fields_terminated_by == "\t"

    def test_fields_enclosed_by_empty(self):
        """Empty fieldsEnclosedBy means no quoting."""
        assert self.desc.fields_enclosed_by == ""

    def test_lines_terminated_by_newline(self):
        """Escape sequence \\n is decoded to a real newline."""
        assert self.desc.lines_terminated_by == "\n"

    def test_ignore_header_lines(self):
        """ignoreHeaderLines is parsed as an integer."""
        assert self.desc.ignore_header_lines == 1

    def test_id_index(self):
        """id_index reflects the index attribute of the <id> element."""
        assert self.desc.id_index == 0

    def test_coreid_index_is_none_for_core(self):
        """Core files have no <coreid> element, so coreid_index is None."""
        assert self.desc.coreid_index is None

    def test_field_count(self):
        """All four <field> elements are parsed."""
        assert len(self.desc.fields) == 4

    def test_field_terms(self):
        """Term URIs are preserved exactly as written in the XML."""
        terms = [f.term for f in self.desc.fields]
        assert "http://rs.tdwg.org/dwc/terms/scientificName" in terms
        assert "http://rs.tdwg.org/dwc/terms/basisOfRecord" in terms

    def test_field_indices(self):
        """Column indices are parsed in declaration order."""
        indices = [f.index for f in self.desc.fields]
        assert indices == [1, 2, 3, 4]

    def test_no_defaults_in_simple_archive(self):
        """Fields without a default= attribute have default=None."""
        for f in self.desc.fields:
            assert f.default is None


# ---------------------------------------------------------------------------
# CSV format variations
# ---------------------------------------------------------------------------

class TestCsvFormat:
    """Optional CSV format attributes are read correctly, with correct defaults."""

    def test_quote_char_single_quote(self):
        """A literal quote character in fieldsEnclosedBy is preserved as-is."""
        xml = meta_xml_from_zip("dwca-simple-test-archive-enclosed.zip")
        core = parse_meta_xml(xml).core
        assert core.fields_enclosed_by == "'"

    def test_ignore_header_lines_zero(self):
        """ignoreHeaderLines='0' means no lines are skipped."""
        xml = meta_xml_from_zip("dwca-noheaders-1.zip")
        core = parse_meta_xml(xml).core
        assert core.ignore_header_lines == 0

    def test_ignore_header_lines_absent_defaults_to_zero(self):
        """When ignoreHeaderLines is omitted, it defaults to 0 (DwC-A spec)."""
        xml = meta_xml_from_zip("dwca-noheaders-2.zip")
        core = parse_meta_xml(xml).core
        assert core.ignore_header_lines == 0

    def test_missing_fields_terminated_defaults_to_tab(self):
        """When fieldsTerminatedBy is absent, it defaults to tab (DwC-A spec)."""
        xml = meta_xml_from_dir("dwca-meta-default-values")
        core = parse_meta_xml(xml).core
        assert core.fields_terminated_by == "\t"

    def test_missing_lines_terminated_defaults_to_newline(self):
        """When linesTerminatedBy is absent, it defaults to \\n (DwC-A spec)."""
        xml = meta_xml_from_dir("dwca-meta-default-values")
        core = parse_meta_xml(xml).core
        assert core.lines_terminated_by == "\n"

    def test_uppercase_encoding(self):
        """Encoding values are preserved as-is, including uppercase 'UTF-8'."""
        xml = meta_xml_from_zip("gbif-results.zip")
        core = parse_meta_xml(xml).core
        assert core.encoding == "UTF-8"


# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------

class TestExtensions:
    """Extension FileDescriptors are parsed in the same way as the core.

    Uses dwca-2extensions.zip which has a Taxon core plus Description and
    VernacularName extensions.
    """

    def setup_method(self):
        xml = meta_xml_from_zip("dwca-2extensions.zip")
        self.descriptor = parse_meta_xml(xml)

    def test_two_extensions_found(self):
        """All <extension> elements are parsed."""
        assert len(self.descriptor.extensions) == 2

    def test_extension_is_not_core(self):
        """FileDescriptor.is_core is False for every extension."""
        for ext in self.descriptor.extensions:
            assert ext.is_core is False

    def test_extension_row_types(self):
        """Each extension rowType URI is read correctly."""
        row_types = {e.row_type for e in self.descriptor.extensions}
        assert "http://rs.gbif.org/terms/1.0/Description" in row_types
        assert "http://rs.gbif.org/terms/1.0/VernacularName" in row_types

    def test_extension_filenames(self):
        """Each extension filename comes from its own <files><location> element."""
        filenames = {e.filename for e in self.descriptor.extensions}
        assert "description.txt" in filenames
        assert "vernacularname.txt" in filenames

    def test_extension_coreid_index(self):
        """coreid_index is read from the <coreid index='...'> element."""
        for ext in self.descriptor.extensions:
            assert ext.coreid_index == 0

    def test_extension_id_index_is_none(self):
        """Extensions have no <id> element, so id_index is None."""
        for ext in self.descriptor.extensions:
            assert ext.id_index is None

    def test_extension_fields(self):
        """Fields belonging to an extension are not mixed with the core fields."""
        desc_ext = next(
            e for e in self.descriptor.extensions
            if e.row_type == "http://rs.gbif.org/terms/1.0/Description"
        )
        terms = [f.term for f in desc_ext.fields]
        assert "http://purl.org/dc/terms/description" in terms
        assert "http://purl.org/dc/terms/language" in terms

    def test_single_extension(self):
        """An archive with exactly one extension produces a one-element list."""
        xml = meta_xml_from_zip("dwca-star-test-archive.zip")
        descriptor = parse_meta_xml(xml)
        assert len(descriptor.extensions) == 1
        assert descriptor.extensions[0].row_type == "http://rs.gbif.org/terms/1.0/VernacularName"


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------

class TestDefaultValues:
    """Default values declared in meta.xml are captured in FieldDescriptor.

    DwC-A allows a field to supply a default value for all rows. The field
    may appear in the CSV (index + default) or exist only as a constant with
    no column at all (no index, default only).
    """

    def test_default_only_field_has_none_index(self):
        """A field with default= but no index= has index=None.

        dwca-test-default.zip declares country with default='Belgium' and
        no index, meaning the column is not in the CSV at all.
        """
        xml = meta_xml_from_zip("dwca-test-default.zip")
        core = parse_meta_xml(xml).core
        country = next(
            f for f in core.fields
            if f.term == "http://rs.tdwg.org/dwc/terms/country"
        )
        assert country.index is None
        assert country.default == "Belgium"

    def test_default_only_field_still_listed_in_fields(self):
        """Default-only fields appear in the fields list despite having no CSV column."""
        xml = meta_xml_from_zip("dwca-test-default.zip")
        core = parse_meta_xml(xml).core
        terms = [f.term for f in core.fields]
        assert "http://rs.tdwg.org/dwc/terms/country" in terms

    def test_field_with_index_and_default(self):
        """A field can have both an index and a default (used as fallback for empty cells).

        dwca-partial-default.zip declares country at index=5 with default='Belgium'.
        """
        xml = meta_xml_from_zip("dwca-partial-default.zip")
        core = parse_meta_xml(xml).core
        country = next(
            f for f in core.fields
            if f.term == "http://rs.tdwg.org/dwc/terms/country"
        )
        assert country.index == 5
        assert country.default == "Belgium"

    def test_regular_fields_have_no_default(self):
        """Fields without a default= attribute have default=None."""
        xml = meta_xml_from_zip("dwca-partial-default.zip")
        core = parse_meta_xml(xml).core
        scientific = next(
            f for f in core.fields
            if f.term == "http://rs.tdwg.org/dwc/terms/scientificName"
        )
        assert scientific.default is None


# ---------------------------------------------------------------------------
# Namespace stripping
# ---------------------------------------------------------------------------

class TestNamespaceStripping:
    """The xmlns declaration on <archive> must be removed before parsing.

    ElementTree treats namespace-qualified elements as '{uri}tagname', which
    breaks plain XPath queries like root.find('core'). The parser strips the
    namespace up front so the rest of the code can use simple tag names.
    """

    def test_namespace_does_not_break_parsing(self):
        """Archives with xmlns=... are parsed successfully."""
        xml = meta_xml_from_zip("dwca-simple-test-archive.zip")
        assert "xmlns=" in xml  # confirm the namespace is present in the source
        result = parse_meta_xml(xml)
        assert result.core.row_type != ""  # would be empty if XPath silently failed

    def test_namespace_stripped_regardless_of_metadata_attribute(self):
        """Namespace stripping works even when the <archive> has no other attributes."""
        xml = meta_xml_from_zip("dwca-nometadata.zip")
        result = parse_meta_xml(xml)
        assert result.core.filename == "occurrence.txt"


# ---------------------------------------------------------------------------
# FieldDescriptor dataclass
# ---------------------------------------------------------------------------

class TestFieldDescriptor:
    """FieldDescriptor can be constructed directly and all attributes are accessible."""

    def test_regular_field(self):
        """A normal field has a term, a column index, and no default."""
        f = FieldDescriptor(
            term="http://rs.tdwg.org/dwc/terms/scientificName",
            index=1,
            default=None,
        )
        assert f.term == "http://rs.tdwg.org/dwc/terms/scientificName"
        assert f.index == 1
        assert f.default is None

    def test_default_only_field(self):
        """A default-only field has index=None and a non-None default."""
        f = FieldDescriptor(term="http://rs.tdwg.org/dwc/terms/country", index=None, default="BE")
        assert f.index is None
        assert f.default == "BE"


# ---------------------------------------------------------------------------
# Inline XML - edge cases not covered by existing fixtures
# ---------------------------------------------------------------------------

class TestInlineXml:
    """Edge cases exercised with minimal inline XML strings.

    These tests target specific combinations that no single fixture archive
    happens to cover, without the overhead of creating new fixture files.
    """

    def test_comma_delimiter_and_double_quote_enclosure(self):
        """Comma delimiter and &quot; enclosure (XML entity) are both decoded correctly."""
        xml = """<archive xmlns="http://rs.tdwg.org/dwc/text/">
          <core encoding="utf-8" fieldsTerminatedBy="," linesTerminatedBy="\\n"
                fieldsEnclosedBy="&quot;" ignoreHeaderLines="1"
                rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
            <files><location>occ.csv</location></files>
            <id index="0"/>
            <field index="1" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
          </core>
        </archive>"""
        core = parse_meta_xml(xml).core
        assert core.fields_terminated_by == ","
        assert core.fields_enclosed_by == '"'

    def test_windows_line_endings(self):
        """\\r\\n in linesTerminatedBy is decoded to the two-character sequence."""
        xml = """<archive xmlns="http://rs.tdwg.org/dwc/text/">
          <core encoding="utf-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\r\\n"
                fieldsEnclosedBy="" ignoreHeaderLines="1"
                rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
            <files><location>occ.txt</location></files>
            <id index="0"/>
          </core>
        </archive>"""
        core = parse_meta_xml(xml).core
        assert core.lines_terminated_by == "\r\n"

    def test_id_at_non_zero_index(self):
        """The <id> element's index value is not assumed to be 0."""
        xml = """<archive xmlns="http://rs.tdwg.org/dwc/text/">
          <core encoding="utf-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n"
                fieldsEnclosedBy="" ignoreHeaderLines="1"
                rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
            <files><location>occ.txt</location></files>
            <id index="3"/>
            <field index="0" term="http://rs.tdwg.org/dwc/terms/basisOfRecord"/>
          </core>
        </archive>"""
        core = parse_meta_xml(xml).core
        assert core.id_index == 3

    def test_extension_coreid_at_non_zero_index(self):
        """The <coreid> element's index value is not assumed to be 0."""
        xml = """<archive xmlns="http://rs.tdwg.org/dwc/text/">
          <core encoding="utf-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n"
                fieldsEnclosedBy="" ignoreHeaderLines="1"
                rowType="http://rs.tdwg.org/dwc/terms/Taxon">
            <files><location>taxon.txt</location></files>
            <id index="0"/>
          </core>
          <extension encoding="utf-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n"
                     fieldsEnclosedBy="" ignoreHeaderLines="1"
                     rowType="http://rs.gbif.org/terms/1.0/VernacularName">
            <files><location>vn.txt</location></files>
            <coreid index="2"/>
            <field index="0" term="http://rs.tdwg.org/dwc/terms/vernacularName"/>
          </extension>
        </archive>"""
        ext = parse_meta_xml(xml).extensions[0]
        assert ext.coreid_index == 2

    def test_default_only_field_on_extension(self):
        """Default-only fields work on extensions the same way they do on the core."""
        xml = """<archive xmlns="http://rs.tdwg.org/dwc/text/">
          <core encoding="utf-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n"
                fieldsEnclosedBy="" ignoreHeaderLines="1"
                rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
            <files><location>occ.txt</location></files>
            <id index="0"/>
          </core>
          <extension encoding="utf-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n"
                     fieldsEnclosedBy="" ignoreHeaderLines="1"
                     rowType="http://rs.gbif.org/terms/1.0/Multimedia">
            <files><location>media.txt</location></files>
            <coreid index="0"/>
            <field index="1" term="http://purl.org/dc/terms/identifier"/>
            <field default="StillImage" term="http://purl.org/dc/terms/type"/>
          </extension>
        </archive>"""
        ext = parse_meta_xml(xml).extensions[0]
        media_type = next(f for f in ext.fields if "type" in f.term)
        assert media_type.index is None
        assert media_type.default == "StillImage"
