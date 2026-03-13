"""
Tests for the core conversion pipeline (_convert.py).

Each test converts a fixture archive to a temporary directory and inspects
the resulting Parquet files using PyArrow.  No mocking - we read real DwC-A
archives and write real Parquet files.

Fixture overview used here:
  dwca-simple-test-archive.zip  - basic occurrence core, 2 rows, has eml.xml
  dwca-2extensions.zip          - taxon core + Description + VernacularName extensions
  dwca-test-default.zip         - default-only field (country, index=None)
  dwca-partial-default.zip      - field with both index and default (country, index=5)
  dwca-nometadata.zip           - no metadata= attribute on <archive>
  gbif-results.zip              - real GBIF download, ignoreHeaderLines=0, 64 columns
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from dwca2parquet import convert

FIXTURES = Path(__file__).parent / "fixtures"


def fx(name: str) -> Path:
    """Return the path to a named test fixture."""
    return FIXTURES / name


# ---------------------------------------------------------------------------
# Basic single-file conversion
# ---------------------------------------------------------------------------

class TestBasicConversion:
    """A simple occurrence archive with a header line and 2 data rows."""

    def test_core_parquet_is_created(self, tmp_path):
        """convert() creates the core Parquet file in the output directory."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        assert result.core_path.exists()

    def test_core_filename_matches_row_type(self, tmp_path):
        """Output filename is derived from the rowType local name in lowercase."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        assert result.core_path.name == "occurrence.parquet"

    def test_core_row_count(self, tmp_path):
        """Row count in ConversionResult matches the number of data rows."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        assert result.core_row_count == 2

    def test_core_row_count_matches_parquet(self, tmp_path):
        """Row count in ConversionResult matches what is actually in the file."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        assert table.num_rows == result.core_row_count

    def test_id_column_present(self, tmp_path):
        """Core file always has a _id column."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        assert "_id" in table.schema.names

    def test_field_columns_present(self, tmp_path):
        """Term-named columns are present and correctly named."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        for col in ("basisOfRecord", "locality", "family", "scientificName"):
            assert col in table.schema.names

    def test_data_values_correct(self, tmp_path):
        """Cell values are read correctly from the CSV."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        names = table.column("scientificName").to_pylist()
        assert "tetraodon fluviatilis" in names
        assert "betta splendens" in names

    def test_all_columns_are_strings(self, tmp_path):
        """Raw mode: every column in the output is a string type."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        for field in table.schema:
            assert pa.types.is_string(field.type) or pa.types.is_large_string(field.type)

    def test_no_extensions(self, tmp_path):
        """Archives with no extensions produce an empty extension list."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        assert result.extension_paths == []
        assert result.extension_row_types == []
        assert result.extension_row_counts == []

    def test_eml_copied_to_output(self, tmp_path):
        """eml.xml is copied to the output directory and eml_path is set."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        assert result.eml_path is not None
        assert result.eml_path.exists()
        assert result.eml_path.name == "eml.xml"

    def test_conversion_result_metadata(self, tmp_path):
        """ConversionResult carries the expected metadata fields."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        assert result.core_row_type == "http://rs.tdwg.org/dwc/terms/Occurrence"
        assert result.conversion_mode == "raw"
        assert result.has_geometry is False
        assert result.geometry_null_count == 0
        assert result.denormalized_path is None
        assert result.elapsed_seconds >= 0


# ---------------------------------------------------------------------------
# Extension files
# ---------------------------------------------------------------------------

class TestExtensions:
    """Archive with a Taxon core and two extension files."""

    def test_extension_files_created(self, tmp_path):
        """One Parquet file is created per extension."""
        result = convert(fx("dwca-2extensions.zip"), tmp_path)
        assert len(result.extension_paths) == 2
        assert all(p.exists() for p in result.extension_paths)

    def test_extension_filenames(self, tmp_path):
        """Extension filenames are derived from their rowType local names."""
        result = convert(fx("dwca-2extensions.zip"), tmp_path)
        names = {p.name for p in result.extension_paths}
        assert "description.parquet" in names
        assert "vernacularname.parquet" in names

    def test_core_filename_is_taxon(self, tmp_path):
        """Core file is named after the Taxon rowType."""
        result = convert(fx("dwca-2extensions.zip"), tmp_path)
        assert result.core_path.name == "taxon.parquet"

    def test_extension_has_coreid_column(self, tmp_path):
        """Every extension file has a _coreid column."""
        result = convert(fx("dwca-2extensions.zip"), tmp_path)
        for ext_path in result.extension_paths:
            table = pq.read_table(ext_path)
            assert "_coreid" in table.schema.names

    def test_extension_does_not_have_id_column(self, tmp_path):
        """Extension files do not have a _id column (that belongs to the core)."""
        result = convert(fx("dwca-2extensions.zip"), tmp_path)
        for ext_path in result.extension_paths:
            table = pq.read_table(ext_path)
            assert "_id" not in table.schema.names

    def test_extension_row_counts(self, tmp_path):
        """Row counts are reported correctly per extension."""
        result = convert(fx("dwca-2extensions.zip"), tmp_path)
        counts = dict(zip(
            [p.name for p in result.extension_paths],
            result.extension_row_counts,
        ))
        assert counts["description.parquet"] == 3
        assert counts["vernacularname.parquet"] == 4

    def test_extension_coreid_values_link_to_core(self, tmp_path):
        """_coreid values in the extension match _id values in the core."""
        result = convert(fx("dwca-2extensions.zip"), tmp_path)
        core = pq.read_table(result.core_path)
        core_ids = set(core.column("_id").to_pylist())

        for ext_path in result.extension_paths:
            ext = pq.read_table(ext_path)
            for coreid in ext.column("_coreid").to_pylist():
                assert coreid in core_ids


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------

class TestDefaultValues:
    """Default values declared in meta.xml are baked into the Parquet output."""

    def test_default_only_column_added(self, tmp_path):
        """A default-only field (no CSV column) is added as a full column."""
        result = convert(fx("dwca-test-default.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        assert "country" in table.schema.names

    def test_default_only_column_all_same_value(self, tmp_path):
        """Every row gets the declared default when no CSV column exists."""
        result = convert(fx("dwca-test-default.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        countries = table.column("country").to_pylist()
        assert all(c == "Belgium" for c in countries)

    def test_default_only_column_metadata(self, tmp_path):
        """Default-only columns carry dwca:has_default metadata."""
        result = convert(fx("dwca-test-default.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        field = table.schema.field("country")
        assert field.metadata.get(b"dwca:has_default") == b"true"
        assert field.metadata.get(b"dwca:default_value") == b"Belgium"

    def test_partial_default_non_empty_value_preserved(self, tmp_path):
        """Rows with a real value in a defaulted column keep their value."""
        result = convert(fx("dwca-partial-default.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        countries = table.column("country").to_pylist()
        assert "France" in countries

    def test_partial_default_empty_value_filled(self, tmp_path):
        """Rows with an empty cell in a defaulted column get the default."""
        result = convert(fx("dwca-partial-default.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        countries = table.column("country").to_pylist()
        assert "Belgium" in countries
        assert "" not in countries


# ---------------------------------------------------------------------------
# GBIF format (no header line, 64 columns)
# ---------------------------------------------------------------------------

class TestGBIFFormat:
    """Real GBIF occurrence download: ignoreHeaderLines=0, many columns."""

    def test_conversion_succeeds(self, tmp_path):
        """A GBIF download archive converts without error."""
        result = convert(fx("gbif-results.zip"), tmp_path)
        assert result.core_path.exists()

    def test_row_count(self, tmp_path):
        """All data rows are read (no header to skip).

        The file has 160 lines but 2 records contain embedded newlines inside
        quoted fields, so PyArrow correctly produces 158 logical rows.
        """
        result = convert(fx("gbif-results.zip"), tmp_path)
        assert result.core_row_count == 158

    def test_id_column_present(self, tmp_path):
        """_id column is present even when id_index=0 overlaps with a field term."""
        result = convert(fx("gbif-results.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        assert "_id" in table.schema.names

    def test_scientific_name_column_present(self, tmp_path):
        """Standard DwC columns are accessible by their short name."""
        result = convert(fx("gbif-results.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        assert "scientificName" in table.schema.names

    def test_metadata_file_copied(self, tmp_path):
        """metadata.xml (not eml.xml) is copied when that is what the archive uses."""
        result = convert(fx("gbif-results.zip"), tmp_path)
        assert result.eml_path is not None
        assert result.eml_path.name == "metadata.xml"


# ---------------------------------------------------------------------------
# Parquet metadata
# ---------------------------------------------------------------------------

class TestParquetMetadata:
    """DwC-A metadata is embedded in the Parquet file and column schemas."""

    def test_file_level_row_type(self, tmp_path):
        """dwca:rowType is stored in the Parquet file footer."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        meta = pq.read_metadata(result.core_path).metadata
        assert meta[b"dwca:rowType"] == b"http://rs.tdwg.org/dwc/terms/Occurrence"

    def test_file_level_is_core_true(self, tmp_path):
        """dwca:is_core is 'true' for the core file."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        meta = pq.read_metadata(result.core_path).metadata
        assert meta[b"dwca:is_core"] == b"true"

    def test_file_level_is_core_false_for_extension(self, tmp_path):
        """dwca:is_core is 'false' for extension files."""
        result = convert(fx("dwca-2extensions.zip"), tmp_path)
        meta = pq.read_metadata(result.extension_paths[0]).metadata
        assert meta[b"dwca:is_core"] == b"false"

    def test_file_level_conversion_mode(self, tmp_path):
        """dwca:conversion_mode is stored in the file footer."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        meta = pq.read_metadata(result.core_path).metadata
        assert meta[b"dwca:conversion_mode"] == b"raw"

    def test_column_term_metadata(self, tmp_path):
        """Each term-mapped column carries its full DwC URI in dwca:term."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        sn_meta = table.schema.field("scientificName").metadata
        assert b"dwca:term" in sn_meta
        assert b"scientificName" in sn_meta[b"dwca:term"]

    def test_column_index_metadata(self, tmp_path):
        """Each column carries its original CSV index in dwca:index."""
        result = convert(fx("dwca-simple-test-archive.zip"), tmp_path)
        table = pq.read_table(result.core_path)
        # scientificName is declared at index 4 in dwca-simple-test-archive
        sn_meta = table.schema.field("scientificName").metadata
        assert sn_meta[b"dwca:index"] == b"4"


# ---------------------------------------------------------------------------
# No metadata file
# ---------------------------------------------------------------------------

class TestNoMetadata:
    """Archives without a metadata file declared in meta.xml."""

    def test_eml_path_is_none(self, tmp_path):
        """eml_path is None when no metadata= attribute is present."""
        result = convert(fx("dwca-nometadata.zip"), tmp_path)
        assert result.eml_path is None

    def test_conversion_still_succeeds(self, tmp_path):
        """Absence of metadata does not prevent conversion."""
        result = convert(fx("dwca-nometadata.zip"), tmp_path)
        assert result.core_path.exists()
        assert result.core_row_count > 0
