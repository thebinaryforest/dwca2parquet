"""
Tests for the GeoParquet geometry module (_geo.py) and its integration
with the conversion pipeline.

All geometry tests use gbif-results.zip - the only fixture with
decimalLatitude / decimalLongitude columns.  That archive produces 158 rows,
of which 56 have valid coordinate pairs and 102 have empty coordinates.

Fixture overview:
  gbif-results.zip          - real GBIF download, 158 rows, 56 with coords
  dwca-simple-test-archive.zip - no coordinate columns (negative path tests)
"""

import json
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from dwca2parquet import convert
from dwca2parquet._geo import (
    _encode_wkb_point,
    _geo_metadata_json,
    add_geometry_to_parquet,
    build_geometry_column,
)

FIXTURES = Path(__file__).parent / "fixtures"
GBIF = FIXTURES / "gbif-results.zip"
NO_COORDS = FIXTURES / "dwca-simple-test-archive.zip"

# Known values from the GBIF fixture (verified by inspection)
GBIF_TOTAL_ROWS = 158
GBIF_VALID_COORD_ROWS = 56
GBIF_NULL_COORD_ROWS = 102  # rows where lat or lon is empty


# ---------------------------------------------------------------------------
# Unit tests: WKB encoding
# ---------------------------------------------------------------------------

class TestWkbEncoding:
    """The _encode_wkb_point function produces well-formed WKB Points."""

    def test_length_is_21_bytes(self):
        """A WKB Point is always 21 bytes (header + two float64)."""
        wkb = _encode_wkb_point(4.35, 50.85)
        assert len(wkb) == 21

    def test_byte_order_flag(self):
        """First byte is 1, indicating little-endian encoding."""
        wkb = _encode_wkb_point(0.0, 0.0)
        assert wkb[0] == 1

    def test_geometry_type_is_point(self):
        """Bytes 1-4 encode geometry type 1 (Point) in little-endian."""
        wkb = _encode_wkb_point(0.0, 0.0)
        geom_type = struct.unpack_from("<I", wkb, 1)[0]
        assert geom_type == 1

    def test_coordinates_round_trip(self):
        """Longitude and latitude survive a pack/unpack round trip exactly."""
        lon, lat = 4.3517, 50.8503
        wkb = _encode_wkb_point(lon, lat)
        _, _, decoded_lon, decoded_lat = struct.unpack("<bIdd", wkb)
        assert decoded_lon == lon
        assert decoded_lat == lat

    def test_negative_coordinates(self):
        """Negative coordinates (southern/western hemisphere) are encoded correctly."""
        lon, lat = -73.935, -15.78
        wkb = _encode_wkb_point(lon, lat)
        _, _, decoded_lon, decoded_lat = struct.unpack("<bIdd", wkb)
        assert decoded_lon == lon
        assert decoded_lat == lat

    def test_zero_coordinates(self):
        """Coordinates (0, 0) - Gulf of Guinea - are encoded as non-null WKB."""
        wkb = _encode_wkb_point(0.0, 0.0)
        assert wkb is not None
        assert len(wkb) == 21


# ---------------------------------------------------------------------------
# Unit tests: build_geometry_column
# ---------------------------------------------------------------------------

class TestBuildGeometryColumn:
    """The build_geometry_column function converts string arrays to WKB."""

    def _make_arrays(self, lats, lons):
        return (
            pa.array(lats, type=pa.string()),
            pa.array(lons, type=pa.string()),
        )

    def test_valid_pair_produces_wkb(self):
        """A valid lat/lon pair produces a non-null 21-byte WKB entry."""
        lat, lon = self._make_arrays(["50.85"], ["4.35"])
        result = build_geometry_column(lat, lon)
        wkb_list = result.array.to_pylist()
        assert wkb_list[0] is not None
        assert len(wkb_list[0]) == 21

    def test_empty_lat_produces_null(self):
        """An empty latitude string results in a null geometry."""
        lat, lon = self._make_arrays([""], ["4.35"])
        result = build_geometry_column(lat, lon)
        assert result.array.to_pylist()[0] is None

    def test_empty_lon_produces_null(self):
        """An empty longitude string results in a null geometry."""
        lat, lon = self._make_arrays(["50.85"], [""])
        result = build_geometry_column(lat, lon)
        assert result.array.to_pylist()[0] is None

    def test_none_lat_produces_null(self):
        """A None latitude value results in a null geometry."""
        lat, lon = self._make_arrays([None], ["4.35"])
        result = build_geometry_column(lat, lon)
        assert result.array.to_pylist()[0] is None

    def test_non_numeric_produces_null(self):
        """Non-numeric strings result in null geometry, not an exception."""
        lat, lon = self._make_arrays(["not-a-number"], ["4.35"])
        result = build_geometry_column(lat, lon)
        assert result.array.to_pylist()[0] is None

    def test_null_count_matches_invalid_rows(self):
        """null_count equals the number of rows that could not be encoded."""
        lat, lon = self._make_arrays(
            ["50.85", "", "48.87", None, "51.50"],
            ["4.35", "3.0", "2.35", "4.35", "not-a-lon"],
        )
        result = build_geometry_column(lat, lon)
        # row 1 (empty lat), row 3 (None lat), row 4 (bad lon) -> 3 nulls
        assert result.null_count == 3

    def test_result_array_type_is_binary(self):
        """The output array has binary type (required for WKB in Parquet)."""
        lat, lon = self._make_arrays(["50.85"], ["4.35"])
        result = build_geometry_column(lat, lon)
        assert pa.types.is_binary(result.array.type)

    def test_bbox_covers_valid_rows(self):
        """Bounding box reflects the actual min/max of encoded coordinates."""
        lat, lon = self._make_arrays(
            ["10.0", "", "20.0"],
            ["30.0", "35.0", "40.0"],
        )
        result = build_geometry_column(lat, lon)
        # row 1 (empty lat) is excluded from bbox
        min_lon, min_lat, max_lon, max_lat = result.bbox
        assert min_lon == 30.0
        assert max_lon == 40.0
        assert min_lat == 10.0
        assert max_lat == 20.0

    def test_bbox_is_none_when_all_invalid(self):
        """bbox is None when no valid coordinate pair exists."""
        lat, lon = self._make_arrays(["", None], ["", ""])
        result = build_geometry_column(lat, lon)
        assert result.bbox is None

    def test_zero_coordinates_valid(self):
        """Coordinates (0.0, 0.0) are treated as valid, not as empty."""
        lat, lon = self._make_arrays(["0.0"], ["0.0"])
        result = build_geometry_column(lat, lon)
        assert result.array.to_pylist()[0] is not None
        assert result.null_count == 0


# ---------------------------------------------------------------------------
# Unit tests: GeoParquet metadata JSON
# ---------------------------------------------------------------------------

class TestGeoMetadataJson:
    """The _geo_metadata_json function produces valid GeoParquet 1.1 JSON."""

    def test_valid_json(self):
        """Output is parseable JSON."""
        s = _geo_metadata_json(None)
        parsed = json.loads(s)
        assert isinstance(parsed, dict)

    def test_version_field(self):
        """GeoParquet version is 1.1.0."""
        parsed = json.loads(_geo_metadata_json(None))
        assert parsed["version"] == "1.1.0"

    def test_primary_column(self):
        """Primary column is named 'geometry'."""
        parsed = json.loads(_geo_metadata_json(None))
        assert parsed["primary_column"] == "geometry"

    def test_encoding_is_wkb(self):
        """Column encoding is WKB."""
        parsed = json.loads(_geo_metadata_json(None))
        assert parsed["columns"]["geometry"]["encoding"] == "WKB"

    def test_geometry_type_is_point(self):
        """Declared geometry type is Point."""
        parsed = json.loads(_geo_metadata_json(None))
        types = parsed["columns"]["geometry"]["geometry_types"]
        assert types == ["Point"]

    def test_crs_is_epsg_4326(self):
        """CRS authority is EPSG and code is 4326."""
        parsed = json.loads(_geo_metadata_json(None))
        crs_id = parsed["columns"]["geometry"]["crs"]["id"]
        assert crs_id["authority"] == "EPSG"
        assert crs_id["code"] == 4326

    def test_bbox_included_when_provided(self):
        """bbox list is present when a valid bbox is passed."""
        parsed = json.loads(_geo_metadata_json((2.0, 49.0, 6.5, 51.5)))
        assert parsed["columns"]["geometry"]["bbox"] == [2.0, 49.0, 6.5, 51.5]

    def test_bbox_absent_when_none(self):
        """bbox key is absent when None is passed (all coordinates were null)."""
        parsed = json.loads(_geo_metadata_json(None))
        assert "bbox" not in parsed["columns"]["geometry"]


# ---------------------------------------------------------------------------
# Integration: add_geometry_to_parquet
# ---------------------------------------------------------------------------

class TestAddGeometryToParquet:
    """add_geometry_to_parquet rewrites a Parquet file with a geometry column."""

    def test_returns_true_when_coords_present(self, tmp_path):
        """Returns (True, null_count) when lat/lon columns exist in the file."""
        result = convert(GBIF, tmp_path)
        did_add, _ = add_geometry_to_parquet(result.core_path)
        assert did_add is True

    def test_returns_false_when_no_coords(self, tmp_path):
        """Returns (False, 0) when the file has no lat/lon columns."""
        result = convert(NO_COORDS, tmp_path)
        did_add, null_count = add_geometry_to_parquet(result.core_path)
        assert did_add is False
        assert null_count == 0

    def test_geometry_column_added(self, tmp_path):
        """A 'geometry' column appears in the schema after processing."""
        result = convert(GBIF, tmp_path)
        add_geometry_to_parquet(result.core_path)
        table = pq.read_table(result.core_path)
        assert "geometry" in table.schema.names

    def test_geometry_column_is_binary(self, tmp_path):
        """The geometry column has binary type (WKB storage)."""
        result = convert(GBIF, tmp_path)
        add_geometry_to_parquet(result.core_path)
        table = pq.read_table(result.core_path)
        assert pa.types.is_binary(table.schema.field("geometry").type)

    def test_null_count_matches_missing_coords(self, tmp_path):
        """Null geometry count equals the number of rows with empty coordinates."""
        result = convert(GBIF, tmp_path)
        _, null_count = add_geometry_to_parquet(result.core_path)
        assert null_count == GBIF_NULL_COORD_ROWS

    def test_valid_geometry_count(self, tmp_path):
        """Number of non-null geometry entries equals rows with valid coords."""
        result = convert(GBIF, tmp_path)
        add_geometry_to_parquet(result.core_path)
        table = pq.read_table(result.core_path)
        non_null = table.column("geometry").null_count
        valid = GBIF_TOTAL_ROWS - non_null
        assert valid == GBIF_VALID_COORD_ROWS

    def test_valid_wkb_length(self, tmp_path):
        """Each non-null geometry entry is exactly 21 bytes (WKB Point)."""
        result = convert(GBIF, tmp_path)
        add_geometry_to_parquet(result.core_path)
        table = pq.read_table(result.core_path)
        for wkb in table.column("geometry").to_pylist():
            if wkb is not None:
                assert len(wkb) == 21

    def test_geo_metadata_key_present(self, tmp_path):
        """The Parquet file footer contains a 'geo' key after processing."""
        result = convert(GBIF, tmp_path)
        add_geometry_to_parquet(result.core_path)
        meta = pq.read_metadata(result.core_path).metadata
        assert b"geo" in meta

    def test_geo_metadata_is_valid_json(self, tmp_path):
        """The 'geo' footer value is valid JSON."""
        result = convert(GBIF, tmp_path)
        add_geometry_to_parquet(result.core_path)
        meta = pq.read_metadata(result.core_path).metadata
        parsed = json.loads(meta[b"geo"])
        assert "version" in parsed

    def test_original_columns_preserved(self, tmp_path):
        """decimalLatitude and decimalLongitude are still present after geometry is added."""
        result = convert(GBIF, tmp_path)
        add_geometry_to_parquet(result.core_path)
        table = pq.read_table(result.core_path)
        assert "decimalLatitude" in table.schema.names
        assert "decimalLongitude" in table.schema.names

    def test_row_count_unchanged(self, tmp_path):
        """Rewriting with geometry does not change the total row count."""
        result = convert(GBIF, tmp_path)
        add_geometry_to_parquet(result.core_path)
        table = pq.read_table(result.core_path)
        assert table.num_rows == GBIF_TOTAL_ROWS

    def test_existing_metadata_preserved(self, tmp_path):
        """DwC-A metadata keys (e.g. dwca:rowType) survive the geometry rewrite."""
        result = convert(GBIF, tmp_path)
        add_geometry_to_parquet(result.core_path)
        meta = pq.read_metadata(result.core_path).metadata
        assert b"dwca:rowType" in meta


# ---------------------------------------------------------------------------
# Integration: convert() with geometry flags
# ---------------------------------------------------------------------------

class TestConvertGeometryIntegration:
    """The convert() function correctly applies geometry based on mode flags."""

    def test_raw_mode_no_geometry_by_default(self, tmp_path):
        """Raw mode does not add geometry by default."""
        result = convert(GBIF, tmp_path)
        assert result.has_geometry is False
        assert result.geometry_null_count == 0

    def test_raw_mode_geometry_opt_in(self, tmp_path):
        """Raw mode adds geometry when geometry=True is passed explicitly."""
        result = convert(GBIF, tmp_path, geometry=True)
        assert result.has_geometry is True

    def test_interpreted_mode_geometry_by_default(self, tmp_path):
        """Interpreted mode adds geometry by default when coords are present."""
        result = convert(GBIF, tmp_path, interpreted=True)
        assert result.has_geometry is True

    def test_interpreted_mode_geometry_suppressed(self, tmp_path):
        """Interpreted mode skips geometry when geometry=False is passed."""
        result = convert(GBIF, tmp_path, interpreted=True, geometry=False)
        assert result.has_geometry is False

    def test_has_geometry_false_when_no_coord_columns(self, tmp_path):
        """has_geometry is False when the archive has no coordinate columns."""
        result = convert(NO_COORDS, tmp_path, interpreted=True)
        assert result.has_geometry is False

    def test_geometry_null_count_in_result(self, tmp_path):
        """geometry_null_count in ConversionResult matches actual null geometry rows."""
        result = convert(GBIF, tmp_path, geometry=True)
        assert result.geometry_null_count == GBIF_NULL_COORD_ROWS

    def test_geometry_column_in_file(self, tmp_path):
        """The written Parquet file contains the geometry column."""
        result = convert(GBIF, tmp_path, geometry=True)
        table = pq.read_table(result.core_path)
        assert "geometry" in table.schema.names

    def test_extensions_never_get_geometry(self, tmp_path):
        """Geometry is only added to the core file, never to extensions."""
        result = convert(FIXTURES / "dwca-2extensions.zip", tmp_path, geometry=True)
        for ext_path in result.extension_paths:
            table = pq.read_table(ext_path)
            assert "geometry" not in table.schema.names
