"""GeoParquet geometry encoding for Darwin Core coordinate columns.

Reads decimalLatitude / decimalLongitude string columns from a Parquet file,
encodes each valid coordinate pair as a WKB Point (EPSG:4326), and rewrites
the file with a 'geometry' column and GeoParquet 1.1 file-level metadata.

WKB encoding is done directly with struct.pack - no external geospatial library
is required for what is a trivial, long-stable binary format.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# WKB Point encoding
# ---------------------------------------------------------------------------

def _encode_wkb_point(lon: float, lat: float) -> bytes:
    """Encode a coordinate pair as a WKB Point (little-endian, EPSG:4326).

    Layout (21 bytes total):
      - 1 byte  : byte order flag (1 = little-endian)
      - 4 bytes : geometry type (1 = Point)
      - 8 bytes : X coordinate (longitude)
      - 8 bytes : Y coordinate (latitude)

    Parameters
    ----------
    lon : float
        Longitude (X), in decimal degrees WGS84.
    lat : float
        Latitude (Y), in decimal degrees WGS84.

    Returns
    -------
    bytes
        21-byte WKB-encoded Point.
    """
    return struct.pack("<bIdd", 1, 1, lon, lat)


# ---------------------------------------------------------------------------
# Column-level geometry builder
# ---------------------------------------------------------------------------

class _GeometryResult(NamedTuple):
    array: pa.Array
    null_count: int
    bbox: tuple[float, float, float, float] | None


def build_geometry_column(
    lat_array: pa.Array,
    lon_array: pa.Array,
) -> _GeometryResult:
    """Build a binary WKB geometry column from string lat/lon arrays.

    Each row is converted to a WKB-encoded Point if both values are present
    and parseable as floats. Rows where either value is null, empty, or
    non-numeric receive a null geometry entry.

    The bounding box covers only rows with valid geometry. It is None when
    no valid coordinate pair is found in the data.

    Parameters
    ----------
    lat_array : pa.Array
        String array of decimalLatitude values.
    lon_array : pa.Array
        String array of decimalLongitude values.

    Returns
    -------
    _GeometryResult
        Named tuple with fields:
        - array      : binary WKB array (null where coordinates are invalid)
        - null_count : number of null entries in the geometry array
        - bbox       : (min_lon, min_lat, max_lon, max_lat), or None
    """
    lats = lat_array.to_pylist()
    lons = lon_array.to_pylist()

    wkb_values: list[bytes | None] = []
    min_lon = min_lat = float("inf")
    max_lon = max_lat = float("-inf")
    valid_count = 0

    for lat_str, lon_str in zip(lats, lons):
        if not lat_str or not lon_str:
            wkb_values.append(None)
            continue

        try:
            lat = float(lat_str)
            lon = float(lon_str)
        except ValueError:
            wkb_values.append(None)
            continue

        wkb_values.append(_encode_wkb_point(lon, lat))

        if lon < min_lon:
            min_lon = lon
        if lat < min_lat:
            min_lat = lat
        if lon > max_lon:
            max_lon = lon
        if lat > max_lat:
            max_lat = lat
        valid_count += 1

    null_count = len(wkb_values) - valid_count
    bbox = (min_lon, min_lat, max_lon, max_lat) if valid_count > 0 else None

    return _GeometryResult(
        array=pa.array(wkb_values, type=pa.binary()),
        null_count=null_count,
        bbox=bbox,
    )


# ---------------------------------------------------------------------------
# GeoParquet 1.1 metadata
# ---------------------------------------------------------------------------

def _geo_metadata_json(
    bbox: tuple[float, float, float, float] | None,
) -> str:
    """Return the JSON string for the 'geo' key in GeoParquet 1.1 metadata.

    The CRS is always WGS84 (EPSG:4326), which is the Darwin Core standard.
    The bbox field is included only when valid coordinates were found.

    Parameters
    ----------
    bbox : tuple or None
        (min_lon, min_lat, max_lon, max_lat) in WGS84 degrees, or None.

    Returns
    -------
    str
        JSON-encoded GeoParquet 1.1 column metadata object.
    """
    column_meta: dict = {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "crs": {
            "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
            "type": "GeographicCRS",
            "name": "WGS 84",
            "id": {"authority": "EPSG", "code": 4326},
        },
    }
    if bbox is not None:
        column_meta["bbox"] = list(bbox)

    return json.dumps({
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": column_meta},
    })


# ---------------------------------------------------------------------------
# File-level post-processing
# ---------------------------------------------------------------------------

def add_geometry_to_parquet(
    path: Path,
    lat_col: str = "decimalLatitude",
    lon_col: str = "decimalLongitude",
) -> tuple[bool, int]:
    """Add a WKB geometry column to an existing Parquet file in place.

    Reads the file, builds a geometry column from the lat/lon string columns,
    and rewrites the file with the new column and GeoParquet 1.1 metadata in
    the file footer. The file is replaced atomically via a temporary file.

    If both lat/lon columns are not present, the file is left unchanged.

    Parameters
    ----------
    path : Path
        Path to the Parquet file to update.
    lat_col : str
        Name of the decimal latitude column (default: 'decimalLatitude').
    lon_col : str
        Name of the decimal longitude column (default: 'decimalLongitude').

    Returns
    -------
    tuple of (bool, int)
        (did_add_geometry, null_count).
        did_add_geometry is False if the lat/lon columns were not found.
        null_count is the number of rows with a null geometry entry.
    """
    table = pq.read_table(path)
    col_names = table.schema.names

    if lat_col not in col_names or lon_col not in col_names:
        return False, 0

    result = build_geometry_column(table.column(lat_col), table.column(lon_col))

    # Append geometry column and update file-level metadata
    table_with_geo = table.append_column(
        pa.field("geometry", pa.binary()), result.array
    )
    updated_meta = dict(table_with_geo.schema.metadata or {})
    updated_meta[b"geo"] = _geo_metadata_json(result.bbox).encode()
    table_with_geo = table_with_geo.replace_schema_metadata(updated_meta)

    # Write to a sibling temp file first, then replace atomically
    tmp_path = path.with_suffix(".tmp.parquet")
    try:
        pq.write_table(table_with_geo, tmp_path, compression="zstd")
        tmp_path.replace(path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    return True, result.null_count
