# dwca2parquet - Technical Reference

## Architecture overview

```
+--------------------------------------------------+
|                  DwC-A (.zip)                    |
|  meta.xml + eml.xml + CSV data files             |
+--------------------------------------------------+
           |
           v
+--------------------------------------------------+
|             dwca2parquet pipeline                |
|                                                  |
|  1. Parse meta.xml -> schema + file layout       |
|  2. For each data file:                          |
|     a. Stream CSV in batches (PyArrow)           |
|     b. Apply defaults from meta.xml              |
|     c. Optionally cast types (interpreted mode)  |
|     d. Inject _id / _coreid columns              |
|     e. Build geometry from coordinates (if any)  |
|     f. Write Parquet / GeoParquet (streaming)    |
|  3. Copy eml.xml to output                       |
|  4. Return ConversionResult                      |
+--------------------------------------------------+
```

## meta.xml parsing

The tool reads the DwC-A `meta.xml` descriptor to extract:

- **Core file declaration:** filename, rowType, encoding, field delimiter, line terminator, quote character, header lines to skip, and the id field index.
- **Extension file declarations:** same as above, plus the coreid field index.
- **Field definitions:** for each file, the list of fields with their index, term (URI), and optional default value.
- **Default values:** field-level `default` attributes in `meta.xml`.

The `meta.xml` is parsed with Python's standard `xml.etree.ElementTree`. No dependency on `python-dwca-reader`.

## CSV reading strategy

CSV files are read using `pyarrow.csv.open_csv()` (streaming reader) or `pyarrow.csv.read_csv()` depending on the mode:

- **Streaming mode** (default for large files): reads the CSV in configurable batches (default: 250,000 rows per batch). Each batch is processed (defaults applied, types cast) and appended to the output Parquet file using `pyarrow.parquet.ParquetWriter`. This allows converting archives that are much larger than available RAM.
- **In-memory mode** (for small files or when explicitly requested): reads the entire CSV into an Arrow table, processes it, and writes the Parquet file in one pass.

The threshold between streaming and in-memory modes can be configured but defaults to automatic detection based on file size within the archive (threshold: 500 MB uncompressed).

CSV parsing configuration is derived from `meta.xml` attributes:

| meta.xml attribute   | PyArrow CSV option                    |
|----------------------|---------------------------------------|
| fieldsTerminatedBy   | delimiter in ParseOptions             |
| linesTerminatedBy    | handled via line ending normalization |
| fieldsEnclosedBy     | quotechar in ParseOptions             |
| ignoreHeaderLines    | skip_rows in ReadOptions              |
| encoding             | transcoded to UTF-8 if needed         |

Column names are assigned from the field terms in `meta.xml` (using the short name, e.g. `decimalLatitude` rather than the full URI `http://rs.tdwg.org/dwc/terms/decimalLatitude`). When `meta.xml` does not provide a term for a field index, the column is named `_field_{index}`.

## Default value handling

Default values declared in `meta.xml` are filled into the data during conversion. This means:

- If a field has a `default` attribute and does **not** appear as a column in the CSV, a new column is created in the Parquet output, filled entirely with the default value.
- If a field has a `default` attribute and **does** appear in the CSV, rows with null/empty values in that column are filled with the default. Rows that already have a value are left unchanged.

This approach ensures the Parquet output is fully self-contained - users do not need to know about or handle DwC-A default values.

**Provenance metadata:** To preserve traceability, columns that had a default applied carry column-level Parquet metadata:

```json
{
  "dwca:has_default": "true",
  "dwca:default_value": "HumanObservation"
}
```

This allows advanced users to distinguish default-filled data from source-provided data if needed.

## Output format specification

### File naming

Output Parquet files are named based on the `rowType` of each data file in `meta.xml`, using the short (local) name in lowercase:

| rowType URI                                    | Output filename             |
|------------------------------------------------|-----------------------------|
| http://rs.tdwg.org/dwc/terms/Occurrence        | occurrence.parquet          |
| http://rs.tdwg.org/dwc/terms/Taxon             | taxon.parquet               |
| http://rs.tdwg.org/dwc/terms/Event             | event.parquet               |
| http://rs.gbif.org/terms/1.0/Multimedia        | multimedia.parquet          |
| http://rs.tdwg.org/dwc/terms/MeasurementOrFact | measurementorfact.parquet   |
| (any other)                                    | lowercase local name + .parquet |

If two files would produce the same name (unlikely but possible), a numeric suffix is added (e.g. `multimedia_2.parquet`).

### Reserved columns

The tool adds two reserved columns to manage relationships:

| Column    | Present in | Description                                                  |
|-----------|------------|--------------------------------------------------------------|
| `_id`     | Core file  | The core record identifier (from the `id` field in meta.xml) |
| `_coreid` | Extensions | The foreign key linking to the core record's `_id`           |

These columns are always strings, even in interpreted mode.

If the original archive uses `id` or `coreid` as actual Darwin Core field names (rare), the reserved column takes precedence and the original is renamed to `_orig_id` / `_orig_coreid`.

### Parquet file-level metadata

Each output Parquet file embeds metadata in its key-value footer:

| Key                  | Value                                      |
|----------------------|--------------------------------------------|
| dwca2parquet:version | Tool version (e.g. 1.0.0)                  |
| dwca:rowType         | Full rowType URI                           |
| dwca:source_archive  | Original archive filename                  |
| dwca:is_core         | true or false                              |
| dwca:core_rowType    | (extensions only) rowType of the core file |
| dwca:conversion_mode | raw or interpreted                         |
| dwca:converted_at    | ISO 8601 timestamp of conversion           |

### Column-level metadata

Each column carries metadata:

| Key                | Value                                             |
|--------------------|---------------------------------------------------|
| dwca:term          | Full DwC term URI (e.g. `http://rs.tdwg.org/...`) |
| dwca:index         | Original field index in the CSV                   |
| dwca:has_default   | `true` if a default was applied, absent otherwise |
| dwca:default_value | The default value (only if `has_default` is true) |
| dwca:original_type | (interpreted mode) `string` - the type before casting |

### Parquet write settings

- **Compression:** Zstandard (zstd), which offers the best compression ratio / speed tradeoff for this type of data. Configurable.
- **Row group size:** 250,000 rows (matches the streaming batch size). Configurable.
- **Data page size:** PyArrow default (1 MB).
- **Dictionary encoding:** enabled by default for all string columns (very effective for DwC data with many repeated values like `basisOfRecord`, `country`, etc.).
- **Parquet version:** 2.6 (supports nanosecond timestamps and other modern features).

## Interpreted mode - type mapping

In interpreted mode, columns corresponding to well-known Darwin Core terms are cast to appropriate Arrow/Parquet types. The mapping is defined in a built-in dictionary and can be extended by users.

**Built-in type mapping (excerpt):**

| Term (short name)             | Parquet type | Notes                         |
|-------------------------------|--------------|-------------------------------|
| decimalLatitude               | float64      |                               |
| decimalLongitude              | float64      |                               |
| coordinateUncertaintyInMeters | float64      |                               |
| coordinatePrecision           | float64      |                               |
| depth                         | float64      |                               |
| depthAccuracy                 | float64      |                               |
| elevation                     | float64      |                               |
| elevationAccuracy             | float64      |                               |
| individualCount               | int64        | Null if non-integer           |
| year                          | int32        |                               |
| month                         | int32        |                               |
| day                           | int32        |                               |
| hasGeospatialIssues           | boolean      | "true" / "false" / "1" / "0" |
| hasCoordinate                 | boolean      | Same parsing as above         |
| taxonKey / speciesKey / etc.  | int64        | GBIF-specific numeric keys    |
| numberOfOccurrences           | int64        |                               |
| dateIdentified                | timestamp    | Only if valid ISO 8601 date   |
| modified                      | timestamp    | Only if valid ISO 8601 date   |
| eventDate                     | string       | Kept as string (see below)    |

**Custom type mapping:** Users can provide their own mapping to override or extend the built-in one:

```python
dwca2parquet.convert(
    "archive.zip",
    output_dir="./out/",
    interpreted=True,
    type_overrides={
        "http://example.org/terms/myCustomField": pa.float32(),
        "individualCount": pa.int32(),  # override the default int64
    },
)
```

**Type conversion failures:** When a value cannot be cast to the target type, it is set to null and a counter is incremented. After conversion, the `ConversionResult` reports the number of type conversion failures per column. If more than a configurable percentage of values fail (default: 10%), a warning is emitted suggesting the column may not be suitable for that type.

## eventDate handling

Darwin Core's `eventDate` field is notoriously variable. Values can be:

- A single ISO date: `2020-06-15`
- A date-time: `2020-06-15T10:30:00Z`
- A year-month: `2020-06`
- A year only: `2020`
- A date range: `2020-01-01/2020-03-31`

Because of this, `eventDate` is always kept as a string in the output, even in interpreted mode.

In interpreted mode, an additional convenience column `_eventDate_start` (and `_eventDate_end` for ranges) of type `date32` is added when the value can be parsed. Rows where parsing fails get null in these derived columns.

## Denormalized mode

When `--denormalize` is passed (or `denormalize=True` in the Python API), the tool produces a single Parquet file by left-joining extensions onto the core table using `_id` / `_coreid`.

This is convenient for simple analyses but comes with caveats:

- Core rows are duplicated for each matching extension row (1-to-many). A core record with 5 multimedia records produces 5 rows.
- If multiple extensions are present, they are joined sequentially, which can cause a combinatorial explosion. The tool emits a warning if the estimated output exceeds 10x the core row count.
- Column name conflicts between core and extension are resolved by prefixing extension columns with the extension's short rowType name (e.g. `multimedia_identifier`, `multimedia_type`).

The denormalized file is named `denormalized.parquet`.

## GeoParquet output

When the core data file contains both `decimalLatitude` and `decimalLongitude` fields, dwca2parquet can produce a GeoParquet 1.1 compliant file with a `geometry` column. Whether it does so by default depends on the conversion mode:

- **Interpreted mode** (`--interpreted`): geometry is created by default. Disable with `--no-geometry`.
- **Raw mode** (default): geometry is **not** created by default. Enable with `--geometry`.

In the Python API, this is controlled by the `geometry` parameter:

```python
# Interpreted mode with geometry (default when interpreted=True)
convert("archive.zip", interpreted=True)

# Interpreted mode, geometry suppressed
convert("archive.zip", interpreted=True, geometry=False)

# Raw mode, geometry opt-in
convert("archive.zip", interpreted=False, geometry=True)
```

Pass `geometry=None` (the default) to use the mode-appropriate default.

### How it works

For each row where both `decimalLatitude` and `decimalLongitude` are present and parseable as floats, a WKB-encoded Point geometry is created and stored in a `geometry` column (binary type). Rows where either coordinate is missing, empty, or non-numeric get a null geometry.

The coordinates are assumed to be WGS84 (EPSG:4326), which is the standard for Darwin Core and the default CRS in the GeoParquet specification.

The original `decimalLatitude` and `decimalLongitude` columns are preserved in the output alongside the geometry column. This avoids any data loss and allows non-spatial tools to work with the coordinates as plain numbers.

### GeoParquet metadata

The file-level Parquet metadata includes a `geo` key (JSON-encoded) following the GeoParquet 1.1 specification:

```json
{
  "version": "1.1.0",
  "primary_column": "geometry",
  "columns": {
    "geometry": {
      "encoding": "WKB",
      "geometry_types": ["Point"],
      "crs": {
        "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
        "type": "GeographicCRS",
        "name": "WGS 84",
        "id": { "authority": "EPSG", "code": 4326 }
      },
      "bbox": [minLon, minLat, maxLon, maxLat]
    }
  }
}
```

The `bbox` is computed from the actual data during conversion.

### Compatibility

A GeoParquet file is a valid Parquet file. Tools that are not GeoParquet-aware simply see a regular Parquet file with an additional binary column named `geometry`. They can read all other columns normally and ignore the geometry column. This means there is no compatibility downside to always producing GeoParquet when coordinates are available.

Tools with GeoParquet support (QGIS, GeoPandas, DuckDB spatial, GDAL >= 3.5, Kepler.gl, BigQuery, and many others) will recognize the geometry column automatically and treat the file as a spatial dataset.

### When geometry is not produced

No geometry column or GeoParquet metadata is added when:

- The archive does not contain `decimalLatitude` and `decimalLongitude` fields.
- Geometry is disabled for the active mode (raw mode by default, or explicitly via `--no-geometry`).
- The data file is an extension (geometry is only added to the core file).

In these cases, the output is a plain Parquet file.

### Future: GeoParquet 2.0

GeoParquet 2.0 (in development) will align with Parquet's native GEOMETRY logical type introduced in Parquet format 2.11. The tool will adopt GeoParquet 2.0 when the ecosystem support is mature. The migration will be transparent to users - output files will remain readable by the same tools.

## ConversionResult object

The `convert()` function returns a `ConversionResult` dataclass:

```python
@dataclass
class ConversionResult:
    core_path: Path                          # Path to the core Parquet file
    core_row_type: str                       # Core rowType URI
    core_row_count: int                      # Number of rows in core
    has_geometry: bool                       # True if GeoParquet geometry was created
    geometry_null_count: int                 # Rows with null geometry (missing coords)
    extension_paths: list[Path]              # Paths to extension Parquet files
    extension_row_types: list[str]           # rowType URIs for each extension
    extension_row_counts: list[int]          # Row counts for each extension
    denormalized_path: Path | None           # Path if denormalize=True
    eml_path: Path | None                    # Path to copied eml.xml, if present
    conversion_mode: str                     # "raw" or "interpreted"
    type_conversion_failures: dict[str, int] # Column name -> failure count (interpreted mode)
    warnings: list[str]                      # Any warnings generated
    elapsed_seconds: float                   # Total conversion time
```

## CLI reference

```
dwca2parquet convert <archive> [options]

Positional arguments:
  archive                     Path to the DwC-A zip file (or an unzipped directory)

Options:
  -o, --output-dir DIR        Output directory (default: ./<archive_name>_parquet/)
  --interpreted               Interpret data: apply types, geometry, date parsing
  --raw                       Keep all columns as strings, no interpretation (default)
  --denormalize               Produce an additional single joined Parquet file
  --batch-size N              Rows per batch for streaming conversion (default: 250000)
  --compression CODEC         Parquet compression: zstd (default), snappy, gzip, none
  --type-overrides FILE       Path to a JSON file with custom type mappings
  --geometry                  Force geometry column creation in raw mode
  --no-geometry               Suppress geometry column creation in interpreted mode
  --no-eml                    Do not copy eml.xml to output
  -v, --verbose               Verbose logging
  -q, --quiet                 Suppress all output except errors
  --version                   Show version and exit
  -h, --help                  Show help and exit

dwca2parquet inspect <archive>

  Print a summary of the archive structure (core type, extensions, field count,
  row count estimate) without converting.
```

## Dependencies

- **Python:** >= 3.10
- **PyArrow:** >= 14.0 (CSV reader, Parquet writer, type system)
- Standard library only for `meta.xml` parsing (`xml.etree.ElementTree`, `zipfile`) and WKB Point encoding (`struct`)
- No dependency on `python-dwca-reader`, Shapely, or GeoPandas - this is a standalone tool.

WKB Point encoding is done directly with `struct.pack('<bId', 1, 1, lon, lat)` (21 bytes per point: byte order flag, geometry type, two float64 coordinates). This avoids pulling in heavy geospatial libraries for what is a trivial and long-stable binary format.

## Limitations and known constraints

- **ZIP structure:** The tool expects a standard DwC-A zip (`meta.xml` at the root level). Nested zips or non-standard archive layouts are not supported.
- **Encoding:** Non-UTF-8 files are transcoded to UTF-8 during conversion. Characters that cannot be decoded are replaced with the Unicode replacement character (U+FFFD) and a warning is logged.
- **Very wide archives:** Archives with hundreds of columns are supported but may produce large Parquet files due to schema overhead. Dictionary encoding mitigates this for string columns.
- **No write-back:** This is a one-way conversion tool. Converting Parquet back to DwC-A is out of scope (though it would be technically possible using the embedded metadata).
- **eventDate precision:** Date parsing covers common ISO 8601 formats and ranges. Exotic formats (e.g. "Summer 2020", "before 1950") are not parsed and result in null in derived date columns.

## DuckDB convenience layer

dwca2parquet ships an optional module that registers converted Parquet files as DuckDB views with relationships pre-configured. This is not a hard dependency - DuckDB is only required if you use this module.

### Quick start

```python
from dwca2parquet.duckdb import open_dwca

# From a conversion result
result = dwca2parquet.convert("archive.zip", output_dir="./out/")
db = open_dwca(result)

# Or from an existing output directory
db = open_dwca("./out/")

# Query immediately - views are ready
df = db.sql("""
    SELECT scientificName, country, geometry
    FROM core
    WHERE year >= 2020
""").fetchdf()

# Extensions are available as views named after their rowType
df = db.sql("""
    SELECT c.scientificName, m.identifier AS media_url
    FROM core c
    JOIN multimedia m ON c._id = m._coreid
    WHERE c.country = 'Belgium'
""").fetchdf()
```

### Command line

```bash
# Launch an interactive DuckDB shell with the archive pre-loaded
dwca2parquet shell ./out/

# Or convert + shell in one step
dwca2parquet shell my_download.zip
```

This opens a DuckDB CLI session where `core`, `multimedia`, etc. are already registered as views. You can start querying right away.

### What open_dwca does

- Creates an in-memory DuckDB connection (or attaches to a user-provided one).
- For each Parquet file in the output directory, creates a view named after the file (e.g. `occurrence.parquet` -> view `occurrence`). The core file is also aliased as `core`.
- Installs and loads the DuckDB spatial extension if it detects a geometry column, enabling spatial functions like `ST_Distance`, `ST_Within`, `ST_AsText`, etc.
- Returns the connection, ready to query.

### API reference

```python
def open_dwca(
    source: ConversionResult | str | Path,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
    spatial: bool = True,
) -> duckdb.DuckDBPyConnection:
    """
    Register converted DwC-A Parquet files as DuckDB views.

    Parameters
    ----------
    source
        A ConversionResult from convert(), or a path to an output directory
        containing Parquet files produced by dwca2parquet.
    connection
        An existing DuckDB connection to use. If None, creates a new
        in-memory database.
    spatial
        If True (default) and a geometry column is detected, automatically
        install and load the DuckDB spatial extension.

    Returns
    -------
    duckdb.DuckDBPyConnection
        The connection with views registered.
    """
```

### Spatial queries

When the core file is GeoParquet and the spatial extension is loaded, you get access to DuckDB's full spatial function set:

```sql
-- Find occurrences within 10km of Brussels
SELECT scientificName, ST_AsText(geometry) AS wkt
FROM core
WHERE ST_DWithin(
    geometry,
    ST_Point(4.3517, 50.8503),
    0.09  -- ~10km in degrees at this latitude
);

-- Bounding box filter (very fast thanks to Parquet row group statistics)
SELECT COUNT(*) AS n_records, country
FROM core
WHERE ST_X(geometry) BETWEEN 2.5 AND 6.4
  AND ST_Y(geometry) BETWEEN 49.5 AND 51.5
GROUP BY country;
```

### Integration with existing workflows

The returned connection is a standard DuckDB connection. You can combine it with other data sources, export results, or hand it off to other tools:

```python
db = open_dwca("./out/")

# Export a filtered subset to a new Parquet file
db.sql("""
    COPY (
        SELECT * FROM core WHERE country = 'Belgium' AND year >= 2015
    ) TO 'belgian_recent.parquet' (FORMAT PARQUET)
""")

# Convert to a pandas or Polars DataFrame
df = db.sql("SELECT * FROM core LIMIT 100000").fetchdf()  # pandas
df = db.sql("SELECT * FROM core LIMIT 100000").pl()       # polars

# Combine with external data
db.sql("""
    CREATE TABLE protected_areas AS
    SELECT * FROM read_parquet('protected_areas.parquet')
""")
db.sql("""
    SELECT c.scientificName, p.name AS protected_area
    FROM core c
    JOIN protected_areas p ON ST_Within(c.geometry, p.geometry)
""")
```

### Dependencies

The DuckDB layer requires:

- `duckdb` >= 0.10.0
- DuckDB spatial extension: auto-installed on first use if `spatial=True`

These are optional dependencies, installable via:

```bash
pip install dwca2parquet[duckdb]
```

## Future considerations

- **Partitioned output:** For very large archives (100M+ rows), optionally partition the Parquet output by a column (e.g. `countryCode`) to enable even faster filtered reads.
- **Remote archives:** Support reading directly from URLs (GBIF download links) or cloud storage (S3, GCS) using `fsspec`, converting on-the-fly without downloading the full archive to disk.
