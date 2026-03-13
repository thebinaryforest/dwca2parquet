# dwca2parquet

[![CI](https://github.com/thebinaryforest/dwca2parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/thebinaryforest/dwca2parquet/actions/workflows/ci.yml)

A Python tool to convert Darwin Core Archive (DwC-A) files into Apache Parquet / GeoParquet format for high-performance data access.

## What does this tool do?

If you work with biodiversity data from GBIF or other sources, you've probably downloaded `.zip` files containing occurrence records, species checklists, or sampling events. These files are called Darwin Core Archives (DwC-A).

**dwca2parquet** converts those archives into Parquet files - a modern, high-performance data format widely used in data science. When the archive contains coordinates (`decimalLatitude` / `decimalLongitude`), the output is a GeoParquet file - meaning it includes a proper geometry column that GIS tools like QGIS can open directly. Even if you don't use GIS tools, everything works the same: DuckDB, Polars, R (arrow), pandas, and other tools read GeoParquet just like regular Parquet.

### Why convert?

- **Speed:** Parquet files are columnar and compressed. Reading 50 million occurrences that would take minutes with CSV-based tools can take seconds.
- **No special knowledge needed:** The tool takes care of DwC-A specifics (default values, extensions, metadata) so you get clean, ready-to-use tables.
- **GIS-ready:** When coordinates are present, the output includes a standard geometry column. Open it directly in QGIS, GeoPandas, or any GeoParquet-compatible tool - no manual coordinate wrangling needed.
- **Use your favorite tool:** The output is standard Parquet - open it with whatever you prefer.

### Installation

```bash
pip install dwca2parquet
```

### Quick start

#### Command line

```bash
# Basic conversion - produces a folder of Parquet files
dwca2parquet convert my_download.zip -o ./my_data/

# Force data types (latitude as float, dates as dates, etc.)
dwca2parquet convert my_download.zip -o ./my_data/ --typed

# Get a single, pre-joined table (convenient for simple archives)
dwca2parquet convert my_download.zip -o ./my_data/ --denormalize
```

#### Python API

```python
import dwca2parquet

result = dwca2parquet.convert(
    "my_download.zip",
    output_dir="./my_data/",
    typed=False,  # True to apply data types
)

print(result.core_path)        # "./my_data/occurrence.parquet"
print(result.extension_paths)  # ["./my_data/multimedia.parquet", ...]
```

### What you get

After conversion, your output folder looks like this:

```
my_data/
├── occurrence.parquet        # The core table
├── multimedia.parquet        # Extension table (if present in archive)
├── measurementorfact.parquet # Another extension (if present)
└── eml.xml                   # Original dataset metadata, copied as-is
```

Each Parquet file is a self-contained table. Extension tables include the core record identifier (e.g. `occurrenceID` or `id`) so you can join them to the core table when needed.

### Opening the data

#### With DuckDB (recommended for large files)

```sql
SELECT scientificName, decimalLatitude, decimalLongitude
FROM read_parquet('./my_data/occurrence.parquet')
WHERE country = 'Belgium'
LIMIT 100;
```

#### With Python (Polars)

```python
import polars as pl

df = pl.scan_parquet("./my_data/occurrence.parquet")
belgian = df.filter(pl.col("country") == "Belgium").collect()
```

#### With Python (pandas)

```python
import pandas as pd

df = pd.read_parquet("./my_data/occurrence.parquet")
```

#### With R

```r
library(arrow)

df <- read_parquet("./my_data/occurrence.parquet")
```

#### With QGIS (drag & drop)

Simply drag `occurrence.parquet` into the QGIS layer panel. Because the file is GeoParquet, QGIS will display the occurrences as points on the map automatically.

#### With GeoPandas (spatial analysis in Python)

```python
import geopandas as gpd

gdf = gpd.read_parquet("./my_data/occurrence.parquet")
gdf.plot()  # instant map
```

### Joining core and extension tables

Extensions have a many-to-one relationship with the core table. They share a common identifier column (the `coreid` in DwC-A terminology). In the Parquet output, this column is always named `_coreid` in extension files, matching the core table's `_id` column.

```sql
-- DuckDB example: join occurrences with multimedia records
SELECT o.scientificName, m.identifier AS media_url, m.type AS media_type
FROM read_parquet('./my_data/occurrence.parquet') o
JOIN read_parquet('./my_data/multimedia.parquet') m
  ON o._id = m._coreid;
```

### Typed vs. raw mode

By default, all columns are stored as strings, exactly as they appear in the original archive. This is the safest option - no data is altered or lost.

With `--typed`, the tool applies data types to well-known Darwin Core fields:

| Field example      | Type applied      |
|--------------------|-------------------|
| decimalLatitude    | float64           |
| decimalLongitude   | float64           |
| eventDate          | string (see below)|
| individualCount    | int64             |
| hasGeospatialIssue | boolean           |

> **Note on dates:** Darwin Core dates can be ranges (`2020-01/2020-03`), partial (`2020`), or standard ISO dates (`2020-06-15`). Because of this variability, `eventDate` is kept as a string even in typed mode. A separate parsed date column (`_eventDate_parsed`) may be added when the value is a valid single date.

Values that cannot be converted to the expected type (e.g. `"unknown"` in a numeric field) are set to null, and a warning is logged.

### Geometry column (GeoParquet)

When the archive contains `decimalLatitude` and `decimalLongitude` fields, dwca2parquet automatically creates a `geometry` column containing Point geometries in the standard GeoParquet format. This happens in both raw and typed modes.

This means you can:

- Open the file directly in QGIS - it appears as a point layer, no import step needed.
- Use GeoPandas for spatial analysis in Python.
- Run spatial queries in DuckDB with the spatial extension.

Records where either coordinate is missing or invalid get a null geometry. The original `decimalLatitude` and `decimalLongitude` columns are preserved as-is alongside the geometry column.

If your archive has no coordinate fields, the output is a plain Parquet file (no geometry column, no GeoParquet metadata). Non-geo tools won't notice any difference either way - the geometry column is simply ignored if you don't use it.

To skip geometry creation even when coordinates are present:

```bash
dwca2parquet convert my_download.zip -o ./my_data/ --no-geometry
```

### Interactive exploration with DuckDB

If you install the optional DuckDB support (`pip install dwca2parquet[duckdb]`), you can jump straight into an interactive SQL shell with your data pre-loaded:

```bash
# Convert and explore in one step
dwca2parquet shell my_download.zip

# Or explore a previously converted directory
dwca2parquet shell ./my_data/
```

This drops you into a DuckDB prompt where the core table and all extensions are already available as views. You can start querying immediately:

```sql
D> SELECT country, COUNT(*) AS n FROM core GROUP BY country ORDER BY n DESC LIMIT 10;
D> SELECT c.scientificName, m.identifier AS image_url
   FROM core c JOIN multimedia m ON c._id = m._coreid
   LIMIT 5;
```

If the data has coordinates, spatial functions work out of the box:

```sql
D> SELECT scientificName FROM core
   WHERE ST_DWithin(geometry, ST_Point(4.35, 50.85), 0.05);
```

You can also use it from Python:

```python
from dwca2parquet.duckdb import open_dwca

db = open_dwca("./my_data/")
df = db.sql("SELECT * FROM core WHERE country = 'Belgium'").fetchdf()
```

---

For implementation details, output format specification, type mappings, and the DuckDB API, see the [Technical Reference](REFERENCE.md).

