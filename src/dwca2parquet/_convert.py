"""
Core DwC-A to Parquet conversion pipeline.

Reads a Darwin Core Archive zip file, converts each data file (core +
extensions) to a Parquet file, and returns a ConversionResult describing
what was produced.

Raw mode (default): all columns stored as strings exactly as they appear in
the source CSV.  Typed mode and geometry creation are handled separately in
_types.py and _geo.py (Step 4).
"""

from __future__ import annotations

import io
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

from dwca2parquet import __version__
from dwca2parquet._geo import add_geometry_to_parquet
from dwca2parquet._meta import FileDescriptor, parse_meta_xml
from dwca2parquet._result import ConversionResult

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _short_name(term: str) -> str:
    """Extract the local name from a full Darwin Core term URI.

    Examples
    --------
    >>> _short_name("http://rs.tdwg.org/dwc/terms/scientificName")
    'scientificName'
    >>> _short_name("http://rs.gbif.org/terms/1.0/Multimedia")
    'Multimedia'
    """
    for sep in ("#", "/"):
        if sep in term:
            return term.rsplit(sep, 1)[-1]
    return term


def _output_filename(row_type: str, used: set[str]) -> str:
    """Determine a unique output Parquet filename from a rowType URI.

    Uses the lowercase local name.  Appends a numeric suffix if the name is
    already taken (e.g. multimedia_2.parquet).
    """
    base = _short_name(row_type).lower()
    name = f"{base}.parquet"
    if name not in used:
        return name
    i = 2
    while True:
        name = f"{base}_{i}.parquet"
        if name not in used:
            return name
        i += 1


# ---------------------------------------------------------------------------
# Column name mapping
# ---------------------------------------------------------------------------

def _peek_column_count(data: bytes, file_desc: FileDescriptor) -> int:
    """Count CSV columns by parsing the first data line.

    Reads at most the first 8 KB so this is fast even for huge files.
    Falls back to max(field index) + 1 if the data is too short.
    """
    chunk = data[:8192].decode(file_desc.encoding, errors="replace")
    lines = chunk.splitlines()

    if file_desc.ignore_header_lines >= len(lines):
        # Not enough lines to peek - derive minimum from meta.xml indices
        indices = []
        if file_desc.id_index is not None:
            indices.append(file_desc.id_index)
        if file_desc.coreid_index is not None:
            indices.append(file_desc.coreid_index)
        for f in file_desc.fields:
            if f.index is not None:
                indices.append(f.index)
        return max(indices) + 1 if indices else 0

    first_data_line = lines[file_desc.ignore_header_lines]
    delimiter = file_desc.fields_terminated_by
    quotechar = file_desc.fields_enclosed_by

    if not quotechar:
        return len(first_data_line.split(delimiter))

    import csv as csv_std
    reader = csv_std.reader([first_data_line], delimiter=delimiter, quotechar=quotechar)
    return len(next(reader, []))


def _build_column_names(file_desc: FileDescriptor, total_cols: int) -> list[str]:
    """Build the ordered list of column names for a CSV file.

    Priority (highest first):
    1. _id / _coreid  - the reserved identifier columns
    2. Short term name from meta.xml field declarations
    3. _field_{i}     - fallback for undeclared columns
    """
    names = [f"_field_{i}" for i in range(total_cols)]

    # Reserved identifier column
    if file_desc.is_core and file_desc.id_index is not None:
        if file_desc.id_index < total_cols:
            names[file_desc.id_index] = "_id"
    elif not file_desc.is_core and file_desc.coreid_index is not None:
        if file_desc.coreid_index < total_cols:
            names[file_desc.coreid_index] = "_coreid"

    # Term-based names (only where a reserved name has not already been set)
    for field in file_desc.fields:
        if field.index is not None and field.index < total_cols:
            if names[field.index] == f"_field_{field.index}":
                names[field.index] = _short_name(field.term)

    return names


# ---------------------------------------------------------------------------
# Schema building
# ---------------------------------------------------------------------------

def _build_parquet_schema(
    col_names: list[str],
    file_desc: FileDescriptor,
    archive_name: str,
    conversion_mode: str,
) -> pa.Schema:
    """Build a PyArrow schema with DwC-A column-level and file-level metadata.

    Column metadata keys:
      dwca:term          - full DwC term URI (when known)
      dwca:index         - original column index in the CSV
      dwca:has_default   - "true" when a default was applied
      dwca:default_value - the default value

    File-level metadata keys:
      dwca2parquet:version, dwca:rowType, dwca:source_archive,
      dwca:is_core, dwca:conversion_mode, dwca:converted_at
    """
    field_by_index = {f.index: f for f in file_desc.fields if f.index is not None}

    pa_fields = []
    for i, col_name in enumerate(col_names):
        fd = field_by_index.get(i)
        col_meta: dict[str, str] = {"dwca:index": str(i)}
        if fd is not None:
            col_meta["dwca:term"] = fd.term
            if fd.default is not None:
                col_meta["dwca:has_default"] = "true"
                col_meta["dwca:default_value"] = fd.default
        pa_fields.append(pa.field(col_name, pa.string(), metadata=col_meta))

    # Default-only columns (no CSV index) are appended after the CSV columns
    for fd in file_desc.fields:
        if fd.index is None and fd.default is not None:
            col_meta = {
                "dwca:term": fd.term,
                "dwca:has_default": "true",
                "dwca:default_value": fd.default,
            }
            pa_fields.append(pa.field(_short_name(fd.term), pa.string(), metadata=col_meta))

    file_meta = {
        "dwca2parquet:version": __version__,
        "dwca:rowType": file_desc.row_type,
        "dwca:source_archive": archive_name,
        "dwca:is_core": "true" if file_desc.is_core else "false",
        "dwca:conversion_mode": conversion_mode,
        "dwca:converted_at": datetime.now(timezone.utc).isoformat(),
    }

    return pa.schema(pa_fields, metadata=file_meta)


# ---------------------------------------------------------------------------
# Default value application
# ---------------------------------------------------------------------------

def _apply_defaults(
    batch: pa.RecordBatch,
    file_desc: FileDescriptor,
    col_names: list[str],
    default_only_names: list[str],
) -> pa.RecordBatch:
    """Apply meta.xml default values to a record batch.

    Two cases:
    - Default-only field (index=None): appends a new column entirely filled
      with the default value.
    - Field with both index and default: replaces null or empty-string values
      with the default; non-empty values are left unchanged.

    Parameters
    ----------
    batch : pa.RecordBatch
        Batch from the CSV reader, already renamed to actual column names.
    file_desc : FileDescriptor
        Descriptor for the data file being processed.
    col_names : list of str
        Ordered column names for the CSV columns (same length as batch width).
    default_only_names : list of str
        Names of default-only columns to append (in declaration order).

    Returns
    -------
    pa.RecordBatch
        Batch with defaults applied and default-only columns appended.
    """
    arrays: dict[str, pa.Array] = {
        name: batch.column(name) for name in batch.schema.names
    }

    for fd in file_desc.fields:
        if fd.default is None:
            continue

        if fd.index is None:
            # Default-only: create a new column filled entirely with the default
            arrays[_short_name(fd.term)] = pa.array(
                [fd.default] * batch.num_rows, type=pa.string()
            )
        elif fd.index < len(col_names):
            # Fill null and empty-string cells with the default
            actual_name = col_names[fd.index]
            if actual_name in arrays:
                col = arrays[actual_name]
                is_null = pc.is_null(col)
                # pc.equal returns null when col is null, so fill_null before or-ing
                is_empty = pc.fill_null(
                    pc.equal(col, pa.scalar("", pa.string())), False
                )
                needs_default = pc.or_(is_null, is_empty)
                arrays[actual_name] = pc.if_else(
                    needs_default, pa.scalar(fd.default, pa.string()), col
                )

    all_names = list(batch.schema.names) + default_only_names
    return pa.record_batch(
        [arrays[name] for name in all_names],
        names=all_names,
    )


# ---------------------------------------------------------------------------
# Single-file conversion
# ---------------------------------------------------------------------------

def _convert_data_file(
    zf: zipfile.ZipFile,
    file_desc: FileDescriptor,
    output_path: Path,
    archive_name: str,
    conversion_mode: str = "raw",  # "raw" or "interpreted"
) -> int:
    """Convert one DwC-A CSV data file to a Parquet file.

    Streams the CSV in batches so archives larger than available RAM are
    handled correctly.

    Parameters
    ----------
    zf : zipfile.ZipFile
        Open zip file handle for the archive.
    file_desc : FileDescriptor
        Descriptor for the data file to convert.
    output_path : Path
        Destination path for the output Parquet file.
    archive_name : str
        Name of the source archive (embedded in Parquet metadata).
    conversion_mode : str
        "raw" or "interpreted" (type casting and geometry are applied by the caller
        in interpreted mode).

    Returns
    -------
    int
        Number of rows written.
    """
    data = zf.read(file_desc.filename)

    total_cols = _peek_column_count(data, file_desc)
    col_names = _build_column_names(file_desc, total_cols)

    # Names of default-only columns to append after the CSV columns
    default_only_names = [
        _short_name(fd.term)
        for fd in file_desc.fields
        if fd.index is None and fd.default is not None
    ]

    pa_schema = _build_parquet_schema(col_names, file_desc, archive_name, conversion_mode)

    read_options = pa_csv.ReadOptions(
        skip_rows=file_desc.ignore_header_lines,
        autogenerate_column_names=True,
    )
    parse_options = pa_csv.ParseOptions(
        delimiter=file_desc.fields_terminated_by,
        # quote_char=False disables quoting when no enclosure character is set
        quote_char=file_desc.fields_enclosed_by or False,
    )
    convert_options = pa_csv.ConvertOptions(
        # Force all CSV columns to string so raw values are preserved exactly
        column_types={f"f{i}": pa.string() for i in range(total_cols)},
        # Keep empty strings as empty strings, not null
        strings_can_be_null=False,
    )

    reader = pa_csv.open_csv(
        io.BytesIO(data),
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )

    row_count = 0
    with pq.ParquetWriter(output_path, pa_schema, compression="zstd") as writer:
        for batch in reader:
            renamed = batch.rename_columns(col_names)
            processed = _apply_defaults(renamed, file_desc, col_names, default_only_names)
            # Build a Table with the full schema (carries column + file metadata)
            table = pa.table(
                {name: processed.column(name) for name in pa_schema.names},
                schema=pa_schema,
            )
            writer.write_table(table)
            row_count += batch.num_rows

    return row_count


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def convert(
    archive: str | Path,
    output_dir: str | Path | None = None,
    *,
    interpreted: bool = False,
    geometry: bool | None = None,
) -> ConversionResult:
    """Convert a Darwin Core Archive to Parquet files.

    Parameters
    ----------
    archive : str or Path
        Path to the DwC-A zip file.
    output_dir : str or Path or None
        Directory to write output files into.  Created if it does not exist.
        Defaults to ``<archive_stem>_parquet/`` next to the archive.
    interpreted : bool
        If True, apply data types, geometry, and other interpretations to
        known Darwin Core fields (interpreted mode).
    geometry : bool or None
        Whether to create a GeoParquet geometry column when coordinates are
        present.  None (default) uses the mode-appropriate default: True in
        interpreted mode, False in raw mode.  Pass True to force geometry in
        raw mode, or False to suppress it in interpreted mode.

    Returns
    -------
    ConversionResult
        Describes all output files and conversion statistics.
    """
    archive = Path(archive)
    if output_dir is None:
        output_dir = archive.parent / (archive.stem + "_parquet")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    warnings_list: list[str] = []
    conversion_mode = "interpreted" if interpreted else "raw"

    # Resolve geometry default: on in interpreted mode, off in raw mode
    create_geometry = interpreted if geometry is None else geometry

    with zipfile.ZipFile(archive) as zf:
        # Locate meta.xml, handling archives that wrap content in a subdirectory
        meta_candidates = [n for n in zf.namelist() if n.endswith("meta.xml")]
        if not meta_candidates:
            raise ValueError(f"No meta.xml found in {archive.name}")
        meta_xml_entry = meta_candidates[0]
        # The directory prefix of meta.xml, used to locate sibling data files
        meta_dir = meta_xml_entry.rsplit("/", 1)[0] + "/" if "/" in meta_xml_entry else ""

        descriptor = parse_meta_xml(zf.read(meta_xml_entry).decode("utf-8"))
        archive_name = archive.name
        used_filenames: set[str] = set()

        # --- Core file ---
        core_filename = _output_filename(descriptor.core.row_type, used_filenames)
        used_filenames.add(core_filename)
        core_path = output_dir / core_filename
        core_row_count = _convert_data_file(
            zf, descriptor.core, core_path, archive_name, conversion_mode
        )

        # --- Extension files ---
        extension_paths: list[Path] = []
        extension_row_types: list[str] = []
        extension_row_counts: list[int] = []

        for ext in descriptor.extensions:
            ext_filename = _output_filename(ext.row_type, used_filenames)
            used_filenames.add(ext_filename)
            ext_path = output_dir / ext_filename
            ext_row_count = _convert_data_file(
                zf, ext, ext_path, archive_name, conversion_mode
            )
            extension_paths.append(ext_path)
            extension_row_types.append(ext.row_type)
            extension_row_counts.append(ext_row_count)

        # --- Metadata file (eml.xml or equivalent) ---
        eml_path = None
        if descriptor.metadata_filename:
            metadata_entry = meta_dir + descriptor.metadata_filename
            if metadata_entry in zf.namelist():
                dest_name = descriptor.metadata_filename.rsplit("/", 1)[-1]
                dest = output_dir / dest_name
                dest.write_bytes(zf.read(metadata_entry))
                eml_path = dest
            else:
                warnings_list.append(
                    f"Metadata file {descriptor.metadata_filename!r} declared in "
                    f"meta.xml but not found in the archive."
                )

    # --- Geometry (GeoParquet) ---
    has_geometry = False
    geometry_null_count = 0
    if create_geometry:
        has_geometry, geometry_null_count = add_geometry_to_parquet(core_path)

    return ConversionResult(
        core_path=core_path,
        core_row_type=descriptor.core.row_type,
        core_row_count=core_row_count,
        has_geometry=has_geometry,
        geometry_null_count=geometry_null_count,
        extension_paths=extension_paths,
        extension_row_types=extension_row_types,
        extension_row_counts=extension_row_counts,
        denormalized_path=None,
        eml_path=eml_path,
        conversion_mode=conversion_mode,  # "raw" or "interpreted"
        type_conversion_failures={},
        warnings=warnings_list,
        elapsed_seconds=time.monotonic() - started,
    )
