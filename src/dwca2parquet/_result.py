"""ConversionResult dataclass returned by convert()."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ConversionResult:
    """
    Result of a DwC-A to Parquet conversion.

    Attributes
    ----------
    core_path : Path
        Path to the core Parquet file.
    core_row_type : str
        Full rowType URI of the core file.
    core_row_count : int
        Number of rows written to the core file.
    has_geometry : bool
        True if a GeoParquet geometry column was created.
    geometry_null_count : int
        Number of core rows with a null geometry (missing or invalid coordinates).
    extension_paths : list of Path
        Paths to extension Parquet files, in the same order as extension_row_types.
    extension_row_types : list of str
        Full rowType URIs for each extension file.
    extension_row_counts : list of int
        Row counts for each extension file.
    denormalized_path : Path or None
        Path to the denormalized Parquet file, or None if not requested.
    eml_path : Path or None
        Path to the copied metadata file (eml.xml or equivalent), or None if absent.
    conversion_mode : str
        "raw" or "typed".
    type_conversion_failures : dict
        Mapping of column name to failure count (typed mode only).
    warnings : list of str
        Any warnings generated during conversion.
    elapsed_seconds : float
        Total wall-clock time for the conversion.
    """

    core_path: Path
    core_row_type: str
    core_row_count: int
    has_geometry: bool
    geometry_null_count: int
    extension_paths: list[Path]
    extension_row_types: list[str]
    extension_row_counts: list[int]
    denormalized_path: Path | None
    eml_path: Path | None
    conversion_mode: str
    type_conversion_failures: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0
