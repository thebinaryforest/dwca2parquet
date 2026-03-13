"""dwca2parquet - Convert Darwin Core Archive files to Apache Parquet / GeoParquet."""

__version__ = "0.1.0"

from dwca2parquet._convert import convert
from dwca2parquet._result import ConversionResult

__all__ = ["convert", "ConversionResult"]
