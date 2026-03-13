"""Command-line interface for dwca2parquet."""

import click


@click.group()
@click.version_option()
def main() -> None:
    """Convert Darwin Core Archive files to Apache Parquet / GeoParquet."""


@main.command()
@click.argument("archive", type=click.Path(exists=True))
@click.option("-o", "--output-dir", default=None, help="Output directory.")
def convert(archive: str, output_dir: str | None) -> None:
    """Convert a DwC-A archive to Parquet files."""
    raise NotImplementedError("Not implemented yet")
