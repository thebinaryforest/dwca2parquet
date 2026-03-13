"""Command-line interface for dwca2parquet."""

import click


@click.group()
@click.version_option()
def main() -> None:
    """Convert Darwin Core Archive files to Apache Parquet / GeoParquet."""


@main.command()
@click.argument("archive", type=click.Path(exists=True))
@click.option("-o", "--output-dir", default=None, help="Output directory.")
@click.option(
    "--interpreted/--raw",
    default=False,
    help="Interpret data: apply types, geometry, and date parsing to known Darwin Core fields.",
)
@click.option(
    "--geometry/--no-geometry",
    default=None,
    help=(
        "Create a geometry column from coordinates.  "
        "Default: on in interpreted mode, off in raw mode."
    ),
)
def convert(archive: str, output_dir: str | None, interpreted: bool, geometry: bool | None) -> None:
    """Convert a DwC-A archive to Parquet files."""
    raise NotImplementedError("Not implemented yet")
