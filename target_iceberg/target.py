"""Iceberg target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_iceberg.sinks import (
    IcebergSink,
)


class TargetIceberg(Target):
    """Sample target for iceberg."""

    name = "target-iceberg"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "catalog_name",
            th.StringType,
            description="Name of Iceberg catalog",
        ),
        th.Property(
            "catalog_uri",
            th.StringType,
            description="SQL URI of Iceberg catalog",
        ),
        th.Property(
            "warehouse_path",
            th.StringType,
            description="Warehouse path",
        ),
        th.Property(
            "namespace",
            th.StringType,
            description="Namespace within catalog to create tables",
        ),
    ).to_dict()

    default_sink_class = IcebergSink


if __name__ == "__main__":
    TargetIceberg.cli()
