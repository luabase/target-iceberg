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
        th.Property(
            "partition_fields",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "stream",
                        th.StringType,
                        description="Name of the stream to be partitioned"
                    ),
                    th.Property(
                        "source_field",
                        th.StringType,
                        description="Name of the field to partition on",
                    ),
                    th.Property(
                        "transform",
                        th.StringType,
                        description="Transform to apply",
                        allowed_values=["hour", "day", "month", "year", "identity"],
                        default="identity",
                    ),
                )
            ),
            description="Partition fields and transforms",
        ),
    ).to_dict()

    default_sink_class = IcebergSink


if __name__ == "__main__":
    TargetIceberg.cli()
