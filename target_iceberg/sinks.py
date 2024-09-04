"""iceberg target sink class, which handles writing streams."""

from __future__ import annotations
import os
import shutil
from typing import Sequence

import gcsfs

from . import conversions

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import (
    HourTransform,
    DayTransform,
    MonthTransform,
    YearTransform,
    IdentityTransform,
)
from singer_sdk import Target
from singer_sdk.sinks import BatchSink


class IcebergSink(BatchSink):
    """iceberg target sink class."""

    max_size = 100000

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: Sequence[str] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._catalog = None

    @property
    def catalog(self):
        if self._catalog is None:
            self._catalog = SqlCatalog(
                self.config.get("catalog_name"),
                **{
                    "uri": self.config.get("catalog_uri"),
                    "warehouse": self.config.get("warehouse_path"),
                },
            )

        return self._catalog

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        if "records" not in context:
            context["records"] = []

        context["records"].append(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy

        namespace = self.config.get("namespace")
        table_name = self.stream_name
        table_id = f"{namespace}.{table_name}"

        try:
            self.catalog.create_namespace(namespace)
            self.logger.info(f"Namespace {self.config.get('namespace')} created")
        except NamespaceAlreadyExistsError:
            pass

        pa_schema = conversions.singer_to_pyarrow_schema(self, self.schema)
        try:
            iceberg_schema = conversions.pyarrow_to_pyiceberg_schema(pa_schema)
        except Exception as e:
            self.logger.error(
                f"Error converting PyArrow schema to PyIceberg schema: {e}"
            )
            return
        
        partition_spec = self._partition_config_to_partition_spec(
            self.config.get("partition_fields"), iceberg_schema
        )

        iceberg_table = None
        try:
            iceberg_table = self.catalog.load_table(table_id)
            self.logger.info(
                f"Table {table_id} already exists, checking for schema compatibility"
            )
            iceberg_column_names = set([f.name for f in iceberg_table.schema().fields])
            pa_column_names = set(pa_schema.names)

            if pa_column_names - iceberg_column_names:
                self.logger.info(
                    f"Found new columns in the stream: {pa_column_names - iceberg_column_names}"
                )
                self.logger.info("Adding new columns to the table")
                new_iceberg_schema = conversions.pyarrow_to_pyiceberg_schema(
                    pa_schema
                )
                try:
                    with iceberg_table.update_schema() as update_schema:
                        update_schema.union_by_name(new_iceberg_schema)
                except Exception as e:
                    self.logger.error(f"Error updating schema: {e}")
                    return
            elif iceberg_column_names - pa_column_names:
                self.logger.warning(
                    f"Existing columns missing in stream: {iceberg_column_names - pa_column_names}"
                )

        except NoSuchTableError:
            if partition_spec:
                iceberg_table = self.catalog.create_table(
                    identifier=table_id,
                    schema=pa_schema,
                    partition_spec=partition_spec,
                )
            else:
                iceberg_table = self.catalog.create_table(
                    identifier=table_id,
                    schema=iceberg_schema
                )
            self.logger.info(f"Table {self.stream_name} created")

        self.logger.info(
            f"Writing batch to iceberg table ({len(context['records'])} records)"
        )
        try:
            # Iceberg can rewrite field-ids, so we need to re-convert the actual table schema back to PyArrow before writing
            pa_schema = iceberg_table.schema().as_arrow()
            pa_table = pa.Table.from_pylist(context['records'], schema=pa_schema)
            iceberg_table.append(pa_table)
        except Exception as e:
            self.logger.error(f"Error writing batch to iceberg table: {e}")
            return

        self._add_version_hint(iceberg_table)

    def _partition_config_to_partition_spec(self, partition_config, iceberg_schema):
        partition_fields = []
        if not partition_config:
            return None

        for field in partition_config:
            if field.get("stream") != self.stream_name:
                continue

            source_id = self._get_field_id_by_name(iceberg_schema, field.get("source_field"))

            if not source_id:
                self.logger.error("Could not found source field", field.get("source_field"))
                return

            partition_fields.append(
                PartitionField(
                    source_id=source_id,
                    field_id=source_id+1000,
                    name=field.get("field_name") + "_" + field.get("transform"),
                    transform=self._get_transform(field.get("transform"))
                )
            )
        return PartitionSpec(partition_fields) if partition_fields else None

    def _get_transform(self, transform_name):
        match transform_name:
            case "hour":
                return HourTransform
            case "day":
                return DayTransform
            case "month":
                return MonthTransform
            case "year":
                return YearTransform
            case "identity":
                return IdentityTransform
            case _:
                raise ValueError(f"Unsupported transform: {transform_name}")

    def _get_field_id_by_name(self, iceberg_schema, field_name):
        for field in iceberg_schema.fields:
            if field.name == field_name:
                return field.field_id
        return None

    def _add_version_hint(self, iceberg_table):
        metadata_location = iceberg_table.metadata_location
        protocol = metadata_location.split(":")[0]

        if protocol == "file":
            metadata_location = metadata_location[7:]
        elif protocol == "gs":
            metadata_location = metadata_location[5:]
        else:
            self.logger.error(f"Unsupported metadata location: {metadata_location}")
            return

        metadata_dir = os.path.dirname(metadata_location)
        new_metadata_file = os.path.join(metadata_dir, "v1.metadata.json")
        version_hint_file = os.path.join(metadata_dir, "version-hint.text")

        if protocol == "file":
            shutil.copy(metadata_location, new_metadata_file)
            with open(version_hint_file, "w") as f:
                f.write("1")
        elif protocol == "gs":
            fs = gcsfs.GCSFileSystem()
            fs.copy(metadata_location, new_metadata_file)
            with fs.open(version_hint_file, "w") as f:
                f.write("1")

        self.logger.info(f"Copied metadata file to {new_metadata_file}")
        self.logger.info(f"Created {version_hint_file} with content '1'")
