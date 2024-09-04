"""
Conversions from Singer schema to PyArrow schema to PyIceberg schema.

Taken from https://github.com/SidetrekAI/target-iceberg/blob/main/target_iceberg/iceberg.py
"""

from typing import cast, Any, List, Tuple, Union
import pyarrow as pa  # type: ignore
from pyarrow import Schema as PyarrowSchema, Field as PyarrowField
from pyiceberg.schema import Schema as PyicebergSchema
from pyiceberg.io.pyarrow import pyarrow_to_schema


# Borrowed from https://github.com/crowemi/target-s3/blob/main/target_s3/formats/format_parquet.py
def singer_to_pyarrow_schema_without_field_ids(
    self, singer_schema: dict
) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""

    def process_anyof_schema(anyOf: List) -> Tuple[List, Union[str, None]]:
        """This function takes in original array of anyOf's schema detected
        and reduces it to the detected schema, based on rules, right now
        just detects whether it is string or not.
        """
        types, formats = [], []
        for val in anyOf:
            typ = val.get("type")
            if val.get("format"):
                formats.append(val["format"])
            if type(typ) is not list:
                types.append(typ)
            else:
                types.extend(typ)
        types = list(set(types))
        formats = list(set(formats))
        ret_type = []
        if "string" in types:
            ret_type.append("string")
        if "null" in types:
            ret_type.append("null")
        return ret_type, formats[0] if formats else None

    def get_pyarrow_schema_from_array(items: dict, level: int = 0):
        type = cast(list[Any], items.get("type"))
        any_of_types = items.get("anyOf")

        if any_of_types:
            self.logger.info("array with anyof type schema detected.")
            type, _ = process_anyof_schema(anyOf=any_of_types)

        if "string" in type:
            return pa.string()
        elif "integer" in type:
            return pa.int64()
        elif "number" in type:
            return pa.decimal128(38,10)
        elif "boolean" in type:
            return pa.bool_()
        elif "array" in type:
            subitems = cast(dict, items.get("items"))
            return pa.list_(get_pyarrow_schema_from_array(items=subitems, level=level))
        elif "object" in type:
            subproperties = cast(dict, items.get("properties"))
            return pa.struct(
                get_pyarrow_schema_from_object(
                    properties=subproperties, level=level + 1
                )
            )
        else:
            return pa.null()

    def get_pyarrow_schema_from_object(properties: dict, level: int = 0):
        """
        Returns schema for an object.
        """
        fields = []

        for key, val in properties.items():
            if "type" in val.keys():
                type = val["type"]
                format = val.get("format")
            elif "anyOf" in val.keys():
                type, format = process_anyof_schema(val["anyOf"])
            else:
                self.logger.warning("type information not given")
                type = ["string", "null"]

            if "integer" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.int64(), nullable=nullable))
            elif "number" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.decimal128(38,10), nullable=nullable))
            elif "boolean" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.bool_(), nullable=nullable))
            elif "string" in type:
                nullable = "null" in type
                if format and level == 0:
                    # this is done to handle explicit datetime conversion
                    # which happens only at level 1 of a record
                    if format == "date":
                        fields.append(pa.field(key, pa.date64(), nullable=nullable))
                    elif format == "time":
                        fields.append(pa.field(key, pa.time64(), nullable=nullable))
                    else:
                        fields.append(
                            pa.field(
                                key, pa.timestamp("us", tz="UTC"), nullable=nullable
                            )
                        )
                else:
                    fields.append(pa.field(key, pa.string(), nullable=nullable))
            elif "array" in type:
                nullable = "null" in type
                items = val.get("items")
                if items:
                    item_type = get_pyarrow_schema_from_array(items=items, level=level)
                    if item_type == pa.null():
                        self.logger.warn(
                            f"""key: {key} is defined as list of null, while this would be
                                correct for list of all null but it is better to define
                                exact item types for the list, if not null."""
                        )
                    fields.append(pa.field(key, pa.list_(item_type), nullable=nullable))
                else:
                    self.logger.warn(
                        f"""key: {key} is defined as list of null, while this would be
                            correct for list of all null but it is better to define
                            exact item types for the list, if not null."""
                    )
                    fields.append(pa.field(key, pa.list_(pa.null()), nullable=nullable))
            elif "object" in type:
                nullable = "null" in type
                prop = val.get("properties")
                inner_fields = get_pyarrow_schema_from_object(
                    properties=prop, level=level + 1
                )
                if not inner_fields:
                    self.logger.warn(
                        f"""key: {key} has no fields defined, this may cause
                            saving parquet failure as parquet doesn't support
                            empty/null complex types [array, structs] """
                    )
                fields.append(pa.field(key, pa.struct(inner_fields), nullable=nullable))

        return fields

    properties = singer_schema["properties"]
    pyarrow_schema = pa.schema(get_pyarrow_schema_from_object(properties=properties))

    return pyarrow_schema


def assign_pyarrow_field_ids(self, fields: List[PyarrowField], field_id: int = 0) -> Tuple[List[PyarrowField], int]:
    new_fields = []
    
    def process_type(field_type: pa.DataType, metadata: dict) -> Tuple[pa.DataType, int]:
        nonlocal field_id
        
        if isinstance(field_type, pa.StructType):
            new_fields = []
            for f in field_type:
                new_f, field_id = process_field(f)
                new_fields.append(new_f)
            return pa.struct(new_fields), field_id
        elif isinstance(field_type, pa.ListType):
            field_id += 1
            list_metadata = metadata.copy()
            list_metadata[b'PARQUET:field_id'] = str(field_id).encode()
            
            # Assign field ID to the list's value type
            field_id += 1
            value_metadata = {b'PARQUET:field_id': str(field_id).encode()}
            new_value_type, field_id = process_type(field_type.value_type, value_metadata)
            
            # Create a new field for the list's item type
            item_field = pa.field('item', new_value_type, metadata=value_metadata)
            
            return pa.list_(item_field), field_id
        elif isinstance(field_type, pa.MapType):
            field_id += 1
            map_metadata = metadata.copy()
            map_metadata[b'PARQUET:field_id'] = str(field_id).encode()
            
            field_id += 1
            key_metadata = {b'PARQUET:field_id': str(field_id).encode()}
            new_key_type, field_id = process_type(field_type.key_type, key_metadata)
            
            field_id += 1
            item_metadata = {b'PARQUET:field_id': str(field_id).encode()}
            new_item_type, field_id = process_type(field_type.item_type, item_metadata)
            
            return pa.map_(new_key_type, new_item_type), field_id
        else:
            return field_type, field_id

    def process_field(field: PyarrowField) -> Tuple[PyarrowField, int]:
        nonlocal field_id
        
        existing_id = field.metadata.get(b'PARQUET:field_id') if field.metadata else None
        
        if existing_id is not None:
            return field, max(field_id, int(existing_id))
        
        field_id += 1
        new_metadata = field.metadata.copy() if field.metadata else {}
        new_metadata[b'PARQUET:field_id'] = str(field_id).encode()
        
        new_type, field_id = process_type(field.type, new_metadata)
        new_field = pa.field(field.name, new_type, nullable=field.nullable, metadata=new_metadata)
        
        return new_field, field_id

    for field in fields:
        new_field, field_id = process_field(field)
        new_fields.append(new_field)
    
    return new_fields, field_id

def singer_to_pyarrow_schema(self, singer_schema: dict) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""
    pa_schema = singer_to_pyarrow_schema_without_field_ids(self, singer_schema)
    pa_fields_with_field_ids, _ = assign_pyarrow_field_ids(self, pa_schema)
    return pa.schema(pa_fields_with_field_ids)


def pyarrow_to_pyiceberg_schema(pa_schema: PyarrowSchema) -> PyicebergSchema:
    """Convert pyarrow schema to pyiceberg schema."""
    pyiceberg_schema = pyarrow_to_schema(pa_schema)
    return pyiceberg_schema
