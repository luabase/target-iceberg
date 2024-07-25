# target-iceberg

*Disclaimer*: This repository is in early stages of development and offers no guarantees of correctness or data safety. Use with caution. Bug reports and pull requests are welcome.

`target-iceberg` is a Singer/Meltano target for Apache Iceberg with support for the followng:

* PostgreSQL catalog backends
* Local and GCS storage backends
* Partitioning
* "Add column" schema evolution

Roadmap items include:

* Better source -> Iceberg type conversions. (See Known Issues.)
* Partition schema evolution

Other catalog backends (like Hive, Glue, or REST) or other storage backends (like S3) are not currently supported. You may find them in other Iceberg targets like (/taeefnajib/target-iceberg)[https://github.com/taeefnajib/target-iceberg] and (SidetrekAI/target-iceberg)[https://github.com/SidetrekAI/target-iceberg].

## Configuration

Take a look at `target.py` to see configuration options. Some notes:

- `warehouse_path` is a directory URI and needs either a `file://` or `gs://` prefix.

- `partition_fields` is an optional array. If you do have partition fields, you need to speficy a transformation (use `identity` if you just want to partition on the field as-is) as well as a stream name. (You can specify multiple partition fields for multiple streams.)

## Known Issues

The schema conversion code isn't very robust, and only sometimes works on nested columns. It works well for flat columns however.