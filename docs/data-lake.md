# Data lake

A broker started with the `iceberg`, `delta` or `parquet` subcommand writes each record of a
topic that has a [schema](schema-registry.md) to a table or a file for that topic, as well as
to the topic. [Commands](commands.md#broker) shows how to start such a broker. The broker only
adds rows to a table. It does not update or delete them.

- [Where the broker writes](#where-the-broker-writes)
- [Removing records from a topic](#removing-records-from-a-topic)
- [Removing data from a table](#removing-data-from-a-table)

## Where the broker writes

| Format | Table or file |
| --- | --- |
| Apache Iceberg | The table named after the topic, in the namespace `--namespace` (default `nisshi`), in the catalog `--catalog`. |
| Delta Lake | The table at `<location>/<database>.<topic>`, where `<location>` is `--location` and `<database>` is `--database` (default `nisshi`). The broker does not register the table in a catalog, so a query engine reads it by its path. |
| Apache Parquet | One file for each batch of records, under `<location>/<topic>/<partition>/`, where `<location>` is `--location` and `<partition>` is the partition number, padded with zeros. |

Each row has a `meta` column with the record's partition (`meta.partition`) and a timestamp
(`meta.timestamp`). The type of `meta.timestamp` depends on the topic's schema:

- Avro and Protocol Buffers: the record's timestamp, in microseconds, as a timestamp without a
  time zone.
- JSON Schema: the record's timestamp, as an RFC 3339 string in UTC.

## Removing records from a topic

DeleteRecords, retention (`retention.ms` and `retention.bytes`) and compaction remove records
from the Kafka log only. The table keeps its rows. Only the PostgreSQL and SQLite engines apply
retention and compaction, as [Topic configuration](compatibility.md#topic-configuration)
describes.

A topic with `nisshi.lake.sink=true` stores its records only in the table, so the table holds
the only copy of them.

## Removing data from a table

The broker does not delete rows or files from a table. It does not expire Apache Iceberg
snapshots, and it does not vacuum a Delta Lake table. To remove data from a table, run the
table format's own commands in your query engine, outside Nisshi. To remove the rows that
retention removed from a topic, select them with a condition on `meta.timestamp`. For a JSON
Schema topic, the condition casts `meta.timestamp` from a string to a timestamp.

Both table formats remove data in two steps. A delete removes the rows from the table's current
version, and the old data files stay in storage, so that a query can still read an earlier
version of the table. A second command deletes those files.

### Apache Iceberg

1. Remove the rows with [`DELETE FROM`][iceberg-delete], through the catalog that the broker
   writes to, for example `DELETE FROM nisshi.<topic> WHERE <condition>` in Apache Spark.
2. Delete the old data files with the [`expire_snapshots`][iceberg-expire] procedure. It expires
   the snapshots older than its `older_than` argument (default: 5 days ago), keeps the last
   `retain_last` snapshots (default: 1), and deletes each data file that no remaining snapshot
   references.
3. Optionally, delete the files that no table metadata references, such as the files of a failed
   write, with the [`remove_orphan_files`][iceberg-orphan] procedure. By default it deletes only
   files older than 3 days.

### Delta Lake

1. Remove the rows with [`DELETE FROM`][delta-delete], by the table's path. For a broker started
   with `delta --location s3://lake`, run
   ``DELETE FROM delta.`s3://lake/nisshi.<topic>` WHERE <condition>`` in Apache Spark. The
   broker adds `/` between `--location` and the table name, so with a `--location` that ends in
   `/` the path has `//` in it. DuckDB 2.0 reads Delta Lake tables, and
   [does not support][duckdb-delta] `DELETE` on them.
2. Delete the old data files with [`VACUUM`][delta-vacuum]. It deletes the files that the table
   no longer references and that are older than the table's `delta.deletedFileRetentionDuration`
   (default: 7 days). Delta Lake does not run `VACUUM` by itself.

On the PostgreSQL, memory, S3 and Google Cloud Storage engines, the broker runs Delta Lake's
`OPTIMIZE` in its maintenance run. The SQLite engine does not. `OPTIMIZE` rewrites a table's
files, and the files that it replaces also stay in storage until `VACUUM` deletes them.

### Apache Parquet

The Parquet files belong to no table. Delete or rewrite the files under `--location` with your
storage's own tools.

[duckdb-delta]: https://duckdb.org/docs/2.0/core_extensions/delta.html#limitations
[delta-delete]: https://docs.delta.io/delta-update/#delete-from-a-table
[delta-vacuum]: https://docs.delta.io/delta-utility/#remove-files-no-longer-referenced-by-a-delta-table
[iceberg-delete]: https://iceberg.apache.org/docs/latest/spark-writes/#delete-from
[iceberg-expire]: https://iceberg.apache.org/docs/latest/spark-procedures/#expire_snapshots
[iceberg-orphan]: https://iceberg.apache.org/docs/latest/spark-procedures/#remove_orphan_files
