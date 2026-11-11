# Topic Configuration

Nisshi reads its own settings from topic configuration, next to the Apache Kafka
settings such as `cleanup.policy`. Set them when you create a topic or change them
later, with any Apache Kafka client or with `nisshi topic create --config`:

```shell
nisshi topic create telemetry --config nisshi.virtual=true
```

## Keys

Nisshi keys start with `nisshi.`. SlateDB, Turso and the null engine ignore `nisshi.virtual` and
`nisshi.lake.sink`. SlateDB also ignores `nisshi.schema.validation`, and always validates.

| Key | Default | Applies to | Effect |
|-----|---------|------------|--------|
| `nisshi.batch` | `false` | proxy | Batches the Produce requests that the proxy receives for the topic. |
| `nisshi.batch.timeout_ms` | `10000` | proxy | Longest time in milliseconds that the proxy holds a batch before it sends it. |
| `nisshi.batch.max_records` | `1000` | proxy | Number of records at which the proxy sends a batch. |
| `nisshi.schema.validation` | `true` | every engine except SlateDB | With a schema registry, validates each produced message against the topic's schema. Set to `false` to accept any message, except on a topic with `nisshi.lake.sink`, which always validates. |
| `nisshi.virtual` | `false` | PostgreSQL, libSQL | Makes `topic/key` a topic that has the records of `topic` with the record key `key`. |
| `nisshi.lake.sink` | `false` | PostgreSQL, libSQL, S3, Google Cloud Storage, memory | With a schema registry and a data lake, stores the topic's records only in the lake table, not in the topic, so a fetch returns none of them. When a lake is configured, the broker writes every topic's records to it whatever this key says. DeleteRecords and retention do not remove records from the lake table, see [Data lake](data-lake.md#removing-records-from-a-topic). |
| `nisshi.lake.partition` | none | Delta Lake | Comma separated columns that the table is partitioned by. |
| `nisshi.lake.z_order` | none | Delta Lake | Comma separated columns that the table is Z-ordered by. |
| `nisshi.lake.normalize` | `false` | Delta Lake | Flattens nested fields into top level columns. |
| `nisshi.lake.normalize.separator` | `.` | Delta Lake | Separator that joins the names of the nested fields that `nisshi.lake.normalize` flattens. |
| `nisshi.lake.generate.<column>` | none | Delta Lake | Adds the column `<column>`. The value is `CAST(<field> AS DATE)`, `CAST(<field> AS INT)` or `CAST(<field> AS INTEGER)`. |

## Deprecated `tansu.` keys

Nisshi was called Tansu, and releases up to 0.7.0 use the prefix `tansu.` for these
keys. Each `tansu.` key is a deprecated alias of the `nisshi.` key with the same name
after the prefix: `tansu.lake.sink` is an alias of `nisshi.lake.sink`, and
`tansu.lake.generate.date` is an alias of `nisshi.lake.generate.date`.

- An alias works everywhere its `nisshi.` key works, so an existing topic keeps
  its behaviour and needs no change when you upgrade.
- When a topic sets both an alias and its `nisshi.` key, the `nisshi.` key applies and
  Nisshi ignores the alias.
- Nisshi stores a key under the name that you set, and DescribeConfigs returns that
  name. It does not rename a stored key.
- Nisshi logs one warning for each alias, the first time the alias is used after
  the broker or proxy starts. The warning names the key and the key that replaces it.
  It does not log the value, and it does not name a topic, because the broker or proxy
  logs it once for all topics. To list the topics that still use an alias, describe each
  topic and search for the prefix. The command uses `--all`, because SlateDB reports a stored
  key as a default config, and `--describe` alone lists only dynamic topic configs. It keeps
  the header lines, because only
  the header names the topic:
  `kafka-configs.sh --bootstrap-server localhost:9092 --describe --all --entity-type topics | grep -E '^(Dynamic|All) configs for topic|^  tansu\.'`

### Move a topic to the `nisshi.` keys

A release up to 0.7.0 reads only the `tansu.` keys. It stores a `nisshi.` key, ignores
it, and logs no warning. So move a topic in this order:

1. Upgrade every broker and proxy to this release.
2. Set the `nisshi.` key to the same value as the `tansu.` key, and keep both keys.
3. Delete the `tansu.` key when you no longer need to roll back to 0.7.0.

While any broker or proxy runs 0.7.0 or earlier, set a key under both names. Keep the two
at the same value: a release that reads only the `tansu.` key applies its value, so a change
to the `nisshi.` key alone is lost on a rollback and on a broker that has not been upgraded.

```shell
kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics \
  --entity-name telemetry --add-config nisshi.virtual=true
```

When you no longer need to roll back:

```shell
kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics \
  --entity-name telemetry --delete-config tansu.virtual
```

If a topic has only the `nisshi.` key, a rollback to 0.7.0 changes the topic:

- `nisshi.schema.validation=false`: PostgreSQL and libSQL validate each message again, so
  producers can get errors.
- `nisshi.lake.sink=true`: the broker stores the records in the topic again. The lake table
  stays complete. The topic has no records for the offsets that the broker produced while
  the key applied, and Kafka consumers skip those offsets.
- `nisshi.virtual=true`: `topic/key` is no longer a virtual topic.
- `nisshi.batch`: a 0.7.0 proxy does not batch the topic.
- `nisshi.lake.partition` and `nisshi.lake.z_order`: the Delta Lake writer ignores them.
- `nisshi.lake.normalize` and `nisshi.lake.generate.<column>`: the batches that the writer
  sends no longer match the schema of the existing table, so each write to the table
  fails, and so does the produce request.

Brokers share storage and topic configuration. While old and new brokers serve a topic
together, each applies its own rules to the same configuration. With `nisshi.lake.sink=true`,
new brokers write to the lake only, and old brokers write to the topic and the lake. The topic
then has gaps that Kafka consumers skip without an error.

A running proxy keeps the configuration that it read for a topic until it restarts, so restart
the proxy after you change a key.

### Removal policy

An alias stays until a later long term support (LTS) release. Nisshi removes an
alias only in an LTS release that follows, by at least one LTS cycle, the
release that announced the deprecation. The release notes announce the removal
before it happens.
