# Connections and generator envelopes

A connection is a named entry under `"connections"`. `kind` is case-sensitive. With exactly one connection, generators don't need `connection`; with two or more, **every** generator must name one.

```json
"connections": { "pg": { "kind": "postgres", "connectionConfigs": { ... } } }
```

`kind` values: `kafka`, `postgres`, `mysql`, `sqlServer`, `oracle`, `databricks`, `motherduck`, `s3`, `googleCloudStorage`, `azureBlobStorage`, `fileSystem`, `redis`, `pubsub`, `kurrentdb` (`eventstore` legacy), `webhook`.

Exact schemas and many examples: `references/docs/connections/<file>.md` (run `python3 scripts/fetch_docs.py` first).

---

## Kafka — `kind: "kafka"`

```json
{
  "kind": "kafka",
  "producerConfigs": {
    "bootstrap.servers": "localhost:9092",
    "key.serializer": "io.shadowtraffic.kafka.serdes.JsonSerializer",
    "value.serializer": "io.shadowtraffic.kafka.serdes.JsonSerializer"
  }
}
```

Generator: `topic` (required), `key?`, `value?`, `headers?`, `producerConfigs?`.

- Required producer configs: `bootstrap.servers`, `key.serializer`, `value.serializer`.
- Built-in serializers include all Apache Kafka primitives plus `io.shadowtraffic.kafka.serdes.JsonSerializer`, Confluent `KafkaAvroSerializer`, `KafkaJsonSerializer`, `KafkaJsonSchemaSerializer`, `KafkaProtobufSerializer`, `WrapperKeySerializer`.
- `headers` is an array of `{ "key": "region", "value": "us-east-1" }`; `value` is coerced to a UTF-8 string.
- Topics are auto-created by default. Control with an **object**:
  ```json
  "topicPolicy": { "policy": "create", "partitions": 6, "replicationFactor": 2,
                   "topicConfigs": { "retention.ms": "86400000" } }
  ```
  `policy`: `create` | `dropAndCreate` | `manual`.
- Schema Registry: add `"schema.registry.url": "http://localhost:8081"` (plus `basic.auth.user.info` if needed) and a Confluent serializer. ShadowTraffic uses an explicit schema hint if supplied, otherwise downloads from the registry (TopicNameStrategy or `schemaRegistrySubject`), otherwise infers one from your generator and registers it.
- Schema hints live in `localConfigs`: `avroSchemaHint`, `jsonSchemaHint`, `protobufSchemaHint`, `schemaRegistrySubject`; external Avro refs via connection-level `avroSchemas: [{subject, schema}]`.
- Avro union values must be wrapped with the type name: `{ "string": { "_gen": "uuid" } }`.
- `consumerConfigs` (`bootstrap.servers`, `key.deserializer`, `value.deserializer`) enable **loader** generators (`"kind": "loader"`) to read existing topics for `lookup`.
- Other knobs: `sendSynchronous: true` (slow, one at a time), `continueOnRuleException: true` (let data-quality violations through), `logLevel`.
- Confluent Cloud: `security.protocol: SASL_SSL`, `sasl.mechanism: PLAIN`, `sasl.jaas.config`, `basic.auth.credentials.source`, `basic.auth.user.info`.
- MSK IAM: `sasl.mechanism: AWS_MSK_IAM`, IAM login module/callback classes; set explicit partitions/replicationFactor.
- Custom serializer: mount a jar and run with `--entrypoint=java -cp '/home/shadowtraffic.jar:/home/serde.jar' io.shadowtraffic.generator.entrypoint`.

---

## Postgres — `kind: "postgres"`

```json
{
  "kind": "postgres",
  "tablePolicy": "create",
  "connectionConfigs": {
    "host": "localhost", "port": 5432, "db": "mydb",
    "username": "postgres", "password": "postgres"
  },
  "batchConfigs": { "lingerMs": 500, "batchRows": 5000 }
}
```

Generator: `table`, `row` (required), `op?` (`insert` default / `update` / `delete`), `where?`.

- `connectionConfigs` requires `host`, `port`, `db`; `username`/`password` are optional in the schema (most servers still need them).
- `tablePolicy`: `create` (default) | `dropAndCreate` (destructive, cascades) | `manual`.
- Batch default: every 20 ms or 500 rows, whichever first.
- Auto-DDL infers types (`text`, `double precision`, `boolean`, ...). Override with `pgHint` per field or `sqlDdlHint` for the whole table.
- `update`/`delete` require `where`: a map of column → value used for equality (`where` values may be functions/lookups).
- Timestamps: use `serialize: { "type": "postgresTimestamp" }` for real `timestamp` columns.

---

## MySQL — `kind: "mysql"`, SQL Server — `kind: "sqlServer"`, Oracle — `kind: "oracle"`

Same shape as Postgres (`table`, `row`, `op`, `where`; `connectionConfigs` requires `host`, `port`, `db`; `username`/`password` optional in the schema; `tablePolicy`, `batchConfigs`).

- Per-column type hints use `sqlHint` (string DDL type), or `sqlDdlHint` for the whole table.
- MySQL additionally supports transaction control via `op`: `start`, `commit`, `abort` — sequence them with a `stateMachine`.
- Oracle supports 19c+.
- Timestamps: `serialize` types `sqlServerTimestamp` / `databricksTimestamp` (Postgres has its own).
- SQL batch default: 20 ms or 500 rows.

---

## Databricks — `kind: "databricks"`

```json
{
  "kind": "databricks",
  "connectionConfigs": {
    "host": "https://xxx.cloud.databricks.com",
    "warehouseId": "abc123",
    "catalog": "main",
    "schema": "sandbox",
    "token": { "_gen": "env", "var": "DATABRICKS_TOKEN", "mask": true }
  }
}
```

Generator: `table`, `row` (required), `tags?`, `evolveSchema?`.

- Auth: PAT `token`, or OAuth M2M `clientId` + `clientSecret`.
- Auto-creates `schema` (`CREATE SCHEMA IF NOT EXISTS`); the `catalog` must exist. Tables auto-created; `sqlHint` / `sqlDdlHint` override.
- `tablePolicy`: `manual` to control tables yourself.
- `batchConfigs`, `statementTimeout`, `poolSize`.
- Loader generators supported (`kind: "loader"`).

---

## MotherDuck — `kind: "motherduck"`

Connect with `token` + `db`, or `jdbcUrl`; optional `queryParams`, `poolSize`.
Generator shapes:
- writer: `table`, `row`, optional `op`/`where` like other SQL connections
- reader: `"kind": "reader"`, `query` (string or function, e.g. `interpolate`) — runs real SELECT traffic and discards results

`tablePolicy`, `sqlHint`, `sqlDdlHint`, `batchConfigs` (default 1000 ms / 10 000 rows) work as expected. MotherDuck is the only connection with reader generators.

---

## S3 — `kind: "s3"`

```json
{
  "kind": "s3",
  "connectionConfigs": { "endpoint": "https://fly.storage.tigris.dev", "pathStyleAccess": true },
  "batchConfigs": { "lingerMs": 2000, "batchElements": 10000, "batchBytes": 5242880 },
  "writerConfigs": { "bufferDepth": 2, "concurrency": 4, "retries": 3 }
}
```

Generator: `bucket`, `bucketConfigs`, `data` (required).

- Credentials from `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (or `AWS_SESSION_TOKEN`) and `AWS_REGION`. Bucket must exist.
- `bucketConfigs`: `keyPrefix` (required), `format` (required: `json` | `jsonl` | `log` | `parquet`), `customSuffix`, `compression: "gzip"`, `pretty`, `explodeJsonlArrays`, `subdir`, `multiBlob`, `tableFormat: "iceberg"` + `iceberg` config.
- Objects roll as `<keyPrefix>-<ulid>.<format>`; default every 500 ms or 5000 elements.
- `subdir` may be a function (e.g. `forkKey`) and rolls independently per subdir.
- `multiBlob` writes several objects per event: map of name → `{ keyPrefix, subdir? }`; `data` must have matching keys.
- S3-compatible: Tigris, SeaweedFS, RustFS via `endpoint` (+ `pathStyleAccess`).
- Loader generators supported (read bucket for `lookup`).

---

## Google Cloud Storage — `kind: "googleCloudStorage"`

`connectionConfigs` requires `projectId` (optional `host`). Set `GOOGLE_APPLICATION_CREDENTIALS` to a mounted credential file. Bucket must exist.

Generator: `bucket`, `bucketConfigs` (`keyPrefix`, `format`, ...), `data`.
- Same file formats/compression/batching/`subdir` as S3 (GCS has no `multiBlob`).
- Works with GCS-compatible services (`host` / fake-gcs-server).
- Lookups can use `blobPrefix` to target one of several generators writing to the same bucket.

---

## Azure Blob Storage — `kind: "azureBlobStorage"`

`connectionConfigs` requires `connectionString` (use `env` + `mask`). Container must exist.

Generator: `container`, `containerConfigs` (`keyPrefix`, `format`, ...), `data`.
- Same formats/compression/batching/multiBlob (`multiBlob` is named the same as S3) and subdir behavior.

---

## Local filesystem — `kind: "fileSystem"`

```json
{ "kind": "fileSystem" }
```

Generator: `directory`, `fileConfigs`, `data` (required).

- `fileConfigs.filePrefix` (required) and `format` (required); plus `customSuffix`, `compression: "gzip"`, `pretty`, `explodeJsonlArrays`, `subdir`, `multiFile`, `tableFormat: "iceberg"`.
- Files roll as `<directory>/[<subdir>/]<filePrefix>-<n>.<format>`; default every 500 ms or 5000 elements; set any batch limit to `0` to flush every event.
- Directories are created automatically.
- Never point two generators at the same directory + prefix: writers contend/overwrite.

File formats (`s3`, `gcs`, `azure`, `fileSystem`): `json`, `jsonl`, `log`, `parquet`. `log` requires `data` to be a string. `gzip` is the only compression. Iceberg output is Parquet-only and requires a REST catalog (`namespace`, `table`, `catalogProps`).

---

## Redis — `kind: "redis"`

```json
{
  "kind": "redis",
  "connectionConfigs": { "host": "localhost", "port": 6379 }
}
```

Connection can also be `url`, or `clusterNodes: [...]`, plus `user`, `password`, `ssl`, `database`.

Generator: `op` (`set` | `hset` | `zadd`), `key`, `attributes`, optional `opParams`, `serialization`.
- `set`: `attributes: { value }`; `opParams` may set expiration (`ex`, `px`, ...).
- `hset`: `attributes: { field, value }`.
- `zadd`: `attributes: { score, member }` (score numeric).
- Values are treated as strings; set `"serialization": "json"` to JSON-encode key + writable attributes.
- `startupPolicy`: `flushDB`, `flushAll`, or `unlink` (with `unlinkPattern`, e.g. `"user:*:action"`). Destructive — only when intended.

---

## Pub/Sub — `kind: "pubsub"`

No credentials in config: set `GOOGLE_APPLICATION_CREDENTIALS`. Optional `connectionConfigs.emulatorHost`.

Generator: `projectId`, `topicId`, `topicConfigs: { serialization: "avro", schemaId: "..." }`, `data`.
- Avro only; an explicit `avroSchemaHint` for `data` is required. Schemas auto-register.
- Topics auto-created; `topicPolicy` controls it.

---

## KurrentDB — `kind: "kurrentdb"` (legacy `eventstore`)

`connectionConfigs: { "url": "..." }`.

Generator: `stream`, `eventType`, `data`, optional `streamSuffix`, `eventId`, `metadata`.
- Batches every 20 ms or 500 events; `batchConfigs`.

---

## Webhook — `kind: "webhook"`

```json
{ "kind": "webhook", "connectionConfigs": { "maxRequests": 20000 } }
```

Generator: `url`, `data` (required), optional `method` (default `POST`), `headers`, `queryParams`, `urlSuffix`.

- Body is always JSON; `content-type: application/json` is forced.
- `maxRequests` default 20000 (bounded by container `ulimit`); `0` makes requests fully synchronous.
- `urlSuffix` lets a state machine hit different routes over time.
- Inside Docker, `localhost` is the container: use `--net=host`, `host.docker.internal`, or a compose service name.
