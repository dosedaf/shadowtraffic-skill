# Generator configuration

Two levels:

- `localConfigs` — on one generator.
- `globalConfigs` — top-level, applies to all generators.
- If both define the same key, **local wins** (except `lookupScope`, which is global-only).

Each config value may itself be a function (dynamic rate, dynamic max, etc.).

## Local + global

### `throttleMs`
Minimum milliseconds between events from a generator. Integer or function.
```json
"localConfigs": { "throttleMs": 200 }
"localConfigs": { "throttleMs": { "_gen": "uniformDistribution", "bounds": [100, 300] } }
```

### `throughput`
Approximate max events/sec. A ceiling, not a guarantee. Independent of connection batching.
```json
"localConfigs": { "throughput": 20 }
```

### `maxEvents`
Stop after this many events. The generator goes dormant.
```json
"localConfigs": { "maxEvents": 5000 }
```

### `maxMs`
Stop after this many milliseconds of running.

### `maxHistoryEvents`
How many events to retain for `lookup` (default `1_000_000`). History is only stored when a lookup references the generator. Increase for richer joins, decrease to save memory.

### `delay`
Out-of-order events: delay a fraction of events by N ms. Delayed events free the generator to create the next event immediately; throttling applies when the event is released.
```json
"localConfigs": { "delay": { "rate": 0.1, "ms": 250 } }
```

### `discard`
Generate but do not send a fraction of events (simulates loss).
```json
"localConfigs": { "discard": { "rate": 0.02 } }
```
Optional `"retainHistory": true` makes discarded events visible to `lookup` anyway.

### `repeat`
Duplicate a fraction of events immediately (simulates retries/at-least-once).
```json
"localConfigs": { "repeat": { "rate": 0.05, "times": 1 } }
```
`times` is *additional* copies (`times: 1` → two total deliveries). May be a function.

## Local only

### `maxBytes`
Stop after writing N serialized bytes. Only Kafka, S3, GCS, Azure Blob, filesystem, webhooks; no-op elsewhere.

### `avroSchemaHint`
Map of generator key → Avro schema object, e.g. `{ "value": { ... } }`. Used by Avro serializers / Schema Registry.

### `jsonSchemaHint`
Map of generator key → JSON Schema object.

### `protobufSchemaHint`
Map of generator key → `{ "schemaFile": "/path/inside/container.proto", "message": "MessageName" }`.

### `schemaRegistrySubject`
Exact subject name to download/register a schema under (Kafka + Schema Registry).

### `sqlDdlHint`
Full `CREATE TABLE` statement for SQL connections; supersedes all `sqlHint` modifiers.

Deprecated equivalents still exist: `kafkaKeyAvroSchemaHint`, `kafkaValueAvroSchemaHint` (use `avroSchemaHint`), `kafkaKeyProtobufHint`, `kafkaValueProtobufHint` (use `protobufSchemaHint`).

## Global only

### `time`
Starting wallclock time as epoch **milliseconds**. Makes `now` start in the past/future.
```json
"globalConfigs": { "time": 1420099200000 }
```

### `timeMultiplier`
Speed of the simulated clock. `4` = 4× faster, `0.25` = 4× slower; `1` = real time. May be a function. Faster clock ⇒ generators do less per real second; slower ⇒ more.
```json
"globalConfigs": { "timeMultiplier": 4 }
```

### `lookupScope`
`instantaneous` (default) or `committed`. `committed` hides events from `lookup` until the target system acknowledges them. This changes startup behavior and can differ from `--stdout` timing.
```json
"globalConfigs": { "lookupScope": "committed" }
```

### `geolocation`
Dataset registration for the `geolocation` function:
```json
"globalConfigs": {
  "geolocation": {
    "countryFiles": { "United States": "/home/data/NAD_r15_TXT.csv" }
  }
}
```

## Deprecated (don't use in new configs)

- `events: { "exactly": N }` → `maxEvents`
- `throttle: { ... }` → `throttleMs`
- `history` → `maxHistoryEvents`

## Where to set connection batching

Not generator configs, but often confused with `throughput`:
- Kafka: producer configs (`linger.ms`, `batch.size`, `acks`, compression)
- Postgres/MySQL/SQL Server/Oracle: `batchConfigs: { lingerMs, batchRows }`
- Files/S3/GCS/Azure: `batchConfigs: { lingerMs, batchElements, batchBytes }` (`0` = flush every event)

`throughput`/`throttleMs` shape **creation**; `batchConfigs` shape **emission**.

## Generator-level constructs (not under localConfigs)

### `vars` and `varsOnce`
Per-event variables vs once-per-generator constants. Reference with `var`:
```json
{
  "vars": { "userId": { "_gen": "uuid" } },
  "varsOnce": { "tenant": { "_gen": "oneOf", "choices": ["a", "b"] } },
  "value": {
    "userId": { "_gen": "var", "var": "userId" },
    "tenant": { "_gen": "var", "var": "tenant" }
  }
}
```
Repeated references to the same var in one event always return the same value. Vars can reference other vars.

### `fork`
Clones a generator into many concurrent instances, one per unique `key`.
```json
"fork": {
  "key": { "_gen": "uuid" },
  "maxForks": 20,
  "stagger": 200,
  "keepAlive": false,
  "oneTimeKeys": false
}
```
- `key` may be an array of literals or a function; it cannot be null. The current key is available as the var `forkKey`.
- `maxForks` caps concurrency. Without it (and without termination), memory grows unbounded.
- `stagger` = minimum ms between fork spawns (avoids bursty starts).
- `keepAlive: true` spawns replacements after `maxForks` is reached even if forks are long-lived.
- `oneTimeKeys: true` prevents respawning after a fork terminates (abandoned carts, one-shot sessions); this retains all keys, so don't use it to spawn unbounded unique keys.
- Default: when a fork terminates, a new one respawns in its place.

### `stateMachine` (generator-level)
Overrides parts of the generator per state; states merge into the base envelope. A state missing from `transitions` is terminal. See `references/docs/functions/stateMachine.md`.

### `schedule`
Top-level, not a generator field. Runs named generators in stages, advancing when every generator in the stage terminates (`maxEvents`, `maxMs`, or a terminal state). `overrides` merges per-stage changes. `loop: true` restarts after the last stage.
```json
{
  "schedule": {
    "stages": [
      { "generators": ["seedUsers"], "overrides": { "seedUsers": { "localConfigs": { "maxEvents": 100 } } } },
      { "generators": ["streamEvents"] }
    ]
  }
}
```
`references/docs/schedule/stages.md` and `references/docs/schedule/loop.md`.

### `kind`
`writer` (default) or `loader`. Loaders read an existing Kafka topic / S3 bucket / Databricks table into lookup history; combine with `maxEvents: 1` for a one-shot load. `reader` exists for MotherDuck only.
