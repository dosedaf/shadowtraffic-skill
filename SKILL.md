---
name: shadowtraffic
description: Writes, runs, and debugs ShadowTraffic data-generation configs. Use when the user wants realistic synthetic traffic or seed data for Kafka, Postgres, MySQL, SQL Server, Oracle, Databricks, MotherDuck, S3, GCS, Azure Blob, local files, Redis, Google Pub/Sub, KurrentDB, or webhooks — including production-traffic simulation, load/soak tests, agent/LLM backend telemetry, e-commerce/IoT/CDC streams, multi-table relational data, and profiling existing data sources.
---

# ShadowTraffic

ShadowTraffic is a containerized, declarative data generator. You give it **one JSON (or YAML) config**; it compiles the `_gen` functions in that config and streams synthetic events to your backend. No application code, no SDK.

- Image: `shadowtraffic/shadowtraffic:latest` (ECR mirror: `public.ecr.aws/s2b8p3j6/shadowtraffic/shadowtraffic:<version>`)
- Full official docs: run `python3 scripts/fetch_docs.py` once to populate `references/docs/` (not bundled; see `references/DOCS.md`)
- Everything is licensed: you need `license.env` with `LICENSE_*` variables from https://shadowtraffic.io/pricing.html

## Mental model (read this first)

Three concepts, nothing else:

1. **Functions** — any JSON object with `"_gen": "<functionName>"`. Replace concrete values with functions; the shape of your data mirrors the shape of your config.
2. **Generators** — backend-specific envelopes (a Kafka `topic`/`key`/`value`, a Postgres `table`/`row`, a webhook `url`/`data`, ...). A config has an array of them; by default they run round-robin.
3. **Connections** — a named map of where data goes (`kind` + credentials). One connection is inferred automatically; with 2+ every generator needs `"connection": "<name>"`.

```json
{
  "generators": [{
    "topic": "sensorReadings",
    "key": { "sensorId": { "_gen": "uuid" } },
    "value": {
      "reading": { "_gen": "normalDistribution", "mean": 60, "sd": 5, "decimals": 2 },
      "timestamp": { "_gen": "now" }
    }
  }],
  "connections": {
    "dev-kafka": {
      "kind": "kafka",
      "producerConfigs": {
        "bootstrap.servers": "localhost:9092",
        "key.serializer": "io.shadowtraffic.kafka.serdes.JsonSerializer",
        "value.serializer": "io.shadowtraffic.kafka.serdes.JsonSerializer"
      }
    }
  }
}
```

Top-level keys: `generators` (required), `connections`, `globalConfigs`, `schedule` (plus `profiler` only for the profiler action).

## Setup workflow for an agent/backend

### 1. Pin down the target and the shape

Ask or infer, then write it down before generating JSON:

- Target system + connection details (Kafka brokers/topic, Postgres host/db/table, webhook URL, bucket, ...).
- Event envelope: topic/table, key, payload fields.
- Volume: events/sec or total events; duration.
- Realism: which fields must correlate across events/tables (user → session → message → tool call).

If the user has existing data, prefer **Profiler** (`--action profile`) or **bootstrap** from JSON Schema/Avro over hand-writing a huge config. See `references/operations.md`.

### 2. Write a minimal config and dry-run it

```bash
# 10 events to stdout, no backend needed
docker run --rm --net=host --env-file license.env \
  -v "$PWD/st.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json --stdout --sample 10 --quiet
```

Iterate with `--watch --stdout --sample 10` and optionally `--with-studio -p 8080:8080`.

### 3. Lint before debugging anything else

ShadowTraffic ignores unrecognized keys silently. Typos look like "my option did nothing".

```bash
docker run --rm --env-file license.env \
  -v "$PWD/st.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --action lint --config /home/config.json
```

### 4. Connect for real and verify

Remove `--stdout --sample`; keep `--net=host` on Linux for `localhost` brokers; expose `--metrics-port 9400`.

```bash
docker run --rm --net=host --env-file license.env \
  -v "$PWD/st.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json --metrics-port 9400
```

Verify on the target, not from ShadowTraffic's logs: `kcat -C -t <topic>`, `psql -c 'select count(*) ...'`, `redis-cli`, or the webhook receiver. Then check `curl localhost:9400 | grep generator_events_sent_total`.

### 5. Tune realism and volume

- **Rate**: unthrottled by default. Use `localConfigs.throughput` (events/sec) or `localConfigs.throttleMs` (min ms between events), and `globalConfigs` for all generators.
- **Time**: `globalConfigs.time` (start epoch ms) and `timeMultiplier` (speed up/slow down wall clock).
- **Messiness**: `delay`, `discard`, `repeat`; `null`, `elide`, `sample` modifiers.
- **Identity**: `vars` (per event), `varsOnce` (fixed per generator), `sequentialInteger/String` (monotonic IDs), `fork` (many instances of one generator), `stateMachine` (lifecycles), `lookup` (cross-stream/table joins).

### 6. Prove reproducibility only if asked

`--seed 42` reproduces values, throttles, delays, and `varsOnce` choices. Logs print the seed of every run.

## Choose the connection

| Target | `kind` | Generator envelope |
|---|---|---|
| Kafka | `kafka` | `topic`, `key?`, `value?`, `headers?`, `producerConfigs?` |
| Postgres | `postgres` | `table`, `row`, `op?` (`insert`/`update`/`delete`), `where?` |
| MySQL | `mysql` | `table`, `row`, `op?`, `where?` |
| SQL Server | `sqlServer` | `table`, `row`, `op?`, `where?` |
| Oracle | `oracle` | `table`, `row`, `op?`, `where?` |
| Databricks | `databricks` | `table`, `row`, `tags?`, `evolveSchema?` |
| MotherDuck | `motherduck` | SQL generator schema (see docs page) |
| S3 | `s3` | `bucket`, `bucketConfigs`, `data` |
| Google Cloud Storage | `googleCloudStorage` | `bucket`, `bucketConfigs`, `data` |
| Azure Blob | `azureBlobStorage` | `container`, `containerConfigs`, `data` |
| Local files | `fileSystem` | `directory`, `fileConfigs`, `data` |
| Redis | `redis` | `op` (`set`/`hset`/`zadd`), `key`, `attributes`, `opParams?` |
| Pub/Sub | `pubsub` | `projectId`, `topicId`, `topicConfigs`, `data` |
| KurrentDB | `kurrentdb` | `stream`, `eventType`, `data`, `metadata?` |
| Webhook | `webhook` | `url`, `data`, `method?`, `headers?`, `queryParams?`, `urlSuffix?` |

Full config fields, auth, file formats and gotchas: `references/connections.md` + `references/docs/connections/<name>.md` (after fetching docs).

## Choose the function

| Need | Use |
|---|---|
| Random ID | `uuid` (v4 default, `version: 7` possible), `ulid`, `gitSha` |
| Monotonic ID / counter | `sequentialInteger`, `sequentialString` (`expr: "user-~d"`) |
| Names, emails, addresses, products, lorem | `string` with Datafaker: `expr: "#{Name.fullName}"`, `#{Address.fullAddress}`, `#{Commerce.productName}`, `#{Lorem.paragraph}` |
| Demographically coherent person/company/email | `profile` (`traits`: `firstName`, `lastName`, `email`, `company`, `website`) |
| Number in range | `uniformDistribution` (`bounds: [min,max]`) |
| Bell curve / natural metric | `normalDistribution` (`mean`, `sd`) |
| Enumerated / weighted choice | `oneOf`, `weightedOneOf` (`choices: [{weight,value}]`) |
| Math | infix `math` (`expr: "(2+4)*3"`, names) or `add`/`subtract`/`multiply`/`divide`/`min`/`max`/`pow` |
| Timestamp | `now` (epoch ms), `formatDateTime` (`ms`, Joda `format`), `duration` |
| Time-of-day behavior | `intervals` (cron → value) |
| Reuse a value in several fields | `vars` + `var`, or `varsOnce` for a fixed value |
| Join streams/tables | `lookup` (`topic`/`table`/`name`, `path`, optional `strategy`, `histogram`) |
| Per-entity lifecycle | `fork` + `stateMachine` + `previousEvent` |
| Random map/array | `map`, `mapEach`, `repeatedly`, `someKeys`, `merge` |
| Fake but realistic geo | `geolocation` (needs dataset), `geoWander`, `waypoints`, `heading` |
| LLM text | `ai` (Ollama/Bedrock/Vertex; external setup) |
| Escape hatch | `customFunction` (Python/Ruby/JS via GraalVM) |

All 57 functions with exact parameters: `references/functions.md`; every modifier: `references/modifiers.md`.

## Multi-stream realism (the pattern that matters)

Use one **writer** per stream, share IDs with **lookup**, and control lifecycles with **fork** + **stateMachine**:

```json
{
  "generators": [
    {
      "name": "users",
      "topic": "app.users",
      "value": {
        "userId": { "_gen": "uuid" },
        "profile": { "_gen": "profile", "traits": ["firstName", "lastName", "email", "company"] },
        "plan": { "_gen": "weightedOneOf", "choices": [
          { "weight": 8, "value": "free" },
          { "weight": 2, "value": "pro" }
        ]}
      },
      "localConfigs": { "throughput": 2 }
    },
    {
      "name": "sessions",
      "fork": { "key": { "_gen": "uuid" }, "maxForks": 50, "stagger": 200 },
      "topic": "app.sessions",
      "key": { "sessionId": { "_gen": "var", "var": "forkKey" } },
      "vars": { "userId": { "_gen": "lookup", "name": "users", "path": ["value", "userId"] } },
      "value": { "sessionId": { "_gen": "var", "var": "forkKey" }, "userId": { "_gen": "var", "var": "userId" } },
      "stateMachine": {
        "_gen": "stateMachine",
        "initial": "ACTIVE",
        "transitions": { "ACTIVE": "IDLE", "IDLE": "CLOSED" },
        "states": {
          "ACTIVE": { "value": { "state": "ACTIVE" } },
          "IDLE": { "value": { "state": "IDLE" } },
          "CLOSED": { "value": { "state": "CLOSED" } }
        }
      },
      "localConfigs": { "throttleMs": 1000 }
    }
  ],
  "connections": { "kafka": { "kind": "kafka", "producerConfigs": {
    "bootstrap.servers": "localhost:9092",
    "key.serializer": "io.shadowtraffic.kafka.serdes.JsonSerializer",
    "value.serializer": "io.shadowtraffic.kafka.serdes.JsonSerializer"
  }}}
}
```

Rules that bite:

- `lookup` can only see events from generators that already ran and were **written**; give the source generator a `name` (cleanest) or address it by output target (`topic`/`table`).
- For Kafka lookups, the envelope is `{topic,key,value,headers}`; path into it explicitly, e.g. `["value","userId"]`.
- For SQL lookups, path through `["row","userId"]`. `path` may start with `"vars"` to read the source event's variables.
- Need skew (80% of tickets from platinum users)? Split the source into named generators per predicate and choose between those lookups with `weightedOneOf`.
- Lookup history defaults to 1,000,000 events (`maxHistoryEvents`) and is only retained when referenced.
- `fork` without `maxForks`, a terminal state, or `maxEvents` can exhaust memory.
- In a top-level `stateMachine`, a state omitted from `transitions` is terminal — that is how a fork ends.
- `previousEvent` reads the last event of the **same generator/state machine**, not another stream.
- Multiple connections ⇒ every generator must set `"connection": "<name>"`.
- JSON is the default config format; `--config-format yaml` accepts YAML too.

More working patterns (42 official examples, all validated): `references/recipes/` and `references/recipes.md`.

## Rate, volume, and licensing

- Default is flat-out generation. Trial licenses allow **600 events/min**, 1 concurrent instance, 200 GB/month. Keep the sum of all generator `throughput` at or below ~10 events/sec (or every `throttleMs` >= 100) while on a trial.
- Developer: $399/yr, 3 instances, unlimited events/min, 1 TB/month.
- Enterprise: unlimited.
- `throughput` caps event *creation*; `batchConfigs` (`lingerMs`, `batchRows`/`batchElements`, `batchBytes`) controls *emission* — they are independent.
- `--sample N` generates a bounded sample and exits; `maxEvents`/`maxMs` stop a generator; `stateMachine` terminal states stop a fork.

## Debugging loop

1. `--action lint` — catches misplaced/typo'd keys (ShadowTraffic is open-schema and silently ignores unknowns).
2. `--stdout --sample 5 --no-pretty` — inspect exact event envelopes.
3. `--watch --sample 10 --stdout --quiet` — edit config, see new output immediately.
4. `--with-studio --watch --sample 10 -p 8080:8080` — timeline UI; inspect event contents, lookup origins, forks, localConfigs.
5. `SHADOWTRAFFIC_DEBUG=1` — dump JVM/resources/env/args, then stays alive for `docker exec`.
6. Metrics `:9400` — `generator_events_sent_total` rate and `generator_iteration_duration_seconds`.
7. Seed reproduction: copy the logged seed, rerun with `--seed <n>`.

## Non-obvious pitfalls

- **Wrong container paths**: `loadJsonFile`, schema files, custom functions, and license files are read *inside* the container. Mount them and use container paths (`/home/...`), not host paths.
- **`localhost` inside Docker isn't the host.** Use `--net=host` (Linux) or `host.docker.internal`/compose service names.
- **Postgres `row`, Kafka `value`** — swapping them fails validation.
- **SQL auto-DDL is inferred**, often `text`/`double precision`. Use `pgHint`/`sqlHint` per field, or `sqlDdlHint`, and `tablePolicy: "dropAndCreate"` to reset (destructive: cascades).
- **Kafka auto-creates topics** by default. Use `topicPolicy: { "policy": "manual" }` or `{ "policy": "dropAndCreate" }` deliberately.
- **`--stdout` still connects loader generators** to real systems; loaders have no dry-run.
- **No `dateToday` function** despite stale docs examples; use `formatDateTime` with `ms: { "_gen": "now" }`.
- **Datafaker syntax**: `#{Name.fullName}`; parameterized calls use single quotes, commas, no spaces: `#{Date.birthday '18','80'}`.
- **`geolocation`** needs a country dataset mounted and registered in `globalConfigs.geolocation.countryFiles`.
- **File outputs** (`s3`, `gcs`, `azure`, `fileSystem`) require bucket/container to exist (dirs are created for `fileSystem`); two generators writing the same file prefix collide.
- **Read-only container** needs `SHADOWTRAFFIC_STAGING_DIR` pointing at writable storage.
- **Webhook** body is always JSON (`application/json`); `maxRequests: 0` makes requests synchronous.
- **Redis** assumes strings unless `"serialization": "json"`.
- **`stateMachine` at generator level** merges its state into the generator envelope; as a nested function it just returns the state value.

## Reference map

| File | Contents |
|---|---|
| `references/functions.md` | All 57 functions: params, purpose, gotchas, doc link |
| `references/modifiers.md` | All 14 function modifiers and execution order |
| `references/generator-config.md` | Every `localConfigs`/`globalConfigs` knob |
| `references/connections.md` | All 15 connections: config, generator shape, auth, formats |
| `references/operations.md` | CLI, Docker, license, lint, bootstrap, profiler, studio, metrics, debug |
| `references/recipes.md` | Index of the 42 official example configs + what each teaches |
| `references/agent-recipes.md` | Ready configs for LLM-agent backends (sessions, tool calls, telemetry) |
| `references/docs/**` | Full official docs, one file per page (fetched on demand, git-ignored) |
| `references/recipes/*.json` | Validated example configs |

When in doubt, fetch the docs and search them (e.g. `grep -r "keywords" references/docs/functions`) instead of guessing a function name or field.
