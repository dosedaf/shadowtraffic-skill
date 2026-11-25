# Operations: CLI, Docker, license, profiling, studio, metrics

## License

Every run needs the `LICENSE_*` environment variables. Put them in `license.env` (never commit it):

```
LICENSE_ID=xxx
LICENSE_EMAIL=xxx
LICENSE_ORGANIZATION=xxx
LICENSE_EDITION=xxx
LICENSE_EXPIRATION=xxx
LICENSE_SIGNATURE=xxx
```

Get a key at https://shadowtraffic.io/pricing.html. Current tiers:

| | Free Trial | Developer | Enterprise |
|---|---|---|---|
| Price | $0, 30 days | $399/year | contact |
| Users | 1 | 1 | unlimited |
| Concurrent instances | 1 | 3 | unlimited |
| Events/min | 600 | unlimited | unlimited |
| Data/month | 200 GB | 1 TB | unlimited |
| Support | none | best effort | standard |

On a trial, keep `throughput <= 10` per generator (600/min aggregated). Exceeding the cap stops you.

## Docker

```bash
docker run --rm --net=host --env-file license.env \
  -v "$PWD/config.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json
```

- `--net=host` (Linux) makes `localhost` brokers/databases reachable. On Docker Desktop use `host.docker.internal`, or a compose service name.
- Mount every file the config references (`loadJsonFile`, schemas, custom functions, geolocation data, license).
- ECR mirror with higher pull limits:
  `docker pull public.ecr.aws/s2b8p3j6/shadowtraffic/shadowtraffic:$version`
- Read-only container: file connectors stage through `/tmp`; set `SHADOWTRAFFIC_STAGING_DIR` to a writable tmpfs/volume.

Compose (entrypoint includes the JVM flags used by the image):

```yaml
shadowtraffic:
  image: shadowtraffic/shadowtraffic:latest
  volumes:
    - .:/workspace
  entrypoint: ["java", "--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow",
               "-Dfile.encoding=UTF-8", "-Dsun.stdout.encoding=UTF-8", "-Dsun.stderr.encoding=UTF-8",
               "-jar", "/home/shadowtraffic.jar"]
  command: ["--config", "/workspace/config.json"]
  env_file:
    - license.env
```

## CLI flags

```
--action <action>                 run (default) | bootstrap | lint | profile | sign-lease
--bootstrap-from-avro-schema <f>  approximate a config from an Avro schema
--bootstrap-from-json-schema <f>  approximate a config from a JSON Schema
--bootstrap-to <to>               target connection type for bootstrap (kafka, postgres, ...)
-c, --config <file>               config path
    --config-base64 <text>        config inline as Base64
    --config-format <format>      json (default) | yaml
    --dry-run                     alias for --stdout
-h, --help
    --lease-expiration <date>     Enterprise lease signing
    --lease-name <team>
    --lease-signing-aws-kms-key-id <id>
    --metrics-port <n>            Prometheus port, default 9400; 0 = random (logged)
    --no-pretty                   raw output with --stdout
-q, --quiet                       no status text, only generated data
    --reload <policy>             completion (default) | immediate (with --watch)
    --report-benchmark            performance stats after generators finish
    --sample <n>                  generate n events then exit
    --seed <n>                    reproducible run
-s, --stdout                      print generated data instead of writing
-w, --watch                       rerun on config change
    --with-studio                 Studio UI on :8080 (requires --watch and --sample)
```

`--stdout` ignores output targets but **still connects loader generators** to real systems (loaders have no dry run). It also still validates the configuration.

## Development loop

```bash
# fastest: 5 events, no connection writes
docker run --rm --net=host --env-file license.env \
  -v "$PWD/st.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json --stdout --sample 5 --no-pretty --quiet

# watch mode: re-emits samples on every save
docker run --rm --net=host --env-file license.env \
  -v "$PWD/st.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json --stdout --sample 10 --watch --quiet

# Studio: timeline UI on http://localhost:8080
docker run --rm --env-file license.env -p 8080:8080 \
  -v "$PWD/st.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json --watch --sample 10 --with-studio
```

Studio shows one timeline per generator, event contents on click, lookup origins ("show origins"), one track per fork, and read-only `localConfigs`.

## Lint

```bash
docker run --rm --env-file license.env \
  -v "$PWD/st.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --action lint --config /home/config.json
```

ShadowTraffic's parser is **open**: unknown keys are silently ignored. Lint names every unrecognized key and its JSON path — always run it after edits, especially when an option "did nothing".

## Bootstrap from a schema

```bash
docker run --rm --env-file license.env \
  -v "$PWD/schema.json:/home/schema.json" \
  shadowtraffic/shadowtraffic:latest \
  --action bootstrap --bootstrap-from-json-schema /home/schema.json --bootstrap-to kafka
```

Works with JSON Schema and Avro (`.avsc`), emitting a starter config with plausible functions. Supports `enum`, `const`, `oneOf`, `anyOf`, multi-type arrays (recent versions). Review/adjust the output; it's a stub, not a perfect mapping.

## Profiler (profile existing data)

```bash
docker run --rm --net=host --env-file license.env \
  -v "$PWD/profiler.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --action profile --config /home/config.json
```

The profiler config only needs `connections` (plus optional `profiler.sampleCount`, default 1,000,000 rows/events per collection):
- Kafka: add `consumerConfigs` and `topics: [{ "topic": "orders" }]`
- databases/files: point at tables/collections supported by the connector

It samples real rows/events, statistically models schema, distributions, null rates, cardinality, and foreign-key-like relationships, and prints a complete ShadowTraffic config to stdout. Privacy-safe: it models properties, it does not copy records. Works with Avro, Protobuf, JSON Schema and plain JSON Kafka topics.

Use this when the user says "make data that looks like our prod topic/table" and can provide read access.

## Metrics

- Prometheus endpoint on `--metrics-port` (default 9400), inside the container.
- `generator_events_sent_total` (counter) — use `rate()` for events/sec.
- `generator_iteration_duration_seconds` (histogram) — generation time excluding writes.
- Labels: `(connection name, output target)`.
- Standard JVM metrics included.

```bash
curl -s localhost:9400 | grep generator_events_sent_total
```

## Debug mode

```bash
docker run ... -e SHADOWTRAFFIC_DEBUG=1 shadowtraffic/shadowtraffic:latest ...
```

Prints resources (CPUs, max memory), Java version, argv, environment variables, and Java properties, then suspends the process so you can `docker exec` in and investigate.

## Reproducing a run

The logs print:

```
✔ Running with seed 720838425. You can repeat this run by setting --seed 720838425.
```

Pass `--seed <n>` to reproduce data, throttles, delays, and `varsOnce` decisions. Without a seed, every run differs.

## Enterprise leases

`--action sign-lease --lease-name "<team>" --lease-expiration yyyy-MM-dd --lease-signing-aws-kms-key-id <kms-id>` issues a short-lived sublicense. Requires an asymmetric AWS KMS signing key registered with ShadowTraffic support, plus AWS credentials in the container. The lease env vars are `LICENSE_EDITION`, `LICENSE_LEASE_KEY`, `LICENSE_LEASE_SIGNATURE`, `LEASE_TEAM`, `LEASE_ORGANIZATION`, `LEASE_EXPIRATION`, `LEASE_SIGNATURE`.
