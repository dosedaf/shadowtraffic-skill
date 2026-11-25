# Agent backend recipes

Four ready-to-adapt configs for testing agents and agent infrastructure. All are valid against v2.5.1 and pass `--action lint`'s expected shape (run lint yourself after editing).

| Config | Target | Use it for |
|---|---|---|
| `assets/agent-recipes/agent-sessions-kafka.json` | Kafka | Conversation + tool-call traffic with users, sessions, messages and correlated IDs |
| `assets/agent-recipes/agent-observability-kafka.json` | Kafka | OTLP-style traces, service metrics and logs with trace/span relationships |
| `assets/agent-recipes/agent-eval-postgres.json` | Postgres | Relational eval fixture: users, docs, conversations, messages, tool calls, eval scores |
| `assets/agent-recipes/agent-webhook-smoke.json` | Webhook | Minimal event firehose for an HTTP collector / local dev server |

### How to run

```bash
# 1. validate shape / eyeball events (no backend writes)
docker run --rm --net=host --env-file license.env \
  -v "$PWD/assets/agent-recipes/agent-sessions-kafka.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json --stdout --sample 12 --quiet

# 2. lint
docker run --rm --env-file license.env \
  -v "$PWD/assets/agent-recipes/agent-sessions-kafka.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --action lint --config /home/config.json

# 3. run for real (Kafka on localhost; drop --net=host on Docker Desktop)
docker run --rm --net=host --env-file license.env \
  -v "$PWD/assets/agent-recipes/agent-sessions-kafka.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json --metrics-port 9400
```

Verify with your own tooling:

```bash
kcat -C -t agent.sessions -c 3
kcat -C -t agent.messages -c 5
kcat -C -t agent.tool_calls -c 5
curl -s localhost:9400 | grep generator_events_sent_total
```

Postgres eval fixture:

```bash
psql "postgresql://postgres:postgres@localhost:5432/agent_eval" \
  -c 'select count(*) from st_users;' \
  -c 'select count(*) from st_messages;' \
  -c 'select * from st_evaluations limit 5;'
```

Webhook smoke (receiver at port 9090):

```bash
python3 -m http.server 9090   # or any collector; the demo just accepts requests
docker run --rm --net=host --env-file license.env \
  -v "$PWD/assets/agent-recipes/agent-webhook-smoke.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest --config /home/config.json
```

## What each recipe demonstrates

### `agent-sessions-kafka.json`

Four generators, run round-robin in listed order so `lookup` sources exist first:

1. `users` — coherent profiles (`profile`), plan mix, throughput 2/s.
2. `sessions` — `fork` creates up to 20 concurrent sessions; each fork has a stable `sessionId` (`forkKey`); a `stateMachine` moves `ACTIVE → IDLE → CLOSED` (CLOSED is terminal, so a new fork respawns).
3. `messages` — looks up a session and reuses its `sessionId`/`userId`; weighted user/assistant roles, prompt/completion token counts, latency distribution.
4. `toolCalls` — looks up a message and reuses its `sessionId`/`messageId`; weighted tool names/statuses.

Tune it:
- More concurrency: raise `fork.maxForks` (watch memory).
- Slower: raise the `throttleMs` values, or replace them with `throughput`.
- Different models/tools: edit the `weightedOneOf` choices.
- Trial license: keep total below 600 events/min (this config is ~1–2/s ⇒ ~60–120/min).

### `agent-observability-kafka.json`

- `traces`: one fork per trace; a state machine emits `root → llm → tool → finalize` spans. `parentSpanId` comes from `previousEvent`, so the trace is a real parent/child chain. Each span has `spanId`, service, model, status, token counts.
- `metrics`: a 5-second service metrics window (CPU, memory, RPM, p95, error rate).
- `logs`: looks up a trace and emits INFO/WARN/ERROR lines with `traceId`, using `oneOf` + Datafaker for variety.

Tune: add more span types, make errors rarer, or add a `logs` entry for every `ERROR` span (use `lookupScope: committed` if ordering matters).

### `agent-eval-postgres.json`

A `schedule` seeds tables in dependency order, then runs the final table indefinitely:

```
users → documents → conversations → messages → tool_calls → evaluations
```

- `tablePolicy: dropAndCreate` wipes/recreates the six `st_*` tables on every run. **Destructive** — never point this at a shared database.
- `pgHint` sets real column types (`UUID`, `TEXT`, `INTEGER`, `DOUBLE PRECISION`, `BOOLEAN`, `JSONB`).
- `lookup` paths use `["row", "id"]` (SQL envelope).
- Remove `localConfigs.maxEvents` from a stage (or the overrides) if you want it to run forever; keep the last stage infinite.

Tune: column names/types, `maxEvents` seed sizes, or add tables (append generators **after** their lookup sources and add them to `schedule.stages`).

### `agent-webhook-smoke.json`

One generator, 5 events/sec, JSON body with event type, session/user IDs (`cardinality` keeps a small pool), latency, success flag, a sparse `payload` (`someKeys`), and ISO timestamps. `X-Agent-Run` comes from `AGENT_RUN_ID` (defaults to `local-smoke`).

Point `url` at your collector; use `host.docker.internal` instead of `localhost` on Docker Desktop, or a compose service name.

## Rules when adapting

1. Keep lookup sources **before** their consumers in the `generators` array (round-robin order), or sequence them with `schedule`.
2. Multiple connections ⇒ add `"connection": "<name>"` to every generator.
3. Kafka lookups use `["value", "field"]` / `["key", "field"]`; SQL lookups use `["row", "field"]`.
4. `fork` needs `maxForks` or termination; otherwise memory grows without bound.
5. Startup lookups may see an empty history. Seed the source first (schedule or a finite `maxEvents` stage) if your pipeline requires referential integrity from event 1.
6. Trial licenses: 600 events/min total. Reduce `throughput`/raise `throttleMs`, or upgrade.
7. Run `--action lint` after every edit; unknown keys are silently ignored.
