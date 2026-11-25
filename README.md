# ShadowTraffic skill

An agent skill for writing, running, and debugging [ShadowTraffic](https://shadowtraffic.io) data-generation configs — Kafka, Postgres, S3, webhooks, Redis, files, and more.

Works with any agent that reads `SKILL.md` (OpenCode, Claude Code, Cursor, and other agentskills.io-compatible tools). The skill ID is `shadowtraffic`.

## What's here

| Path | Contents |
|---|---|
| `SKILL.md` | Mental model, setup workflow, connection/function pickers, realism patterns, pitfalls, debugging loop |
| `references/functions.md` | All 57 functions with required/optional parameters |
| `references/connections.md` | All 15 connectors: config, generator envelope, auth, formats |
| `references/generator-config.md` | Every `localConfigs`/`globalConfigs` knob, plus `vars`, `fork`, `stateMachine`, `schedule` |
| `references/modifiers.md` | All 14 function modifiers and their execution order |
| `references/operations.md` | Docker, license, CLI, lint, bootstrap, profiler, Studio, metrics, debug |
| `references/recipes.md` | Index of 42 validated official example configs |
| `references/recipes/` | The 42 example configs as JSON |
| `references/agent-recipes.md` | Guide to the four LLM-agent backend configs |
| `assets/agent-recipes/` | Agent sessions/tool calls, observability, Postgres eval fixture, webhook smoke test |
| `references/DOCS.md` | How to fetch the full official docs |

The public repo intentionally does **not** bundle ShadowTraffic's documentation. Fetch it locally when you want page-level detail:

```bash
python3 scripts/fetch_docs.py   # writes references/docs/, git-ignored
```

The curated references above work without this step.

## Install

Clone, then link the skill directory into your agent's skills folder.

OpenCode:

```bash
git clone https://github.com/dosedaf/shadowtraffic-skill.git
ln -s "$PWD/shadowtraffic-skill" ~/.config/opencode/skills/shadowtraffic
```

Claude Code uses `~/.claude/skills/shadowtraffic`; other agents use their equivalent skills directory. The folder name should be `shadowtraffic` so it matches the skill ID.

## Quick smoke test

You need a ShadowTraffic license (`license.env`) from https://shadowtraffic.io/pricing.html.

```bash
# print events without touching a backend
docker run --rm --env-file license.env \
  -v "$PWD/assets/agent-recipes/agent-sessions-kafka.json:/home/config.json" \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json --stdout --sample 10 --quiet
```

Swap in any config from `references/recipes/` or your own.

## Recipes

`references/recipes/` contains working patterns for relationships and lookups, state machines and lifecycles, distributions and timing, and full retail datasets across Kafka/Postgres/S3. `assets/agent-recipes/` adapts those patterns to LLM-agent backends: sessions, messages, tool calls, traces/spans, metrics, logs, and a relational eval fixture.

All JSON files are parsed and checked against the documented function schemas. They still expect you to replace `localhost` endpoints and credentials.

## Documentation and trademarks

ShadowTraffic is a commercial product of ShadowTraffic, Inc. This repo is an independent skill and is not affiliated with or endorsed by ShadowTraffic. The docs fetched by `scripts/fetch_docs.py` remain the property of ShadowTraffic and are not redistributed here.

No license has been chosen for this repo yet. Add one before accepting outside contributions.
