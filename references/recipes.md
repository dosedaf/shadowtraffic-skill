# Official example gallery

42 validated configs from ShadowTraffic's official AI context (`shadowtraffic-ai-context`). They are working patterns, not production configs: endpoints default to `localhost` and a few use illustrative ranges. Copy the technique, then adapt connection details.

All files: `references/recipes/<slug>.json`.

## Start here

| File | Teaches |
|---|---|
| `hello-world-with-kafka.json` | Minimum Kafka generator |
| `hello-world-with-postgres.json` | Minimum Postgres table/row generator |
| `hello-world-with-s3.json` | Minimum S3 bucket/object generator |
| `hello-world-with-a-webhook.json` | Minimum HTTP webhook generator |
| `untitled.json` | Same as hello-world Kafka |

## Relationships, lookups, multi-stream

| File | Teaches |
|---|---|
| `orders-have-a-pre-existing-customer.json` | Two streams joined by `lookup` |
| `support-ticket-messages-arrive-every-5000ms.json` | Customers → purchases → tickets, chained lookups + throttles |
| `harvest-customer-ids-from-postgres-for-kafka-events.json` | Postgres generator feeding Kafka lookups (cross-connection) |
| `publish-80-of-the-tweets-from-20-of-the-users.json` | `histogram` bias on lookup population |
| `suspicious-accounts-transacting-that-log-in-with-a-new-ip-ad.json` | Three streams, weighted statuses, lookups, sequential IDs |
| `the-nexmark-streaming-benchmark-of-auction-streams.json` | Benchmark-style people → auctions → bids |
| `an-inventory-of-films-are-tracked-in-100-stores-like-the-sak.json` | 3-table Sakila-like inventory with `repeatedly` arrays |
| `the-northwind-data-set.json` | 10-table relational dataset with `cache`, `lookup`, `env`, `duration`, `math` — the best multi-table reference |
| `inventory-is-updated-every-200ms-and-queries-check-its-statu.json` | Write stream + read/query stream sharing keys |

## State machines and lifecycles

| File | Teaches |
|---|---|
| `5-sensors-whose-value-is-the-previous-value-plus-a-random-nu.json` | `stateMachine` + `previousEvent` drift |
| `30-jvms-report-their-heap-readings-every-250-ms-which-oscill.json` | Same drift pattern for metric oscillation |
| `customers-go-through-a-4-stage-funnel.json` | Funnel states with weighted transitions |
| `shopping-carts-add-items-check-out-and-sometimes-get-abandon.json` | Cart lifecycle, abandoned terminal state, repeated items |
| `bots-post-social-content-that-get-likes-and-shares-only-5-of.json` | Users + posts, engagement states, joins |
| `a-new-user-comes-online-every-250ms-and-changes-their-ip-eve.json` | Stateful IP changes per user |
| `a-server-transitions-between-healthcheck-statuses-every-15-s.json` | Simple health-status state machine |
| `debezium-envelopes-have-3-discrete-states.json` | CDC envelope modeling with `someKeys` + `previousEvent` |
| `every-2-seconds-a-new-game-is-scheduled-to-start-with-bets-p.json` | Game/bet lifecycle with lookups |

## Rates, timing, distributions, noise

| File | Teaches |
|---|---|
| `support-ticket-messages-arrive-every-5000ms.json` | `throttleMs` per stream |
| `send-messages-every-500-ms-with-a-std-dev-of-40-ms.json` | Dynamic `throttleMs` with `normalDistribution` |
| `latency-is-about-10-milliseconds-with-bursts-to-50-and-150-e.json` | `intervals` cron-based latency bursts |
| `telemetry-data-gets-randomly-delayed-10-of-the-time-discarde.json` | `delay`, `discard`, `repeat` realism knobs |
| `place-exactly-15-orders.json` | `maxEvents` to stop exactly |
| `pick-a-date-timestamp-between-yesterday-and-tomorrow.json` | Time math with `now`, `duration`, `add`/`subtract`, `formatDateTime` |
| `transactions-are-uniformly-priced-between-2-and-200.json` | `uniformDistribution` + `decimals` |
| `57-of-votes-are-cast-for-franklin-roosevelt.json` | `weightedOneOf` |
| `customers-have-a-name-age-and-membership-level.json` | Datafaker `string` + `oneOf` |
| `a-stream-of-the-h2o-dataset-configured-for-n-10m-k-10.json` | Group-by benchmark shape with arrays/maps |
| `a-stream-of-rides-from-new-york-s-yellow-taxi-network.json` | Taxi-style trip events with distances/durations |
| `flights-take-off-every-5-seconds-and-report-their-geolocatio.json` | `waypoints` travel + geolocation |
| `a-client-emits-otel-telemetry-data.json` | OTEL-ish telemetry event |
| `50-machines-ddosing-ec2-instances-in-us-east-1-with-200-byte.json` | Fixed-size payloads, security traffic |
| `200-merchants-have-their-businesses-audited-once-every-25-da.json` | Long-horizon scheduled events |
| `3-support-agents-field-phone-calls-arriving-once-a-second.json` | Agents + calls with states |
| `70-of-all-posts-are-from-repeat-users.json` | Skewed actor distribution |
| `publish-80-of-the-tweets-from-20-of-the-users.json` | Histogram-based skew |

## Format round-trips

| File | Teaches |
|---|---|
| `the-kitchen-sink-kafka-retail-data.json` | Same retail model to Kafka |
| `the-kitchen-sink-postgres-retail-data.json` | Same retail model to Postgres (`table`/`row`) |
| `the-kitchen-sink-s3-retail-data.json` | Same retail model to S3 (`bucket`/`data`) |

Use the three kitchen-sink files together to see how the envelope changes while the payload stays identical.

## Notes

- `hello-world-*` are the fastest sanity checks; run with `--stdout --sample 5`.
- The Northwind config is intentionally large and uses `env` for credentials — replace with literals for a local dry run.
- A few examples use `/tmp` paths or `localhost` connections; adjust before running.
- Validate any edit with `--action lint`.
