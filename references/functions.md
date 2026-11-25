# ShadowTraffic functions

Every function is a JSON object with `"_gen": "<name>"`. Required and optional keys below come from the official v2.5.1 schemas. Almost every function also accepts the [function modifiers](modifiers.md) (`path`, `null`, `elide`, `cast`, `decimals`, `clamp`, `cardinality`, `keyNames`, `selectKeys`, `sample`, `serialize`, `*Hint`).

Full examples for each: `references/docs/functions/<name>.md` (run `python3 scripts/fetch_docs.py` to populate).

## Identity, strings, scalars

### `uuid`
Required: none. Optional: `version` (default 4; `7` = time-sortable).
Random UUID string.

### `ulid`
Required: none.
Random, lexicographically sortable ULID string. Better than `uuid` when you want time-ordered IDs across many concurrent instances.

### `sequentialInteger`
Required: none. Optional: `startingFrom` (default 0), `advanceBy` (default 1), `loopAfter`.
Stateful. Emits 0, 1, 2, ... per generator run.

### `sequentialString`
Required: `expr`. Optional: `startingFrom`.
Stateful. `~d` is replaced with the sequence number: `"expr": "sensor-~d"` → `sensor-0`, `sensor-1`...

### `characterString`
Required: `n`, `case` (`lower`/`upper`/`mixed`). Optional: `unique`.
Random alphabetic string of length `n`.

### `digitString`
Required: `n`.
Random numeric string of length `n` (leading zeros possible; use `cast: "integer"` if you need a number).

### `string`
Required: `expr`. Optional: `names`, `case` (`lower`/`upper`/`capitalized`), `length`, `removeSubstring`, `locale: [lang, region]`.
Templated string using `#{...}` references to local names, vars, or Java Datafaker 2.4.1 expressions.
Examples: `#{Name.fullName}`, `#{Address.fullAddress}`, `#{Commerce.productName}`, `#{Lorem.paragraph}`, `#{Date.birthday '18','80'}`, `"http://x/#{userId}"`.
Datafaker params: single quotes, commas, no spaces. Local `names` win over vars, vars win over Datafaker.

### `boolean`
Required: none.
`true`/`false` with equal probability.

### `bytes`
Required: `size`.
Random byte array of `size` bytes (performance/load testing).

### `constant`
Required: `x`.
Always returns `x`; useful when you want a fixed value but still want `null`/`elide` modifiers.

### `var`
Required: `var`. Optional: `path`.
Reads a generator-level `vars`/`varsOnce` entry, optionally drilling into it. Repeated references in one event return the same value.

### `gitSha`
Required: none.
Random 64-char Git SHA-256-looking hex string.

## Numbers and math

### `uniformDistribution`
Required: `bounds: [min, max]`. Optional: `decimals`.
Uniform random number in the range (bounds may be functions).

### `normalDistribution`
Required: `mean`, `sd`. Optional: `decimals`.
Normal (bell-curve) random number. Use `decimals: 0` for integers.

### `add` / `subtract` / `multiply` / `divide` / `min` / `max`
Required: `args: [...]`.
Fold the argument list with the operation. Args may be numbers or functions.

### `pow`
Required: `base`, `exp`.

### `math`
Required: `expr`. Optional: `names`.
Infix expression evaluated by exp4j-style parser; supports `+ - * / % ^`, parentheses, and `abs acos asin atan cbrt ceil cos cosh exp floor log log10 log2 max min pow signum sin sinh sqrt tan tanh`.
`names` maps identifiers to generator expressions, e.g. `"expr": "now - offset"`.
Compiled once; performance is comparable to nested function calls. Use this instead of deeply nested math functions.

### `trichotomy`
Required: `value`, `low`, `mid`, `high`.
Buckets `value`: below `low` → `low`; above `high` → `high`; otherwise `mid`.

### `histogram`
Required: `bins: [{ "bin": <number 0..1>, "frequency": <number> }]`.
Chooses a bin proportional to frequency; mainly used to bias `lookup` population selection.

### `cos` / `sin` / `tan`
Required: `degrees`.
Trigonometry in degrees. Can also be used through `math`.

## Selection and state

### `oneOf`
Required: `choices: [...]`.
Uniform random choice; choices may be literals or functions.

### `weightedOneOf`
Required: `choices: [{ "weight": <number>, "value": <any> }]`.
Weighted random choice. Weights are relative, not percentages.

### `someKeys`
Required: `object`.
Returns a random non-empty subset of the object's keys (models partial/sparse payloads).

### `lookup`
Required: one target (`topic`, `table`, `name`, or Pub/Sub `projectId`+`topicId`). Optional: `connection`, `path`, `strategy` (`first`/`last`/`random`, default random), `histogram`.
Retrieves a previously generated event so streams/tables can share identifiers.
- Address by generator `name` (best) or by its output target (`topic`/`table`).
- `path` drills into the stored envelope: Kafka `["value","userId"]` or `["key","id"]`; SQL `["row","userId"]`.
- `strategy` targets first/last event; `histogram` biases which slice of the population is chosen.
- `path` may start with `"vars"` to read the variables that existed when the target event was generated (e.g. `["vars","customerTier"]`).
- Predicated lookups: split one logical stream into several named generators (one per predicate), then pick among them with `weightedOneOf`.
- Only events already written (or generated, see `lookupScope`) are visible; history defaults to 1,000,000 events.

### `previousEvent`
Required: none. Optional: `path`, `defaultValue`.
Stateful. Returns the last event produced by the **same generator** (very common inside a top-level `stateMachine` to evolve a value).

### `stateMachine`
Required: `transitions`, `states`. Optional: `initial`, `merge: { "previous": true }`.
Stateful. Two modes:
- **Nested function** — each state is a value; transitions are `{state: nextState}`.
- **Top-level generator field** — each state is an object merged into the generator envelope (can override `value`, `key`, `topic`, etc.). A `null`/absent transition terminates the generator/fork.
`merge.previous: true` merges the previous event into the next state (ideal for "update" states).
See `references/docs/functions/stateMachine.md` for transition arrays with `times` and conditional transitions.

### `cache`
Required: `on`, `to`.
Caches `to` per unique `on` value for the life of the process: same input always yields the same output. Accrete-only — beware memory.

### `cycle`
Required: `sequence`.
Emits elements in order, then loops. Stateful. Handy for deterministic round-robin assignment.

## Object and array transformation

### `merge`
Required: `objects: [...]`.
Shallow-merges objects left to right. Commonly used with `previousEvent` to evolve an event.

### `map`
Required: `key`, `value`, `n`.
Builds an object of `n` generated key/value pairs; duplicate keys collapse to the latest.

### `mapEach`
Required: `over`. Optional: `function`, `juxt`.
Applies `function` to every element of array `over`. Inside the function, the current element is the reserved var `mapItem`. `juxt` is an array of paths extracted from each element before applying the function (useful for price×qty sums).

### `repeatedly`
Required: `n`, `target`. Optional: `iterateVars`, `localVars`.
Builds an array of `n` evaluations of `target` (n may be a function).

### `interpolate`
Required: `template`, `params`.
Fast token interpolation: `:name` tokens in `template` replaced from `params`. Pre-parsed/cached.

## Time

### `now`
Required: none.
Current wallclock time in **epoch milliseconds** (affected by `globalConfigs.time` and `timeMultiplier`).

### `formatDateTime`
Required: `ms`. Optional: `format` (Joda pattern, default `yyyy-MM-dd'T'HH:mm:ss.SSSZZ`), `timezone`.
Human-readable timestamp. Example: `format: "yyyy-MM-dd"`.

### `duration`
Required: none. Optional: `days`, `hours`, `minutes`, `seconds`, `ms`.
Builds a duration in milliseconds for time math, e.g. `{ "_gen": "math", "expr": "now - week", "names": { "week": { "_gen": "duration", "days": 7 } } }`.

### `intervals`
Required: `intervals: [[cron, value], ...]`. Optional: `defaultValue`, `defaultIndex`.
Returns the value for the first matching UNIX cron expression based on wallclock time. Common use: variable `throttleMs` for business hours/bursts. If nothing matches and no default is set, retries ~200 ms later.

### `toInstant`
Required: `ms`. **Deprecated** — use the `serialize` modifier for real timestamp types.

## Geo

### `geolocation`
Required: `country`. Optional: `state`, `city`, `zipCode`, `format` (`address` default, `coordinates`, `object`, or an array of formats).
Real addresses/coordinates from a mounted public-domain dataset. Requires `globalConfigs.geolocation.countryFiles` and volume-mounted CSV. Narrowing params are arrays; multiple params AND together, values within one OR. Cache lives in `/tmp/geolocations`.

### `geoWander`
Required: `boundingBox: [[lat,lon],[lat,lon]]`, `speed` (km/h). Optional: `scale`, `unit` (`radians` default / `degrees`).
Stateful. Random walk; outputs `latitude`, `longitude`, `heading`, `duration` (ms between points).

### `waypoints`
Required: `waypoints: [[lat,lon], ...]`, `speed`. Optional: `scale`, `unit`, `loop`.
Stateful. Travels a route; outputs `latitude`, `longitude`, `heading`, `duration`, `points`, `distance`, `time`. Throttle by the `duration` output for realistic speed.

### `heading`
Required: `src`, `dst`. Optional: `unit`.
Bearing in degrees between two coordinates.

## People, LLMs, preprocessors, escape hatches

### `profile`
Required: `traits` (array of `firstName`, `lastName`, `email`, `company`, `website`). Optional: `email`.
Demographically coherent person/company/domain (e.g. `Edgar Rivera` + `Payscale` + `erivera@payscale.com`). First call loads a few seconds of data.

### `ai`
Required: `service` (`ollama`/`bedrock`/`vertex`) plus that service's object. Optional: model options map.
Calls an LLM for free text. `ollama: {url, model, prompt}`, `bedrock: {modelId, prompt}`, `vertex: {projectId, location, modelId, prompt}`. Vars evaluated before the call are sent as context.

### `env`
Required: `var`. Optional: `as` (`string` default / `integer` / `double` / `boolean`), `default`, `mask`.
Preprocessor. Loads an environment variable before compilation; ideal for connection strings and secrets. Preprocessors ignore modifiers.

### `loadJsonFile`
Required: one of `file`, `files`, `data`. Optional: `data` (Mustache substitutions).
Preprocessor. Inlines JSON from files inside the container; `files` merges a group (later wins). Paths must be container paths, and files must be mounted.

### `loadPropertiesFile`
Required: one of `file`, `files`, `data`. Optional: `overrides`.
Preprocessor. Inlines Java `.properties` (Kafka client configs), with optional overrides merged in (e.g. add serializers).

### `customFunction`
Required: `language` (`python`/`ruby`/`js`), `file`, `function`. Optional: `args`, `exposeVars`.
Runs your code on GraalVM (GraalPy/TruffleRuby/GraalJS). The function receives `{args, state, vars}` and returns `{value, state?}`. Mount the file into the container. See `references/docs/functions/customFunction.md`.

## Easing

### `easing`
Two forms:
- Eased ramp: `direction` (`easeIn`/`easeOut`/`easeInOut`), `degree` (`linear`/`quad`/`cubic`/`quart`/`quint`), `from`, `to`, plus **either** `duration` (ms) or `events` (count).
- Custom easing: `ease` (a 0..1 number or function), plus **either** `duration` or `events`.

Stateful. Glides a numeric value from `from` to `to` at a controlled acceleration instead of jumping. After completion it pegs at the end value.

### `easingChain`
Required: `stages: [...]`. Optional: `loop`.
Runs easings in sequence; a numeric function in a stage holds a steady value. Mix time-based and count-based easings.
