# Function modifiers

Modifiers are extra keys placed **next to `_gen`** on any function object. They post-process the generated value. They work on almost every function (including nested ones), with a few exceptions for preprocessors (`env`, `loadJsonFile`, `loadPropertiesFile`), which run before compilation and ignore modifiers.

```json
{ "_gen": "normalDistribution", "mean": 60, "sd": 5, "decimals": 2, "null": { "rate": 0.05 } }
```

## Execution order

Modifiers run in this fixed order:

1. `cardinality`
2. `elide`
3. `null`
4. `keyNames`
5. `selectKeys`
6. `clamp`
7. `decimals`
8. `cast`
9. `path`
10. `sample`
11. `serialize`

If you need a different order (e.g. `selectKeys` after `path`), wrap the value in `constant` and apply modifiers to the wrapper:

```json
{
  "_gen": "constant",
  "x": { "_gen": "constant", "x": { "a": { "b": "c" } }, "path": ["a"] },
  "selectKeys": ["b"]
}
```

## Reference

### `path`
Array of string keys / integer indices. Drills into a map or array; returns `null` when absent. The workhorse for `lookup` and `previousEvent`:
`"path": ["value", "customerId"]`, `"path": ["row", "id"]`.

### `selectKeys`
Array of keys. Keeps only those keys from an object result.

### `keyNames`
Map of `originalName -> newName`. Renames keys in an object result.

### `null`
`{ "rate": 0..1 }` (or a function). Returns `null` that fraction of the time. Common in raw event data.

### `elide`
Removes the **whole field** from the parent object, not just its value. Exactly one strategy:
- `{ "rate": 0..1 }` — drop this percentage.
- `{ "whenPresent": <function> }` — drop when the reference is non-null.
- `{ "whenAbsent": <function> }` — drop when the reference is null.

### `cardinality`
Positive integer. Caps the number of distinct values; once the cap is reached, earlier values are reused at random. `cardinality: 1` freezes a value forever. Ideal for "50 users, many events".

### `clamp`
`[low, high]`. Clamps numeric output into the range.

### `decimals`
Non-negative integer. Half-up rounding for >0; `0` truncates the decimal suffix and yields an integer.

### `cast`
One of `string`, `integer`, `double`, `boolean`. Casts are strict to/from string (no lossy double↔integer); use `decimals` to shape numbers first.

### `sample`
`{ "rate": 0..1 }` (or function). Keeps a random subset of elements of an **array** result. Note the nesting: `"sample": { "rate": 0.5 }`.

### `serialize`
`{ "type": ... }`, one of `avroFixed`, `avroDecimal`, `postgresTimestamp`, `sqlServerTimestamp`, `databricksTimestamp`. Converts the value to its final wire type (e.g. a real Postgres `timestamp` instead of text). Once serialized, don't apply further functions/modifiers — the value is bytes.

### `pgHint`
String SQL type for auto-created Postgres columns, e.g. `"pgHint": "TEXT NOT NULL"`, `"INTEGER"`, `"TIMESTAMPTZ"`, `"JSONB"`, `"NUMERIC(10,2)"`.

### `sqlHint`
Same idea for MySQL, SQL Server, Oracle (and other SQL connections): `"sqlHint": "VARCHAR(255)"`.

### `avroHint`
Map describing the Avro type for this field when ShadowTraffic infers/creates an Avro schema.

### Schema hints (generator configs, not per-function)
- `avroSchemaHint`, `jsonSchemaHint`, `protobufSchemaHint` — explicit wire schema for a generator field (e.g. `value`).
- `schemaRegistrySubject` — subject to download from Schema Registry.
- `sqlDdlHint` — full `CREATE TABLE` DDL, supersedes all `sqlHint`s.

See `references/docs/function-modifiers/` for every example (run `python3 scripts/fetch_docs.py` first).
