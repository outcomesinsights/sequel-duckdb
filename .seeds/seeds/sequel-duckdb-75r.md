---
id: sequel-duckdb-75r
title: Sequel types for DuckDB composite columns and the Ruby value for TIME
status: captured
type: question
created_at: 2026-10-19T17:42:00.813900+00:00
updated_at: 2026-10-19T17:46:18.504865+00:00
---

Not ruled. Two questions about the Sequel type that `db.schema` reports, and the Ruby value a model gets, for DuckDB columns that sequel-duckdb-4a8 (commit 4b1283d) did not cover.

## Question 1: composite and other non-scalar types

`map_duckdb_type_to_sequel` reports `:string` for LIST and ARRAY (`INTEGER[]`, `INTEGER[3]`), STRUCT, MAP, UNION, ENUM, JSON and BIT.

Measured 2026-10-19 (DuckDB 1.5.5, ruby-duckdb 1.5.5.1; logs `claude_stuff/time-and-composite-probe-20261019-103956.log`, `claude_stuff/composite-model-probe-*.log`):

```
result l (INTEGER[]):       Array [1, 2]
result st (STRUCT):         Hash {a: 1}
result m (MAP):             Hash {"k" => 1}
result u (UNION):           Integer 1      (the member value)
result e (ENUM):            String "x"
result j (JSON):            String "{\"a\":1}"
result b (BIT):             String "101"
auto_validations on a model: l and st "is not a valid string"
raise_on_typecast_failure = true: assigning [1] to l raises Sequel::InvalidValue
```

With the default model settings, assignment works only because Sequel keeps the value when the `:string` typecast fails. A model with the `auto_validations` plugin cannot save a LIST or STRUCT value.

`db.schema_type_class` returns nil for `:array`, `:struct`, `:map`, `:json`, `:enum`, `:interval`, `:bigint` and `:uuid`. So a type symbol that Sequel does not know has no typecast method and no schema-type validation: the value passes through unchanged.

Options:

- (a) Leave them as `:string`. No change. The `auto_validations` failure stays.
- (b) Use the type symbols of Sequel's PostgreSQL extensions: `:integer_array` style for lists, `:json`, `:enum`, `:hstore`-like for maps. A user who loads those extensions on a DuckDB database could get typecasts that expect PostgreSQL objects.
- (c) Use one symbol for each DuckDB kind: `:array` (LIST and ARRAY), `:struct`, `:map`, `:union`, `:enum`, `:json`, `:bit`. Values pass through unchanged and validation skips them.

Agent recommendation: (c). It fixes the validation failure, it adds no typecast that could convert a value wrongly, and the symbols say what the DuckDB type is.

## Question 2: the Ruby value for TIME

Measured 2026-10-19 (`claude_stuff/time-dirty-probe-*.log`):

```
loaded from the database:           2026-10-19 13:45:30 -0700   (ruby-duckdb: Time on today's date)
model assigned "13:45:30":          1970-01-01 13:45:30 -0800   (the adapter's typecast_value_time)
changed_columns after that assign:  [:t]
Sequel core typecast_value_time:    Sequel::SQLTime "13:45:30"
```

Assigning the time a row already has marks the column as changed, because the two Time objects have different dates (and different UTC offsets). Sequel's own convention for a TIME column is `Sequel::SQLTime`, which the postgres and sqlite adapters return.

Options:

- (a) Leave it. The changed-column defect stays.
- (b) Remove the adapter's `typecast_value_time`, so model assignment gives `Sequel::SQLTime` (Sequel's default), and convert TIME result values to `Sequel::SQLTime` too. Assigned and loaded values then compare equal. Result values change class, which is a breaking change for code that compares them with a Time on a specific date.
- (c) Only remove the adapter's `typecast_value_time`. Assignment gives `Sequel::SQLTime`. Result values do not change. This does NOT fix the changed-column defect: measured 2026-10-19 (`claude_stuff/sqltime-dirty-probe-*.log`), the column is still marked as changed, because Sequel's model also compares the classes of the old and new values (Time and Sequel::SQLTime).

Agent recommendation: (b). It is the only option that fixes the changed-column defect, it gives one representation everywhere, and it matches other Sequel adapters. It needs a "Changed" entry in the CHANGELOG.

Related bead: sequel-duckdb-agp removes the `typecast_value` override, which never acts. That bead does not change TIME behavior.

RULED 2026-10-19 (Question 2, TIME): option (b). Ryan: "I think for item 3, B does make sense." TIME values become Sequel::SQLTime for both assigned and loaded values.

UPDATE 2026-10-19 (Question 1, composite types): not ruled. Ryan asked to revisit it, so it moved to its own seed, sequel-duckdb-1h0, with new findings: Sequel core has no JSON or array type, and writing a Ruby Array or Hash through sequel-duckdb produces wrong SQL.
