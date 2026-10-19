---
id: sequel-duckdb-1h0
title: Array, struct, map and JSON support for DuckDB columns
status: captured
type: question
created_at: 2026-10-19T17:46:05.708957+00:00
updated_at: 2026-10-19T17:46:05.708957+00:00
---

Not ruled. Split out of sequel-duckdb-75r (Question 1) on 2026-10-19, because Ryan wanted to revisit it.

Ryan, 2026-10-19: "I was kind of expecting [Sequel] to have at least some sort of JSON support and array support. There isn't any in the core of [Sequel]. Some of them I don't expect it to really necessarily know. A union, a struct, maybe bit. I don't actually really understand the benefit to providing a DuckDB kind that then just gets passed through."

## What Sequel provides (sequel 5.108.0, measured 2026-10-19)

- Core `schema_column_type` knows string, integer, date, datetime, time, boolean, float, decimal, blob and enum. It has no JSON and no array type.
- JSON and array support are PostgreSQL extensions: `pg_json` (schema type `:json`/`:jsonb`, Ruby wrapper objects, literals cast with `::json`) and `pg_array` (schema types like `:integer_array`, a `PGArray` wrapper, literals as `ARRAY[...]`). `pg_json_ops` and `pg_array_ops` add the PostgreSQL operators. These write PostgreSQL syntax and depend on PostgreSQL type OIDs.
- `sqlite_json_ops` adds SQLite JSON function calls only. It has no schema type.

## What sequel-duckdb does today (claude_stuff/composite-insert-probe-\*.log)

Reading works. ruby-duckdb returns a LIST as a Ruby Array, a STRUCT or MAP as a Hash, and JSON as a String.

Writing a Ruby Array or Hash produces wrong SQL, because Sequel treats them as conditions:

```
insert(l: [1, 2])   -> INSERT INTO "x" ("l") VALUES (1 = 2)
                       Conversion Error: Unimplemented type for cast (BOOLEAN -> INTEGER[])
insert(st: {a: 1})  -> INSERT INTO "x" ("st") VALUES (("a" = 1))
                       Binder Error: Referenced column "a" was not found
insert(j: '{"a":1}') -> works (a JSON column takes a String)
```

The `:string` schema type also makes Sequel's `auto_validations` plugin reject a LIST or STRUCT value ("is not a valid string").

## The benefit question

A pass-through schema symbol (`:array`, `:struct`) only fixes the `auto_validations` rejection and labels the column in `db.schema`. `db.schema` already gives the exact DuckDB type in `:db_type` (for example `INTEGER[]`). It does not make writes work.

## Directions (not ruled)

- (a) Do nothing now.
- (b) Write support for lists only: a wrapper (for example `Sequel.duckdb_list([1, 2])`) that literalizes as `[1, 2]`, and schema type `:array` so models accept Ruby Arrays. Comparable to `pg_array`, without operators.
- (c) Write support for lists, structs and maps, each with a wrapper that literalizes as DuckDB syntax (`[1, 2]`, `{'a': 1}`, `MAP {'k': 1}`).
- (d) JSON support comparable to `pg_json`: schema type `:json`, Ruby Hash/Array values serialized to JSON text on write and optionally parsed on read. DuckDB already reads JSON text from a String, so the gain is the automatic serialization and parsing.
- Any of (b)-(d) could ship as an opt-in extension, the way sequel-duckdb-pbj's .2 and .5 were ruled to be opt-in.
