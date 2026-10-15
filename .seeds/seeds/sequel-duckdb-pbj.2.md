---
id: sequel-duckdb-pbj.2
title: read_parquet helper with named parameters (hive_partitioning, hive_types, union_by_name)
status: resolved
type: exploration
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:35:59.657282+00:00
updated_at: 2026-10-15T03:41:07.109446+00:00
resolved_at: 2026-10-15T03:41:07.109435+00:00
resolution: 'Shipped in 5409d10 (bead sequel-duckdb-acd.6): Sequel.duckdb_named and duckdb_read_parquet/csv/json in lib/sequel/extensions/duckdb_ops.rb:189-212; test/duckdb_ops_test.rb. Pathifier was not rebuilt on the new class; see the UPDATE note. Efficacy: tweaking none.'
tags:
  - hew
  - dsl
---

## Proposal

Add a `read_parquet` helper that takes DuckDB's named parameters: `hive_partitioning`, `hive_types` and `union_by_name`. Put it in an opt-in extension. hew ranks it second. It needs the most design work of the seven items.

## Verified facts (0.3.1, Sequel 5.109)

- `Pathifier` and `Database#read_something_sql` are private, and they accept no read options.
- Sequel's `SQL::Function` accepts only positional arguments. So this call cannot be written with the DSL: `read_parquet(..., hive_partitioning = true, hive_types = {...}, union_by_name = true)`.

## Design question

The helper needs a small expression class that emits `name = value` arguments. The same class could cover `read_json`'s `columns = {...}` and the other DuckDB table functions with named parameters. Decide whether the class is general or specific to parquet.

## Placement (not ruled): opt-in extension

This is a new expression builder, so it goes in an opt-in extension, as `pg_json_ops` does for Postgres. The named-argument expression class should be general. It would then cover `read_parquet`, `read_csv` and `read_json`'s `columns = {...}`, which hew uses at `carve.rb:284`. The private `Pathifier` could then build its SQL on top of the same class.

## UPDATE 2026-10-14: what shipped (bead sequel-duckdb-acd.6)

The named-argument class is general: `Sequel::DuckDB::NamedArguments`, built by `Sequel.duckdb_named(**args)`. It is the last argument of any `Sequel.function` call. Helpers `duckdb_read_parquet`, `duckdb_read_csv` and `duckdb_read_json` use it. The private `Pathifier` was NOT rebuilt on the new class. It still writes its own read\_\* SQL. That reuse was a suggestion, not a requirement, and it remains possible.
