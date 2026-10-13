---
id: sequel-duckdb-pbj.2
title: read_parquet helper with named parameters (hive_partitioning, hive_types, union_by_name)
status: captured
type: exploration
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:35:59.657282+00:00
updated_at: 2026-10-13T17:42:48.212420+00:00
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
