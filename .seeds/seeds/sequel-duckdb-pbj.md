---
id: sequel-duckdb-pbj
title: DuckDB DSL features requested by hew to remove raw SQL
status: captured
type: exploration
created_at: 2026-10-13T17:35:58.869373+00:00
updated_at: 2026-10-13T17:42:47.359240+00:00
tags:
  - hew
  - dsl
---

## Request

hew, a Ruby data pipeline that writes DuckDB SQL through Sequel, asked for these proposals on 2026-10-13. hew depends on sequel-duckdb ~> 0.3 (0.3.1 locked, Sequel 5.109).

Ryan ruled on 2026-10-12 that hew may use SQL outside the Sequel DSL. hew must record every such SQL fragment in a ledger, together with a sequel-duckdb feature that could remove it. The seven child seeds come from that ledger. None of them is ruled. Do not build anything until Ryan rules.

## The seven proposals, in hew's order of value

1. A public `Dataset#copy_to_sql` that returns the COPY text and fixes the Copier defects.
2. A `read_parquet` helper with named parameters (`hive_partitioning`, `hive_types`, `union_by_name`).
3. Declare `supports_filtered_aggregates?` in the adapter.
4. Declare `supports_is_distinct_from?` in the adapter.
5. `Sequel.try_cast(expr, type)`.
6. `* EXCLUDE (...)` and `* REPLACE (...)` dataset methods. hew recommends deferring this item.
7. A DuckDB type vocabulary that checks type names. This has the least value.

## Proposed placement principle

Capability flags and COPY go in the adapter. Sequel's shared Postgres adapter has `copy_table`, `copy_into`, `insert_conflict`, `merge` and `lock`. `sequel_impala` has `set`, `load_data`, `to_parquet` and `left_semi_join`.

New expression builders go in an opt-in extension, so a user who does not load it gets the same SQL as before. About 25 of Sequel's 104 core extensions are specific to one database, for example `pg_json_ops`, `pg_array_ops` and `sqlite_json_ops`.

## Hazard for any release

Items 3 and 4 change the SQL that ConceptQL gets from sequel-duckdb, because ConceptQL uses `.filter` aggregates and distinct-from comparisons. ConceptQL's suite must pass before a release that contains either item.

## Correction and open idea: Database#set already exists in sequelizer (2026-10-13)

hew's request said that Sequel core has no `Database#set`. That is true for core. But sequelizer already has a `settable` extension with `Database#set(opts)` and a private `set_sql`. sequelizer's `db_opts` extension loads `settable` automatically.

`settable` emits `SET key=value` and does not quote the value. On a real in-memory DuckDB (gem 0.3.1):

- `SET threads=4` runs.
- `SET memory_limit=1GB` fails: `Parser Error: syntax error at or near "GB"`.
- `SET memory_limit='1GB'` runs.
- `SET timezone=America/Los_Angeles` fails: `Binder Error: SET value cannot contain column names`.

So `settable` is broken for DuckDB string values today.

sequel-duckdb has its own `Database#set_pragma(key, value)` and `configure_duckdb(opts)` in the adapter. They quote and escape strings correctly, and they emit `PRAGMA key = value`.

Ryan's idea, not ruled: `Database#set` and `Database#set_pragma` could both be sequelizer extensions. Questions to decide:

- Should `settable` quote values through the dataset's literalizer? That fixes DuckDB and changes the SQL for other databases.
- Should `set_pragma` move from this adapter to a sequelizer extension? This adapter would then need to depend on sequelizer, or keep a copy.
- Should `set_sql` become public, so a caller can get the text without running it? This is the same need as the `copy_to_sql` child seed.

## Placement recommendation (sequel-duckdb session, 2026-10-13, not ruled)

The rule follows Sequel's own conventions.

- **sequel-duckdb core adapter, always loaded:** capability flags, bug fixes, and DuckDB-only statements that Sequel would put in an adapter. Postgres keeps `copy_table` and `copy_into` in its adapter. Items .1, .3 and .4 go here. `set_pragma` stays here. Item .6 goes here too if it is ever built, because Sequel puts select syntax that only one database has in that database's adapter.
- **An opt-in sequel-duckdb extension:** new expression builders, so that a user who does not load the extension gets the same SQL as before. This mirrors Sequel's `pg_*_ops` extensions. Items .2 and .5 go here.
- **sequelizer extension:** only behaviour that is generic across databases. The `settable` fix (`Database#set`) goes here. Nothing in .1 to .7 belongs in sequelizer, because COPY, PRAGMA, FILTER, TRY_CAST and the table functions all have syntax specific to DuckDB.
- **Drop:** item .7. Sequel passes a Symbol cast type through unchecked on every adapter, and DuckDB reports a bad type name when it binds the query.

A pattern appears in .1, in hew's S3 entry and in `settable`: callers need the SQL text without running it. hew needs it for plan files and for DuckDB CLI `-cmd` arguments. Sequel's own convention is a public `*_sql` method beside each method that runs SQL, for example `insert_sql` beside `insert`. Each new method that runs SQL should get one: `copy_to_sql`, a public `set_sql`, and possibly `pragma_sql`.

## hew's N\* entries (checked against hew's ledger, 2026-10-13)

- N1 maps to .5, N2 and N9 map to .3, and N3 maps to .4.
- N4 to N8 need no feature. hew writes them in the DSL today.
- The ledger also lists carve fragments that it does not itemize. One of them has no seed: DuckDB lambda syntax (`list_transform(l, x -> ...)`, at hew's `carve.rb:289`). It would be a candidate for the opt-in extension if hew itemizes it.
