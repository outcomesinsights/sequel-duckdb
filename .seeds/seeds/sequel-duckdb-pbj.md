---
id: sequel-duckdb-pbj
title: DuckDB DSL features requested by hew to remove raw SQL
status: captured
type: exploration
created_at: 2026-10-13T17:35:58.869373+00:00
updated_at: 2026-10-15T03:45:53.404801+00:00
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

## RULING 2026-10-13 (Ryan)

Ryan accepted the placement recommendation and asked for beads and an implementation:

- Build .1, .3 and .4 in the core adapter.
- Build .2 and .5 in an opt-in sequel-duckdb extension.
- Defer .6.
- Abandon .7.
- The `settable` fix belongs to sequelizer. It is filed there as sequelizer-c85.

## Design rulings 2026-10-13 (Ryan, from the seeds-to-beads questions)

- **Extension:** one opt-in extension named `duckdb_ops`, holding `try_cast` and the table-function helpers.
- **Named arguments:** a general named-argument builder that works with any function, plus the helpers `read_parquet`, `read_csv` and `read_json` built on it.
- **COPY format:** use `format:` when given. Otherwise infer it from the file extension. If neither gives a format, raise `Sequel::Error` naming the path.
- **ConceptQL:** a verification bead runs ConceptQL's suite against this checkout before the work counts as done.
- **Copier test:** the one assertion `(FORMAT CSV, DELIMITER |)` in `test_copy_with_delimiter_option` may change to the valid `(FORMAT CSV, DELIMITER '|')`. DuckDB rejects the old SQL. Every other existing test stays unchanged.

## UPDATE 2026-10-13: ConceptQL verification (bead sequel-duckdb-acd.7)

ConceptQL's DuckDB suite passes against the final state of this bead set (main at 56142ea). No test regressed. The candidate loaded the new FILTER and IS DISTINCT FROM flags, but ConceptQL never reaches them: its lib and sequelizer contain no aggregate `.filter`, no `is_distinct_from`, and no `copy_to`.

The suite ran through ConceptQL's `just test-duckdb` recipe, in a throwaway clone of ConceptQL at 75cae435. Both runs used one fixture file and minitest seed 15329.

```
baseline  (sequel-duckdb 0.3.0, lock revision ce86567): 504 runs, 1854 assertions, 0 failures, 0 errors, 0 skips
candidate (sequel-duckdb 0.3.1, path to this checkout): 504 runs, 1854 assertions, 0 failures, 0 errors, 0 skips
```

The logs are claude_stuff/acd.7/conceptql-baseline-20261013-114032.log and claude_stuff/acd.7/conceptql-candidate-20261013-114411.log.

The Postgres and Spark jobs of ConceptQL's CI were not run. They need Docker services, and they do not load sequel-duckdb.

A side finding belongs to ConceptQL. Its pinned fixture checksum no longer matches the published Dropbox fixture, so its own `just test-duckdb` stops at the checksum check today. For this comparison, the current publish was re-pinned in the throwaway clone only.

## UPDATE 2026-10-14: settable has changed since the 2026-10-13 note

Read from sequelizer's current `settable` and `db_opts` source:

- `set_sql` is now public. It returns the SET statements without running them.
- A value wrapped in `Sequel.expr` goes through `literal`, so `DB.set(memory_limit: Sequel.expr('1GB'))` gives `SET memory_limit='1GB'`. A plain String is still written raw, so `DB.set(memory_limit: '1GB')` still fails on DuckDB.
- `db_opts` quotes any value that contains a non-word character before it calls `set_sql`.

Verified on DuckDB 1.5.5 (claude_stuff/set-vs-pragma-probe-\*.log): `SET name = value` and `PRAGMA name = value` change the same setting. `memory_limit`, `threads` and `TimeZone` were checked. So for DuckDB settings, `set_pragma` gives no capability that `DB.set` lacks. Nothing in this adapter calls `set_pragma` or `configure_duckdb` on connect.

Agent recommendation, not ruled: keep `set_pragma` in the adapter and do not move it to sequelizer. Moving it means that this adapter depends on sequelizer, or keeps a copy. `PRAGMA name = value` exists only in DuckDB and SQLite. The useful fix is in sequelizer: `settable` should quote plain String values through `literal`, as `db_opts` already does.

## RULED 2026-10-14 (Ryan)

Ryan: "let's not move either then I guess". `set_pragma` and `configure_duckdb` stay in this adapter. `settable` stays in sequelizer. Neither moves. Not ruled: whether sequelizer's `settable` should quote plain String values. That question belongs to sequelizer.
