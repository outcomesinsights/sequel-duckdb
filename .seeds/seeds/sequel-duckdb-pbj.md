---
id: sequel-duckdb-pbj
title: DuckDB DSL features requested by hew to remove raw SQL
status: captured
type: exploration
created_at: 2026-10-13T17:35:58.869373+00:00
updated_at: 2026-10-13T17:35:58.869373+00:00
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
