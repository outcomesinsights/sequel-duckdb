---
id: sequel-duckdb-pbj.1
title: Public Dataset#copy_to_sql and Copier fixes (path escaping, explicit format, quoted partition columns)
status: resolved
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:35:59.281130+00:00
updated_at: 2026-10-15T03:41:06.261409+00:00
resolved_at: 2026-10-15T03:41:06.261399+00:00
resolution: "Shipped in 13efc23 and a972d8f (beads sequel-duckdb-acd.2, sequel-duckdb-acd.5): Dataset#copy_to_sql at lib/sequel/adapters/shared/duckdb.rb:878; Copier escaping, quoting and format inference in lib/sequel/duckdb/helpers/copier.rb; test/copy_to_sql_test.rb. The signature became (path, **options); see the UPDATE note. Efficacy: minor. Planning miss: the bead did not ask for a release note about the changed COPY text, which hew's feedback raised later (seed sequel-duckdb-pbj.9)."
tags:
  - hew
  - dsl
---

## Proposal

Add a public `Dataset#copy_to_sql(path, format:, partition_by:)` to the adapter. It returns the COPY statement as text. It escapes the path, takes the format explicitly, and quotes the partition columns as identifiers. `Database#copy_to` then becomes a thin wrapper that runs the text. hew says this removes five of its SQL fragments. hew ranks it first.

## Defects in `Sequel::DuckDB::Helpers::Copier` (0.3.1)

Verified on 2026-10-13 against a mock database. The log is in claude_stuff, which git does not track.

- `to_sql` builds `"COPY (#{source}) TO '#{@dst}' ..."` and does not escape the path. The path `/tmp/o'x/out` gives `TO '/tmp/o'x/out'`, which is invalid SQL.
- The format comes only from the file extension. A partitioned target is a directory with no extension. The statement then contains `(FORMAT , PARTITION_BY C)`, which is invalid SQL.
- `format_option` upcases every value. `partition_by: "c"` becomes `PARTITION_BY C`. The column name is not quoted, so a mixed-case or reserved column name breaks.
- The `destination` method expands the path, but `to_sql` does not call it. The method has no effect.
- `Database#copy_to` runs the statement at once. A caller cannot get the text, for example to write a plan file from a mock database.

## Notes

The path escaping and the identifier quoting are bug fixes even if Ryan rejects the new public method.

## Placement (not ruled): core adapter

The Copier fixes are bug fixes, so they go in the core adapter whether or not the new method is accepted. `Dataset#copy_to_sql` is DuckDB-only syntax with an adapter precedent: Postgres has `copy_table` and `copy_into` in its adapter. `Database#copy_to` keeps its signature and runs `copy_to_sql`.

## UPDATE 2026-10-14: what shipped (beads sequel-duckdb-acd.2, sequel-duckdb-acd.5)

The signature shipped as `Dataset#copy_to_sql(path, **options)`, not `(path, format:, partition_by:)`. Every COPY option passes through. The format comes from `format:`, else from the file extension, else Copier raises `Sequel::Error`. The `destination` method was removed. Two changes went beyond the proposal. Values that are not a single word, such as `delimiter: "|"`, are now quoted strings, because the old text was a DuckDB syntax error. Integer and Float values are written bare. The change in COPY text is noted in CHANGELOG.md under `[Unreleased]`.
