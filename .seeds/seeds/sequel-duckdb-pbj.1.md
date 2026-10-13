---
id: sequel-duckdb-pbj.1
title: Public Dataset#copy_to_sql and Copier fixes (path escaping, explicit format, quoted partition columns)
status: captured
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:35:59.281130+00:00
updated_at: 2026-10-13T17:35:59.281130+00:00
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
