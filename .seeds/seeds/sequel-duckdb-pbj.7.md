---
id: sequel-duckdb-pbj.7
title: Checked DuckDB type vocabulary (Sequel.duckdb_type)
status: abandoned
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:36:01.531800+00:00
updated_at: 2026-10-13T17:51:38.226237+00:00
resolved_at: 2026-10-13T17:51:38.226217+00:00
tags:
  - hew
  - dsl
---

## Proposal

Add a DuckDB type vocabulary, for example `Sequel.duckdb_type("BIGINT")`, that checks the type name. hew ranks it seventh, with the least value.

## Verified fact (0.3.1, Sequel 5.109)

`Sequel.cast` emits a Symbol type without a check. On a mock database, `Sequel.cast(:x, :hugeint_bogus)` became `CAST("x" AS hugeint_bogus)`. The error appears only when DuckDB runs the query.

## Recommendation (not ruled): abandon

Sequel never checks a cast type name on any adapter. `Sequel.cast(x, :SYMBOL)` passes the name through by convention, and DuckDB reports a bad type name when it binds the query. A checked vocabulary would need a copy of DuckDB's type list, and that copy must change with each DuckDB release. hew's D3 can stay as it is.
