---
id: sequel-duckdb-pbj.7
title: Checked DuckDB type vocabulary (Sequel.duckdb_type)
status: captured
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:36:01.531800+00:00
updated_at: 2026-10-13T17:36:01.531800+00:00
tags:
  - hew
  - dsl
---

## Proposal

Add a DuckDB type vocabulary, for example `Sequel.duckdb_type("BIGINT")`, that checks the type name. hew ranks it seventh, with the least value.

## Verified fact (0.3.1, Sequel 5.109)

`Sequel.cast` emits a Symbol type without a check. On a mock database, `Sequel.cast(:x, :hugeint_bogus)` became `CAST("x" AS hugeint_bogus)`. The error appears only when DuckDB runs the query.
