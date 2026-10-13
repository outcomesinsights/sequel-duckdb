---
id: sequel-duckdb-pbj.3
title: Declare supports_filtered_aggregates? so DuckDB gets native FILTER
status: captured
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:36:00.053261+00:00
updated_at: 2026-10-13T17:36:00.053261+00:00
tags:
  - hew
  - dsl
---

## Proposal

Declare `supports_filtered_aggregates?` as true in the adapter's dataset methods. DuckDB supports `f(x) FILTER (WHERE c)` natively. hew ranks it third.

## Verified facts (0.3.1, Sequel 5.109)

- The adapter does not declare the flag. It is a core Sequel flag (`dataset/features.rb:264`), so Sequel emulates FILTER.
- The emulation rewrites `f(x) FILTER (WHERE c)` as `f(CASE WHEN c THEN x END)`. On a mock database, `first(x ORDER BY ts) FILTER (WHERE x IS NOT NULL)` became `first((CASE WHEN ("x" IS NOT NULL) THEN "x" ELSE NULL END) ORDER BY "ts")`.
- That emulated query gives a wrong result with no error. `first` returns the NULL of the latest row that fails the condition, not the first value that passes it.

## Hazard

This changes the SQL that ConceptQL gets for every `.filter` aggregate. ConceptQL's suite must pass before a release.
