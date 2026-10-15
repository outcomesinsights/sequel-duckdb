---
id: sequel-duckdb-pbj.3
title: Declare supports_filtered_aggregates? so DuckDB gets native FILTER
status: resolved
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:36:00.053261+00:00
updated_at: 2026-10-15T03:41:05.443027+00:00
resolved_at: 2026-10-15T03:41:05.443017+00:00
resolution: 'Shipped in c7aad9c (bead sequel-duckdb-acd.1): supports_filtered_aggregates? returns true at lib/sequel/adapters/shared/duckdb.rb:924; test/filtered_aggregates_test.rb. ConceptQL verified with no regressions (bead sequel-duckdb-acd.7). Efficacy: tweaking none.'
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

## Wrong result demonstrated on a real DuckDB (2026-10-13)

The table has rows (g, x, ts): (1, 10, 1), (1, NULL, 2), (2, NULL, 1) and (2, 20, 2). The query is `first(x ORDER BY ts DESC) FILTER (WHERE x IS NOT NULL)` grouped by g.

- Sequel's emulation (`first(CASE WHEN x IS NOT NULL THEN x ELSE NULL END ORDER BY ts DESC)`) returns g=1 → nil. This is wrong.
- Native FILTER returns g=1 → 10. This is correct.
- hew's workaround, `any_value(x ORDER BY ts DESC)`, also returns 10.

## Placement (not ruled): core adapter

This is a correctness fix and a true capability flag, so it goes in the core adapter. ConceptQL's suite must pass before a release that contains it.
