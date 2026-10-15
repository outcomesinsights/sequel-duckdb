---
id: sequel-duckdb-pbj.4
title: Declare supports_is_distinct_from? so DuckDB gets native IS DISTINCT FROM
status: resolved
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:36:00.425476+00:00
updated_at: 2026-10-15T03:41:05.837042+00:00
resolved_at: 2026-10-15T03:41:05.837031+00:00
resolution: "Shipped in d8a1397 (bead sequel-duckdb-acd.4): supports_is_distinct_from? returns true at lib/sequel/adapters/shared/duckdb.rb:930; test/is_distinct_from_test.rb. README section 'IS DISTINCT FROM' added in b242cc5. Efficacy: minor. Planning miss: the bead did not say that the is_distinct_from extension must be loaded on the Database, so a README note came later from hew's feedback (seed sequel-duckdb-pbj.9)."
tags:
  - hew
  - dsl
---

## Proposal

Declare `supports_is_distinct_from?` as true in the adapter's dataset methods. hew ranks it fourth.

## Verified facts (0.3.1, Sequel 5.109)

- The adapter does not declare the flag. Sequel's `is_distinct_from` extension checks `defined?(super)` first, so an adapter can declare it.
- Without the flag, the extension emits a CASE expression. On a mock database, `Sequel.is_distinct_from(:a, :b)` became `(CASE WHEN (("a" = "b") OR (("a" IS NULL) AND ("b" IS NULL))) THEN 0 ELSE 1 END) = 1`. DuckDB cannot run that expression as a hash join.

## Hazards

- This changes the SQL that ConceptQL gets for distinct-from comparisons. ConceptQL's suite must pass before a release.
- The flag itself does not change results. But a caller that changes a join from `=` to IS NOT DISTINCT FROM changes the join semantics, because NULL keys then match.

## Placement (not ruled): core adapter

This is a capability flag, so it goes in the core adapter. It changes SQL only for callers that load Sequel's `is_distinct_from` extension. ConceptQL's suite must pass before a release that contains it.
