---
id: sequel-duckdb-pbj.5
title: Sequel.try_cast(expr, type) in an opt-in duckdb_ops extension
status: resolved
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:36:00.792628+00:00
updated_at: 2026-10-15T03:41:06.710313+00:00
resolved_at: 2026-10-15T03:41:06.710304+00:00
resolution: 'Shipped in a8c0815 (bead sequel-duckdb-acd.3): Sequel.try_cast in the opt-in duckdb_ops extension, lib/sequel/extensions/duckdb_ops.rb:184; test/duckdb_ops_test.rb. Efficacy: tweaking none.'
tags:
  - hew
  - dsl
---

## Proposal

Add `Sequel.try_cast(expr, type)`, which emits `TRY_CAST(<expr> AS <type>)`. Put it in an opt-in extension. A working name for the extension is `duckdb_ops`. hew ranks it fifth.

## Verified facts (0.3.1)

- The gem has no `try_cast`. `lib/` contains no TRY_CAST and no `duckdb_ops`.
- Today a caller writes TRY_CAST as a literal string, so the expression is outside the DSL.

## Placement (not ruled): opt-in extension

This is a new expression builder, so it goes in the same opt-in extension as .2. The working name for the extension is `duckdb_ops`.
