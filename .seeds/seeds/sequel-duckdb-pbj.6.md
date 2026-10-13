---
id: sequel-duckdb-pbj.6
title: Dataset methods for SELECT * EXCLUDE and * REPLACE (hew recommends deferral)
status: deferred
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:36:01.163409+00:00
updated_at: 2026-10-13T17:51:37.410384+00:00
tags:
  - hew
  - dsl
---

## Proposal

Add dataset methods for DuckDB's `* EXCLUDE (...)` and `* REPLACE (...)` select forms. hew ranks it sixth and recommends that we defer it.

## Why hew recommends deferral

- The change alters how the adapter generates the select clause. That is a larger change than the other items.
- hew can name its columns explicitly instead, so the need is small.

## Verified fact (0.3.1)

The gem has no support for EXCLUDE or REPLACE.

## Placement (not ruled): core adapter dataset method, deferred

If this is ever built, it goes in the core adapter as a dataset method. Sequel puts select syntax that only one database has in that database's adapter. It emits nothing new unless a caller calls it. I agree with hew's recommendation to defer it: hew can select the columns by name, because `Plan#types_before` already knows them.
