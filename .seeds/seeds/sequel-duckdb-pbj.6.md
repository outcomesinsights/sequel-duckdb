---
id: sequel-duckdb-pbj.6
title: Dataset methods for SELECT * EXCLUDE and * REPLACE (hew recommends deferral)
status: captured
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-13T17:36:01.163409+00:00
updated_at: 2026-10-13T17:36:01.163409+00:00
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
