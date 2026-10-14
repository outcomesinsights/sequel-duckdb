---
id: sequel-duckdb-pbj.9
title: 'hew feedback on unreleased main: list builder, IS DISTINCT FROM README notes, COPY text change'
status: captured
type: question
parent: sequel-duckdb-pbj
created_at: 2026-10-14T17:29:33.374986+00:00
updated_at: 2026-10-14T17:29:33.374986+00:00
tags:
  - hew
  - dsl
  - release
---

## Source

hew sent this feedback on 2026-10-14, before a release of the unreleased main (pbj.1 to pbj.5 and pbj.8). hew reports no blocking defects. hew moved almost all of its raw SQL onto the new features through a Bundler path override on an unmerged hew branch. It uses copy_to_sql with format: and partition_by:, duckdb_read_json with a struct columns: value, Sequel.try_cast, duckdb_lambda (list_reduce with two parameters and list_transform), native FILTER and native IS DISTINCT FROM. hew reports its suite passes: 2049 examples, 0 failures. A real-data rebuild was still running when hew sent this.

None of the three points below is ruled.

## 1. No public list-literal builder for expressions

duckdb_ops has an internal ListLiteral for named-parameter values. It has no public `Sequel.duckdb_list(expr, ...)` that a caller can use inside an expression, for example `[e]` in the let idiom `list_transform([e], lambda v: ...)[1]`. The workaround is `Sequel.function(:list_value, x)`, which works. Sequel writes a Ruby Array as `(1, 2, 3)`, which DuckDB does not read as a list (found while building sequel-duckdb-2mi). hew calls a public builder optional.

Question: add `Sequel.duckdb_list`, or document `list_value` in the README as the way to write a list?

## 2. IS DISTINCT FROM: README notes next to supports_is_distinct_from?

Both facts come from Sequel core's design, not from this adapter. Verified here 2026-10-14 (claude_stuff/hew-feedback-probe-20261014-102915.log):

```text
Sequel.extension only: NoMethodError: undefined method 'is_distinct_from_sql_append' for an instance of Sequel::DuckDB::Dataset
db.extension:          SELECT * FROM "t" WHERE ("a" IS DISTINCT FROM "b")
negated:               SELECT * FROM "t" WHERE NOT ("a" IS DISTINCT FROM "b")
```

hew also reports that EXPLAIN on DuckDB turns `NOT (a IS DISTINCT FROM b)` into a hash join on IS NOT DISTINCT FROM for LEFT and FULL joins. This session did not check that.

Question: add a README note that the extension must be loaded on the Database, and that negation with `~` is the IS NOT DISTINCT FROM form?

## 3. COPY text changed for string-matching consumers

copy_to_sql now writes `FORMAT PARQUET` in upper case and quotes partition columns: `(FORMAT PARQUET, PARTITION_BY ("year"))`. DuckDB accepts both. The change altered every expected COPY string in hew's specs. Other consumers whose specs match COPY text will see the same change.

Question: should the release notes for the next version state this change in COPY text explicitly, beyond the fix(copy) commit subject that git-cliff will list?
