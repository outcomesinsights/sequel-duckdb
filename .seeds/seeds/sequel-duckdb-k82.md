---
id: sequel-duckdb-k82
title: Report ruby-duckdb InstanceCache hang (get_or_create after close) upstream
status: captured
type: decision
created_at: 2026-10-15T13:18:23.495966+00:00
updated_at: 2026-10-15T13:18:23.495966+00:00
tags:
  - duckdb
  - upstream
---

## What to report

Upstream is suketa/ruby-duckdb on GitHub. Measured with ruby-duckdb 1.5.5.1 and libduckdb 1.5.5 on Linux (titan). Logs: claude_stuff/ruby-instance-cache-\*.log. Probe scripts: claude_stuff/ruby-instance-cache-hang-probe.rb and claude_stuff/ruby-instance-cache-close-probe.rb.

1. **Hang.** `cache.get_or_create(path)` hangs when it is called again on the same `DuckDB::InstanceCache` after the Database it returned was closed. It did not return in 20 seconds. It ignored SIGTERM, so only SIGKILL stopped it. A new `InstanceCache` object, or `DuckDB::Database.open`, opens the same file normally.
2. **Shared Ruby object.** Two `get_or_create` calls for one path return the same Ruby object. Closing it for one holder makes the other holder's next query raise `DuckDB::Error: connection error`. In the C API, each `duckdb_get_or_create_from_cache` call returns its own `duckdb_database` handle, and each handle is closed separately with `duckdb_close`. This one may be intended behaviour. The report should ask, not assert.

Minimal reproduction of the hang:

```ruby
require "duckdb"
cache = DuckDB::InstanceCache.new
db = cache.get_or_create("/tmp/x.duckdb")
db.connect.query("create table t(x int)")
db.close
cache.get_or_create("/tmp/x.duckdb")   # never returns
```

## Checked before filing

On 2026-10-15, `gh search issues --repo suketa/ruby-duckdb` for "InstanceCache" and "get_or_create" found three closed issues. None describes this hang: #1462 (write-barrier protection), #1459 (WB_PROTECTED TypedData) and #1447 (use-after-free of registered functions through InstanceCache). Search again just before filing.

## Why it matters here

sequel-duckdb does not depend on this. Ryan ruled on 2026-10-15 that the adapter keeps its own registry of open files (seed sequel-duckdb-8it, bead sequel-duckdb-zn2), and that avoids `InstanceCache`. A fix upstream would let the adapter use DuckDB's own cache later. That cache would also give the "different configuration" error for free.

## Not decided

Ryan asked for this seed on 2026-10-15 ("make a seed for reporting upstream"). He has not said to file the issue. Filing it publishes to a public tracker, so it needs his explicit go. Before filing, rerun the reproduction on the newest ruby-duckdb release.
