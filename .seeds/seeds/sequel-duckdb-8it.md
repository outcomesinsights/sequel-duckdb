---
id: sequel-duckdb-8it
title: 'One DuckDB instance per file: share it across Sequel::Database objects, and close it on disconnect'
status: captured
type: question
created_at: 2026-10-15T13:05:17.133281+00:00
updated_at: 2026-10-15T13:16:02.885317+00:00
tags:
  - duckdb
  - t_shank
---

## The defect

Reported 2026-10-15 by a t_shank session. Each `Sequel::Database` on a DuckDB file opens its own `::DuckDB::Database` in `adapter_initialize` (`lib/sequel/adapters/duckdb.rb:62`). Two Sequel databases on one file in one process are two independent DuckDB instances. DuckDB's file lock does not stop this, because fcntl locks are per process. Writes made through one instance do not reliably reach the other.

`Sequel::Database#disconnect` closes only the pool's connections. `@duckdb_database` stays open until the process exits, and the adapter has no public way to close it.

Reproduced in this repo at bd87fe6, libduckdb 1.5.5 (claude_stuff/two-instances-probe-\*.log):

```
a = Sequel.connect(url); a creates s1.t; a.disconnect
b = Sequel.connect(url); b creates s2.t
c = Sequel.connect(url)
b sees s1.t: true
c sees s2.t: true
a sees s2.t: false
distinct DuckDB::Database objects: 3
```

At scale, t_shank saw a third instance, opened after an export wrote 192 tables, fail with "schema export\_<id> does not exist" while the first instance was still open. t_shank is not blocked: its bench builds in a forked child process, and its production exports open one instance each.

## Design options (not ruled)

1. **Close on disconnect.** Override `disconnect` so that it also closes `@duckdb_database` once the pool is empty, and reopen it lazily in `connect`. A caller can then release the file. It does not stop two live Sequel databases on one file from diverging.
2. **One shared instance per file per process.** Keep a process-wide registry keyed by the file's real path, with a reference count. Each `Sequel::Database` on that path uses the shared instance. The instance closes when the last holder disconnects. Python's duckdb package keeps a similar instance cache. `:memory:` is never shared, because each in-memory database is separate by design.
3. **Both.** The registry from option 2, and `disconnect` releases this database's reference.

Hazard from the reporter: never close an instance under live connections of the same `Sequel::Database`. With option 2 or 3, also never close it under another `Sequel::Database` that still holds a reference.

Open question for option 2 or 3: `adapter_initialize` passes no DuckDB config today. If one is added later (for example read-only), two Sequel databases on one path with different config cannot share an instance. They would need an error or separate registry keys.

Agent recommendation: option 3. It fixes the divergence that t_shank hit, and it gives callers a way to release the file.

## RULED 2026-10-15 (Ryan)

Ryan chose option 3: "3 please". Keep a process-wide registry with one shared `::DuckDB::Database` per file, counted by reference. `disconnect` releases this `Sequel::Database`'s reference. The instance closes when the last reference is released. `:memory:` is never shared. The config question stays open, because the adapter passes no DuckDB config today. It must be answered before any config is added to `adapter_initialize`.

## UPDATE 2026-10-15: how Python's duckdb handles different settings

Measured with Python duckdb 1.5.6 (claude_stuff/python-duckdb-config-probe-\*.log). A second `duckdb.connect` on the same file with the same settings shares the open instance and sees its tables. A second connect with different settings raises `ConnectionException: Can't open a connection to same database file with a different configuration than existing connections`. This happened for `threads`, `memory_limit` and `read_only=True`. After every connection is closed, the file opens with new settings. The rule is first opener wins until the instance closes, and a mismatch is an error. It is not a silent second instance.

This answers the open config question for this adapter, if config is ever added: raise `Sequel::DatabaseConnectionError` on a mismatch, as Python does. Not ruled.

## UPDATE 2026-10-15: how other DuckDB client libraries share instances

Every official client that shares instances follows one rule. Connections to the same file share one instance. The settings are fixed by whoever opens the file first. A later open with different settings is an error. The instance closes when its last connection closes. In-memory databases are never shared.

- Python duckdb 1.5.6: measured (claude_stuff/python-duckdb-config-probe-\*.log). Same settings share the instance. Different `threads`, `memory_limit` or `read_only` raise `ConnectionException: Can't open a connection to same database file with a different configuration than existing connections`. After all connections close, new settings are accepted.
- Java JDBC: documented (duckdb.org/docs/current/clients/java/connecting). The cache is keyed by absolute path and is on by default (`jdbc_instance_cache`). A different configuration fails with the same message. The instance shuts down when its last connection closes, unless `jdbc_pin_db` is set.
- R duckdb: documented (r.duckdb.org/reference/duckdb.html). `duckdb()` with the same `dbdir` returns the same instance while it is alive. A later call with `read_only` or `config` fails rather than dropping them. The instance is released when its last connection closes.
- Node (@duckdb/node-api): documented. Caching is opt-in through `DuckDBInstance.fromCache(path)` or a `DuckDBInstanceCache`. Plain `DuckDBInstance.create` does not cache.
- C API: `duckdb_create_instance_cache` and `duckdb_get_or_create_from_cache` exist. The docs say the cache "is necessary if a client/program (re)opens multiple databases to the same file within the same process".

## ruby-duckdb's DuckDB::InstanceCache is not safe for this design (measured, ruby-duckdb 1.5.5.1, libduckdb 1.5.5)

ruby-duckdb wraps the C cache as `DuckDB::InstanceCache#get_or_create(path, config)`. Logs: claude_stuff/ruby-instance-cache-\*.log.

- Two calls for one path return the SAME Ruby object (`a.equal?(b)` is true). Closing it for one holder breaks the other: the next query raises `DuckDB::Error: connection error`.
- A different config raises the same `Can't open a connection ... different configuration` error as Python.
- After the returned Database is closed, a second `get_or_create` for that path on the SAME cache object hangs. It did not return in 20 seconds and ignored SIGTERM, so `timeout` could not stop it. A NEW cache object, or a plain `DuckDB::Database.open`, opens the file normally.

So the adapter should keep its own registry: a Mutex-guarded Hash from expanded path to the `DuckDB::Database` and a reference count. It opens with plain `DuckDB::Database.open`, and closes and removes the entry when the count reaches zero. That avoids the hang, because a reopen after a close never touches a stale cache. Agent recommendation, not ruled. The hang may be worth a report to ruby-duckdb, and that is Ryan's decision.
