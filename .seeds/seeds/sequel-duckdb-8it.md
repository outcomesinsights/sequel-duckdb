---
id: sequel-duckdb-8it
title: 'One DuckDB instance per file: share it across Sequel::Database objects, and close it on disconnect'
status: captured
type: question
created_at: 2026-10-15T13:05:17.133281+00:00
updated_at: 2026-10-15T13:08:02.639559+00:00
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
