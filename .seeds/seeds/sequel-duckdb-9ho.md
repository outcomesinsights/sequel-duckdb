---
id: sequel-duckdb-9ho
title: 'fork() ENOMEM on the Ubuntu 26.04 runner with DuckDB 2.0.0: measurements and open questions'
status: captured
type: exploration
created_at: 2026-11-12T23:13:13.150583+00:00
updated_at: 2026-11-12T23:13:13.150583+00:00
tags:
  - ci
  - duckdb-2
  - memory
  - test-suite
---

## What happened

GitHub CI on main went red on 2026-11-12 (run 43270416382, commit 4ed79cd). One job failed: "Test Ruby 4.0, DuckDB 2.0.0". Dependabot PR #50 failed the same way on 2026-11-01 (run 41262953442).

```
SharedInstanceTest#test_forked_child_does_not_reuse_the_parents_instance:
Errno::ENOMEM: Cannot allocate memory - fork(2)
    test/shared_instance_test.rb:211:in 'Kernel#fork'
```

The defect is tracked as bead sequel-duckdb-k1a. The fix is commit 4617c36 ("test: close the databases that each test opens"). This seed holds the measurements and the questions that the fix does not answer.

## Why only that job, and only from November

- The last green push run (40527955839, 17986ab, 2026-10-27) ran on runner image ubuntu-24.04 20261012.334.1.
- Both red runs ran on ubuntu-26.04 20261101.186.1. GitHub moved `ubuntu-latest` to 26.04 between 2026-10-19 and 2026-11-19.
- The 2.0.0 library (SHA-256 pinned in bin/install-libduckdb) and the gem duckdb 2.0.0.1 were the same in all three runs. The three DuckDB 1.5.6 jobs pass on 26.04.
- actions/runner-images `configure-environment.sh` sets `ResourceDisk.SwapSizeMB=4096` through waagent, and its own comment says that ephemeral-OS VMs have no Azure resource disk. So 24.04 runners had 4 GB of swap, and the 26.04 runners probably have none. This was not measured on a runner.

## Cause

- Sequel keeps every Sequel::Database in `Sequel::DATABASES`, so garbage collection never frees one.
- The adapter keeps an in-memory DuckDB database open after `#disconnect`, so its data survives (lib/sequel/adapters/duckdb.rb). A file database gives its instance back to InstanceRegistry on disconnect.
- `SequelDuckDBTest.create_test_db` opens `duckdb::memory:`, and nothing closed it. So each test left one DuckDB instance and all of its worker threads alive until the process exited.
- DuckDB 2.0.0 starts about 160 more threads per database than 1.5.6, whatever the `threads` setting.

## Measurements (titan, 2026-11-12)

titan: 40 cores, 1.5 TB RAM, 32 GB swap, `vm.overcommit_memory = 0`, kernel 7.0.0-38. DuckDB 2.0.0 is the custom build at `~/.local/opt/duckdb/v2.0.0`, because the stock 2.x build needs AVX2. Ruby 4.0.7 for 2.0.0 and Ruby 3.4.8 for 1.5.6. The logs are in `claude_stuff/duckdb2/` and `claude_stuff/fork-probe-local-*`, `thread-leak-local-*` and `fix-test-db-leak-*` in the main checkout.

OS threads started by one in-memory database (`threads_per_db.rb`). Both versions stop all of them on `close`:

```
lib=1.5.6 threads=default  +39
lib=1.5.6 threads=4        +3
lib=2.0.0 threads=default  +199
lib=2.0.0 threads=4        +163
```

Process state at the fork test, seed 31646, before the fix (`fork_probe.rb`):

```
lib    threads  accounted mappings  accounted memory  RSS
1.5.6   19,557              20,106            162 GB  2.3 GB
2.0.0  100,197             100,763            828 GB  8.6 GB
```

The thread count at the fork depends on the test order. Another 1.5.6 seed gave 1,292 threads.

The unfixed 2.0.0 suite, seed 31646, under `systemd-run --user -p MemoryMax=<limit> -p MemorySwapMax=0`:

```
9G   oom-kill before the fork test
10G  oom-kill before the fork test
12G  oom-kill at the fork test
13G  pass
14G  pass
16G  pass
```

Pinning to 4 CPUs with `taskset -c 0-3` did not change the thread count.

After the fix (4617c36), 2.0.0, MemoryMax=4G, no swap:

```
seed   threads at fork  RSS at fork  result
31646              241       162 MB  pass
29850              241       146 MB  pass
61239              241       159 MB  pass
```

## What the kernel source shows

Linux v6.8 (24.04) and v7.0 (26.04) have the same overcommit check (`__vm_enough_memory` in mm/util.c) and the same per-mapping charge in `dup_mmap`. In mode 0 (heuristic), `fork` fails that check only when ONE mapping is larger than RAM plus swap. The largest mapping at the fork was the 2.1 GB heap, so the check cannot explain the error. Every other ENOMEM in `dup_mmap` is a failed kernel allocation. So the cause is memory pressure: the unfixed suite needs 12 to 13 GB, and the 16 GB runner without swap does not have that much free.

## Open questions

1. **Why ENOMEM and not an OOM kill?** Under a memory cgroup on titan, the process is killed. On the runner, `fork` returns ENOMEM and the suite continues. Not reproduced. The runner's `vm.overcommit_memory`, swap and free memory were never measured there. A diagnostic run would print them, but it costs CI minutes and does not change the fix.
2. **Should the adapter offer a way to close an in-memory database?** Today only garbage collection closes one, after the Database leaves `Sequel::DATABASES`. The test helper reaches into `@duckdb_state` to close it. A program that opens many short-lived in-memory databases leaks the same way. Not ruled.
3. **What are DuckDB 2.0.0's extra ~160 threads, and can a setting limit them?** They do not follow `threads`. This matters when the library moves to 2.x (re-evaluation in January 2027) for any host that opens several databases in one process. Not investigated.
