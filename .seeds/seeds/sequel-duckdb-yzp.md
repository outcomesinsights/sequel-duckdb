---
id: sequel-duckdb-yzp
title: 'DuckDB merges d + 30 days with d + 1 month (interval equality in its optimizer): report upstream, and handle the day/time residual?'
status: captured
type: question
created_at: 2026-10-16T13:44:51.378532+00:00
updated_at: 2026-11-09T14:11:51.035168+00:00
---

## The defect

In one query, DuckDB computes `d + INTERVAL 30 DAY` and `d + INTERVAL 1 MONTH` as one expression. Both return the value of whichever one comes first. The same happens for 60 days and 2 months, for 360 days and 1 year, and inside aggregates such as `max(d + 30 day)` and `max(d + 1 month)`. Reproduced with the DuckDB CLI v1.5.6 and with libduckdb 1.5.5 (conceptql seed conceptql-3wy, 2026-10-14).

Minimal case:

```
with t as (select date '2008-05-17' d)
select d + interval 30 day as a, d + interval 1 month as b from t
-- a = b = 2008-06-16; b should be 2008-06-17
```

Cause, as conceptql's session found it: expression_rewriter folds both intervals into constants. common_subexpressions then compares them with interval equality, and in that equality 1 month = 30 days. `SET disabled_optimizers TO 'common_subexpressions'` is not enough, because the aggregates still merge.

## What sequel-duckdb does about it

c139c5e, fix(date_arithmetic), writes years and months as `d - INTERVAL (-n) MONTH` and days and time units as `+`. The two forms no longer compare equal. test/interval_merge_test.rb covers 9 query shapes against a real DuckDB, so it fails if a later DuckDB merges the two forms again.

## Residual case: a day and a time interval of the same length (verified here)

c139c5e does not cover a DAY interval and a time interval of equal length, such as 1 day and 24 hours. Both are written with `+`, so they still merge. This changes the result only for TIMESTAMPTZ values across a daylight-saving change. Verified 2026-10-16 with libduckdb 1.5.5 and TimeZone America/Los_Angeles (claude_stuff/interval-residual-probe-\*.log):

```
with t as (select timestamptz '2026-03-07 12:00:00-08' d)
select d + interval 1 day as a, d + interval 24 hour as b from t
-- [2026-03-08 19:00:00 UTC, 2026-03-08 19:00:00 UTC]
select d + interval 24 hour as b, d + interval 1 day as a from t
-- [2026-03-08 20:00:00 UTC, 2026-03-08 20:00:00 UTC]
```

The correct values are 19:00 UTC for 1 day and 20:00 UTC for 24 hours. In each query, whichever expression comes first decides both values. `Sequel.date_add(ts, days: 1)` and `Sequel.date_add(ts, hours: 24)` in one query reach this case. conceptql uses only years, months and days, so it does not reach it.

## Open questions (not ruled)

1. Should the defect be reported to duckdb/duckdb? If yes, check it on the newest DuckDB release first. One search on 2026-10-14 found no existing DuckDB issue, but one query is not proof that none exists. Ryan ruled for conceptql: "that's sequel duckdb's issue, not yours". So the decision belongs to this repo.
2. Does the residual DAY/time case need handling in sequel-duckdb? One option is to write time units with a third form, for example `d - INTERVAL (-n) HOUR`, and keep days as `+`. That changes the SQL text for hours, minutes and seconds.

Related: conceptql bead conceptql-a4j bumps sequel-duckdb in conceptql's Gemfile.lock after the push, reruns its DuckDB suite, and then closes.

## RULED 2026-10-16 (Ryan): fix the day/time case too

Ryan: "fix the day/hour case too". The upstream report (open question 1) is not ruled.

UPDATE 2026-10-16: the day/time case shipped in 6ec6ec5 (bead sequel-duckdb-eca). Time units are written as INTERVAL n HOUR + d. The subtraction form was rejected, because it merges 720 hours with 1 month (claude_stuff/interval-third-form-probe-\*.log). The upstream report is still not ruled.

UPDATE 2026-10-27: The defect is still present in libduckdb 2.0.0 (claude_stuff/interval-merge-duckdb2-20261027-103759.log). Raw SQL, without sequel-duckdb's rewrite:

```
select d + interval 30 day as a, d + interval 1 month as b      -> [2008-06-16, 2008-06-16]
select d + interval 1 month as b, d + interval 30 day as a      -> [2008-06-17, 2008-06-17]
select max(d + interval 30 day) a, max(d + interval 1 month) b  -> [2008-06-16, 2008-06-16]
timestamptz 2026-03-07 12:00-08: d + 1 day, d + 24 hour (America/Los_Angeles) -> [2026-03-08 19:00 UTC, 2026-03-08 19:00 UTC]
```

So sequel-duckdb's interval forms (c139c5e, 6ec6ec5) are still needed on 2.x. Ryan chose on 2026-10-27 to record this only. The upstream report (open question 1) stays not ruled.

UPDATE 2026-11-09: DuckDB fixed the defect upstream, but only on main, which becomes 2.x. The issue is duckdb/duckdb#28308, and it closed on 2026-11-06. The fix is PR #28331, merged into `main` on 2026-11-06. It makes `BoundConstantExpression::Hash` and `Equals` compare INTERVAL constants by their raw (months, days, micros) fields. The fix was not backported to the 1.5 branch.

Ryan, 2026-11-09: "looks like the issue was fixed, but only for 2.0+. We're not moving to 2.0 until after the new year, so please just make note that we should revisit our patch at that time."

So sequel-duckdb keeps its interval forms (c139c5e, 6ec6ec5) while the library stays on 1.5.x. Revisit them when the fleet moves to DuckDB 2.x, after 2027-01-01. Before removing them, run test/interval_merge_test.rb and the raw-SQL probe against the 2.x release that contains #28331. Also check the aggregate case: on v2.0.0 the binder merged the `max()` aggregates before the optimizer ran. Keep the forms if the library range still allows 1.5.x.
