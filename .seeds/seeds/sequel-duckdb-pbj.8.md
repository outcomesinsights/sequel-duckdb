---
id: sequel-duckdb-pbj.8
title: 'DuckDB lambda expressions (lambda x: ...) in the duckdb_ops extension'
status: captured
type: idea
parent: sequel-duckdb-pbj
created_at: 2026-10-14T14:17:25.781098+00:00
updated_at: 2026-10-14T15:52:15.342528+00:00
tags:
  - hew
  - dsl
---

## Request

hew asked for this seed on 2026-10-14. Ryan approved filing it in the hew session on 2026-10-14: "I agree with your recommendations". Ryan has NOT ruled on building it.

pbj.1 to pbj.5 are built, pbj.6 is deferred, and pbj.7 is abandoned. After that, DuckDB lambda expressions are the only construct in hew's SQL ledger that the Sequel DSL cannot express at all. The "hew's N\* entries" note on sequel-duckdb-pbj already named this gap (carve.rb) as a candidate for the opt-in extension. hew's ledger records it as "Candidate: DuckDB lambda expressions".

## hew's uses (reported by hew, DuckDB 1.5.6, `lambda x: ...` syntax)

1. partition_id.rb line 82: a two-parameter lambda in list_reduce with an initial value. It is R's digest2int (Jenkins one-at-a-time hash) over a value's bytes, the fallback for values the unrolled form cannot handle.

   ```sql
   list_reduce(<list>, lambda hew_h, hew_b: xor(<step>, (<step>) >> 6), CAST(0 AS BIGINT))
   ```

2. partition_id.rb line 96: a one-parameter lambda in list_transform that maps hex byte strings to signed integers.

3. partition_id.rb line 102: a "let" idiom. It binds an expression to a name once, so the body can use it several times.

   ```sql
   list_transform([<expr>], lambda <var>: <body>)[1]
   ```

4. carve.rb line 296: hew plans to remove this one by calculating the offsets in Ruby, so it is not a requirement.

   ```sql
   list_sum(list_transform(list_slice(pieces, 1, i), lambda p: p."rows"))
   ```

## Verified here (2026-10-14, DuckDB 1.5.5 in this bundle)

All four forms run. The deprecated arrow form also still runs on 1.5.5.

```text
list_transform([1, 2, 3], lambda x: x + 1)                               => [2, 3, 4]
list_reduce([1, 2, 3], lambda acc, x: acc + x, CAST(0 AS BIGINT))        => 6
list_transform([40 + 2], lambda v: v * v + v)[1]                         => 1806
list_sum(list_transform([{rows: 3}, {rows: 4}], lambda p: p."rows"))     => 7
list_transform([1, 2], x -> x + 1)                                       => [2, 3]
```

Sequel already literalizes every part of a lambda BODY correctly. The only part it cannot write is the `lambda a, b:` prefix.

```text
Sequel.subscript(Sequel.function(:list_value, 10, 20), 1)   (list_value(10, 20))[1]   => 10
lambda p: + db.literal(Sequel.qualify(:p, :rows))           lambda p: "p"."rows"      => 7
lambda x: + db.literal(Sequel.+(:x, 1))                     lambda x: ("x" + 1)       => [2, 3]
quoted parameter name                                       lambda "x": "x" + 1       => [2, 3]
```

So a parameter can be written as a quoted identifier on both sides. The body is an ordinary Sequel expression whose parameter references are plain identifiers (Symbols). Logs: claude_stuff/lambda-probe-20261014-071650.log and claude_stuff/lambda-probe-sequel-20261014-071704.log.

## Placement (recommendation, not ruled)

The opt-in duckdb_ops extension, beside try_cast and duckdb_named. It is an expression builder, and users who do not load the extension get no change. That is the same rule that placed pbj.2 and pbj.5.

## Open questions (not ruled)

- Builder shape. (a) Parameters and a body expression: `Sequel.duckdb_lambda(:acc, :x, body)`. (b) A block that receives the parameter identifiers: `Sequel.duckdb_lambda(:acc, :x) { |acc, x| ... }`. Shape (b) is closer to Sequel's virtual-row style and makes a misspelled parameter a Ruby error.
- Whether to quote parameter names. Quoted works (verified above). Quoting matches how Sequel writes the references in the body, so both sides agree.
- Whether to add list-function helpers (list_transform, list_reduce, list_filter) or leave them to Sequel.function. Sequel.function already works for them once the lambda exists.
- Whether the "let" idiom deserves its own helper, or is just list_transform plus Sequel.subscript.

## RULED 2026-10-14 (Ryan, in the sequel-duckdb session)

Ryan: "let's move forward with implementing lambda support". He then ruled on the open questions:

- Builder shape: "Both forms". A block form, `Sequel.duckdb_lambda(:acc, :x) { |acc, x| acc + x }`, and a body-argument form, `Sequel.duckdb_lambda([:acc, :x], body)`, used when no block is given.
- Syntax: "lambda keyword (Recommended)". Emit `lambda x: body`, not the deprecated `x -> body`. The README says it needs a DuckDB that has the lambda keyword.
- Scope: "Lambda only (Recommended)". Add only `Sequel.duckdb_lambda` to the duckdb_ops extension. List functions go through `Sequel.function`, `[1]` goes through `Sequel.subscript`, and the README shows the "let" idiom as an example. No let helper and no list-function wrappers.
- Parameter names are quoted identifiers, so that they match how Sequel writes the references in the body (verified above).
