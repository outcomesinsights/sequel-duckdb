# Mock Connection Bug Analysis

## Error Summary

The downstream project is encountering a `NoMethodError` when using `Sequel::Mock::Connection`:

```
undefined method 'query' for an instance of Sequel::Mock::Connection (NoMethodError)
```

The error occurs at [duckdb.rb:928](../lib/sequel/adapters/shared/duckdb.rb#L928) in the `execute_statement` method.

## Root Cause

The `execute_statement` method in [duckdb.rb:906-965](../lib/sequel/adapters/shared/duckdb.rb#L906-L965) directly calls DuckDB-specific methods on the connection object:

1. **Line 917**: `conn.prepare(sql)` - Calls DuckDB::Connection#prepare
2. **Line 928**: `conn.query(sql)` - Calls DuckDB::Connection#query

However, `Sequel::Mock::Connection` only implements:
- `execute(sql)` - Delegates to the mock database's `_execute` method
- No `query()` method
- No `prepare()` method

## Problem Details

### Where It Fails

The error occurs in the `execute_statement` method when there are no parameters:

```ruby
def execute_statement(conn, sql, params = [], _opts = {})
  # ...
  if params && !params.empty?
    # Line 917: This will also fail with Mock::Connection
    stmt = conn.prepare(sql)
    # ...
  else
    # Line 928: THIS IS WHERE THE ERROR OCCURS
    result = conn.query(sql)
  end
  # ...
end
```

### Why It Happens

The adapter assumes it will always receive a `::DuckDB::Connection` object, which has:
- `query(sql)` - Execute SQL and return results
- `prepare(sql)` - Create a prepared statement
- `execute()` - Execute a prepared statement

But when using `Sequel.mock('duckdb://...')`, the connection pool contains `Sequel::Mock::Connection` objects instead, which only support the generic `execute(sql)` method.

## Impact

This bug prevents using the DuckDB adapter with Sequel's mock testing framework. Any test code that uses `Sequel.mock` with the DuckDB adapter will fail when attempting to execute queries.

The error manifests in the test suite when:
1. Using `Sequel.mock` to create a mock DuckDB database
2. Attempting any query operation (SELECT, INSERT, UPDATE, DELETE)
3. The `execute_statement` method tries to call `conn.query()` or `conn.prepare()`

## Solution Approach

### Research: How SQLite Adapter Works with Mock

**The Answer:** SQLite's adapter **DOES NOT** call adapter-specific connection methods directly in its `execute_statement`-equivalent code. Instead, it uses a completely different architecture.

**Key Files:**
- `sequel-5.96.0/lib/sequel/adapters/sqlite.rb` (lines 169-176, 251-270)
- `sequel-5.96.0/lib/sequel/adapters/mock.rb` (lines 7-30, 111-113, 143-173)

**How SQLite Works:**

1. **Database-level execute methods** (sqlite.rb:169-176):
   ```ruby
   # Public database methods that route through _execute
   def execute(sql, opts=OPTS, &block)
     _execute(:select, sql, opts, &block)
   end

   def execute_dui(sql, opts=OPTS)
     _execute(:update, sql, opts)
   end

   def execute_insert(sql, opts=OPTS)
     _execute(:insert, sql, opts)
   end
   ```

2. **Private _execute method** (sqlite.rb:251-270):
   ```ruby
   def _execute(type, sql, opts, &block)
     synchronize(opts[:server]) do |conn|
       # ONLY calls conn.query/execute INSIDE synchronize block
       # with REAL SQLite connections
       case type
       when :select
         log_connection_yield(sql, conn){conn.query(sql, args, &block)}
       when :insert
         log_connection_yield(sql, conn){conn.execute(sql, args)}
       end
     end
   end
   ```

3. **How Mock Database overrides this** (mock.rb:111-113):
   ```ruby
   # Mock::Database OVERRIDES execute to bypass _execute!
   def execute(sql, opts=OPTS, &block)
     synchronize(opts[:server]){|c| _execute(c, sql, opts, &block)}
   end
   ```

4. **Mock::Connection delegation** (mock.rb:27-29):
   ```ruby
   # When conn.execute(sql) is called, it goes back to database
   def execute(sql)
     @db.send(:_execute, self, sql, :log=>false)
   end
   ```

**The Critical Difference:**

**SQLite approach:**
- Database has `execute()`, `execute_dui()`, `execute_insert()` methods
- These call private `_execute(type, sql, opts)`
- `_execute` calls `synchronize` which yields the connection
- Inside `synchronize`, it calls `conn.query()` or `conn.execute()`
- **Mock::Database OVERRIDES the public execute() methods**, so `_execute` never calls `conn.query()`!

**DuckDB current approach (BROKEN):**
- DuckDB's **shared** adapter defines `execute()` (unlike SQLite's shared adapter which doesn't)
- This `execute()` calls `synchronize(){|conn| execute_statement(conn, ...)}`
- `execute_statement` DIRECTLY calls `conn.query()` and `conn.prepare()`
- When mocking, `conn` IS a Mock::Connection, which doesn't have those methods
- Mock::Database can't override this because the shared adapter's `execute()` is what gets extended

### Implementation Strategy

**Approach: Follow SQLite's Pattern - Don't Call Connection Methods Directly in Private Helpers**

The issue is that `execute_statement` is a **helper method** that receives an already-obtained connection and tries to call adapter-specific methods on it. SQLite doesn't do this - it only calls adapter-specific connection methods **inside the synchronize block** in `_execute`.

**Solution:** DuckDB's adapter should **NOT** have connection-specific logic in `execute_statement` when mocking. Instead:

1. **Check if connection is a Mock::Connection at the START of execute_statement**
2. **If mock, call conn.execute() and return immediately** (which delegates to database's `_execute`)
3. **If real DuckDB connection, use the existing logic**

```ruby
def execute_statement(conn, sql, params = [], _opts = {})
  # Pattern: Check for Mock::Connection first (inspired by sqlite.rb architecture)
  # Mock connections should be handled before any adapter-specific calls
  if conn.is_a?(Sequel::Mock::Connection)
    # Mock connections only support execute(), which delegates back to
    # database's _execute method (mock.rb:27-29)
    # The database's _execute handles logging and result formatting
    return conn.execute(sql)
  end

  # Real DuckDB connection - use native methods
  start_time = Time.now

  begin
    log_sql_query(sql, params)

    if params && !params.empty?
      stmt = conn.prepare(sql)
      params.each_with_index do |param, index|
        stmt.bind(index + 1, param)
      end
      result = stmt.execute
    else
      result = conn.query(sql)
    end

    # ... rest of existing logic for real connections
  end
end
```

**Why `is_a?(Sequel::Mock::Connection)` instead of `respond_to?`:**
- SQLite doesn't need this check because Mock::Database overrides at database level
- DuckDB has connection-level logic, so we need to detect mocks explicitly
- `is_a?` is clearer about intent: "handle mocks differently"
- `respond_to?(:query)` could give false negatives if DuckDB adds new connection types

**Why this pattern works:**
- Mock connections have a special delegation pattern that needs early detection
- Real connections get the full DuckDB-specific optimizations
- Follows the principle: "Detect special cases early, handle normal cases normally"
- Mirrors SQLite's architectural approach of keeping adapter-specific calls away from generic code paths

## Testing Implications

Once fixed, the adapter should support:

1. **Mock Testing**: Full compatibility with `Sequel.mock` for unit testing
2. **SQL Verification**: Tests can verify SQL generation without a real database
3. **Behavior Testing**: Test application logic without DuckDB dependency
4. **Test Isolation**: Fast, isolated tests that don't require database setup

## Files to Modify

- [lib/sequel/adapters/shared/duckdb.rb](../lib/sequel/adapters/shared/duckdb.rb) - Modify `execute_statement` method (lines 906-965)

## Validation

After implementation, verify:

1. Existing tests still pass with real DuckDB connections
2. Mock connections work correctly
3. Both parameterized and non-parameterized queries work with mocks
4. Error handling still functions properly
5. SQL logging works with mock connections

## Can We Move Execute Logic to Real Adapter (Like SQLite)?

**Short answer: Yes, we could, but there's no good reason to.**

**Comparison:**

**SQLite adapter:**
- 461 lines total
- 27 methods
- Simple execution logic (no custom logging, basic error handling)
- Real adapter has execute logic
- Shared adapter only has schema introspection (no execute methods)

**DuckDB adapter:**
- 2,483 lines in shared adapter alone
- 141 methods
- Sophisticated execution features:
  - Custom SQL query/timing/error logging (log_sql_query, log_sql_timing, log_sql_error)
  - Enhanced error mapping (database_exception_class maps 10+ error patterns)
  - Enhanced error messages (database_exception_message adds SQL + params context)
  - Parameterized query support with binding
  - Row hash conversion from DuckDB result arrays
  - Result type detection (::DuckDB::Result)

**Why DuckDB's execution logic is in shared adapter:**
1. **Code reuse** - Both real adapter AND mock need the same sophisticated error handling
2. **Testing** - Mocks should get the same error messages, logging, etc.
3. **Architecture** - Real adapter is minimal (just connection lifecycle), shared adapter is the "brain"

**What's DuckDB-specific that can't be shared:**
- Everything in execute_statement references `::DuckDB::Connection`, `::DuckDB::Result`, `::DuckDB::Error`
- These types don't exist when mocking

**Could we move execute() to real adapter?**
- Yes, technically possible
- Would require duplicating all the error handling, logging, parameter binding logic
- OR would require the shared adapter to still have helper methods that call `conn.query()`
- Either way, we'd still need Mock::Connection detection somewhere

**~~Better~~ Band-aid solution:**
- ~~Keep architecture as-is (it's well-designed)~~
- ~~Add Mock::Connection detection to `execute_statement()`~~
- ~~This is a 3-line fix vs restructuring the entire adapter~~

**ACTUAL Better solution:**
- The adapter is over-engineered (see [over_engineering_analysis.md](over_engineering_analysis.md))
- DuckDB reimplements what Sequel already provides (logging, error handling, timing)
- SQLite uses `log_connection_yield()` in ONE LINE - handles all logging/timing/errors
- DuckDB should be simplified to follow SQLite's patterns
- This would both fix the Mock issue AND reduce code from 2,483 to ~750 lines

## Summary

**Root Cause:** The DuckDB adapter calls adapter-specific connection methods (`conn.query()`, `conn.prepare()`) in a helper method that receives pre-obtained connections. Mock::Connection doesn't implement these methods.

**Why SQLite works:**
- Real SQLite adapter (sqlite.rb:169-270) defines `execute()` and `_execute()` that call `conn.query()`
- **Shared SQLite adapter (shared/sqlite.rb) does NOT define execute methods** - only schema introspection
- Mock::Database (mock.rb:111-113, 143-173) **overrides execute() and provides its own _execute()** that NEVER calls connection methods
- So when mocking, Mock::Database's execute() is used, which bypasses all adapter-specific connection method calls

**Why DuckDB breaks (architectural difference):**

DuckDB uses a different adapter architecture than SQLite:

**SQLite architecture:**
- Real adapter (sqlite.rb) defines `execute()` and `_execute()` - contains ALL execution logic
- Shared adapter (shared/sqlite.rb) defines ZERO execution methods - only schema introspection
- When mocking, Mock::Database's execute() is used, bypassing the real adapter entirely

**DuckDB architecture:**
- Real adapter (duckdb.rb) defines ONLY connection lifecycle methods (`connect`, `disconnect_connection`)
- **Shared adapter (shared/duckdb.rb:75) defines ALL execution logic including `execute()`**
- Real adapter includes the shared adapter's DatabaseMethods module
- Both real AND mock use the same execute() from the shared adapter

**The breakage:**
- When you call `Sequel.mock('duckdb://...')`, Mock::Database extends shared/duckdb.rb
- Mock::Database gets the shared adapter's `execute()` method
- This execute() calls `synchronize(){|conn| execute_statement(conn, ...)}`
- execute_statement() calls `conn.query()` on what IS a Mock::Connection
- Mock::Connection doesn't implement query() → NoMethodError

**Why this architecture was chosen:**
- All execution logic in shared adapter = code reuse
- Real adapter only handles DuckDB-specific connection details
- But this means the shared adapter MUST handle mocking too

**Fix:** Add an early check for `Sequel::Mock::Connection` at the start of `execute_statement()` and delegate to `conn.execute()` which will route back to the Mock::Database's `_execute` method for proper handling.

**Inspiration:** SQLite adapter's architecture (sequel-5.96.0/lib/sequel/adapters/sqlite.rb:169-270) combined with understanding of Mock::Connection delegation pattern (sequel-5.96.0/lib/sequel/adapters/mock.rb:27-29, 111-113).
