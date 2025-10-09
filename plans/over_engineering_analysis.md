# Over-Engineering Analysis: DuckDB vs SQLite Adapters

## Executive Summary

After deep analysis of Sequel core, SQLite adapter, and DuckDB adapter, the verdict is clear:

**The DuckDB adapter is over-engineered by ~1900 lines (71% code bloat)**

- **SQLite Total**: ~1536 lines (462 real + 1074 shared)
- **DuckDB Current**: ~2741 lines (257 real + 2484 shared)
- **DuckDB Target**: ~800 lines
- **Lines to Delete**: ~1941 lines

## What Sequel Already Provides

### Logging System (`database/logging.rb`)

**`log_connection_yield(sql, conn, args)` - The ONE method for SQL execution**

Automatically provides:
- SQL query logging with arguments
- Timing/duration measurement (via `Sequel.start_timer`)
- Exception logging (via `log_exception`)
- Connection info (if `log_connection_info` enabled)
- WARN level for slow queries (configurable via `log_warn_duration`)
- Respects user's `sql_log_level` configuration (:info, :debug)
- Zero overhead when `loggers` array is empty

**Usage Pattern:**
```ruby
log_connection_yield(sql, conn, args) do
  conn.execute(sql, args)
end
```

**No Need For:**
- `log_sql_query(sql, params)` - log_connection_yield does this
- `log_sql_timing(sql, time)` - log_connection_yield does this
- `log_sql_error(sql, error)` - log_connection_yield does this
- Manual `Time.now` calls - log_connection_yield handles timing
- Custom timing variables - log_connection_yield uses `Sequel.start_timer`

### Error Handling System (`database/misc.rb`)

**`raise_error(exception, opts)` - The ONE method for exception conversion**

Automatically:
- Converts driver exceptions to Sequel exception classes
- Preserves original message and backtrace
- Calls `database_error_class(exception, opts)` for classification
- Respects `database_error_classes` for filtering
- Uses `database_specific_error_class` for error codes
- Falls back to `database_error_regexps` for pattern matching
- Supports SQLState codes via `database_exception_sqlstate`

**Exception Hierarchy:**
```
Sequel::Error
├── Sequel::DatabaseError
│   ├── Sequel::DatabaseConnectionError
│   ├── Sequel::DatabaseDisconnectError
│   ├── Sequel::ConstraintViolation
│   │   ├── Sequel::CheckConstraintViolation
│   │   ├── Sequel::ForeignKeyConstraintViolation
│   │   ├── Sequel::NotNullConstraintViolation
│   │   └── Sequel::UniqueConstraintViolation
│   └── Sequel::SerializationFailure
```

**No Need For:**
- `database_exception_class(exception, opts)` - Use `database_specific_error_class` + `database_error_regexps`
- `database_exception_message(exception, opts)` - Sequel formats messages
- `handle_constraint_violation(exception, opts)` - `raise_error` handles all exceptions

### Connection Management (`database/connecting.rb`)

**`synchronize(server, &block)` - The ONE method for connection access**

Automatically:
- Acquires connection from thread-safe pool
- Yields connection to block
- Returns connection to pool (even on exception)
- Handles server-specific connections
- Manages connection lifecycle

**No Need For:**
- Direct `@pool` access
- Custom connection management
- Manual connection return
- Custom pooling logic

## SQLite's Approach (The Gold Standard)

### File Structure

```
adapters/sqlite.rb (462 lines)
  - Type conversion: 100 lines (SQLite-specific necessity)
  - Connection: 50 lines
  - Execution (_execute): 100 lines
  - Dataset (fetch_rows): 100 lines
  - Comments: 112 lines

adapters/shared/sqlite.rb (1074 lines)
  - DatabaseMethods: 556 lines
    - Configuration: 50 lines
    - Schema introspection: 150 lines
    - ALTER TABLE emulation: 250 lines (SQLite limitation)
    - Error classification: 33 lines (DECLARATIVE)
    - Other: 73 lines
  - DatasetMethods: 490 lines
    - SQL generation overrides: 300 lines
    - Feature detection: 50 lines
    - Helpers: 140 lines
  - Comments: 28 lines

Total: ~1536 lines
```

### Execution Pattern (sqlite.rb:251-270)

**The ENTIRE execution system in 20 lines:**

```ruby
def execute(sql, opts=OPTS, &block)
  _execute(:select, sql, opts, &block)
end

def execute_dui(sql, opts=OPTS)
  _execute(:update, sql, opts)
end

def execute_insert(sql, opts=OPTS)
  _execute(:insert, sql, opts)
end

private

def _execute(type, sql, opts, &block)
  synchronize(opts[:server]) do |conn|
    return execute_prepared_statement(conn, type, sql, opts, &block) if sql.is_a?(Symbol)

    log_args = opts[:arguments]
    args = {}
    opts.fetch(:arguments, OPTS).each{|k, v| args[k] = prepared_statement_argument(v)}

    case type
    when :select
      log_connection_yield(sql, conn, log_args){conn.query(sql, args, &block)}
    when :insert
      log_connection_yield(sql, conn, log_args){conn.execute(sql, args)}
      conn.last_insert_row_id
    when :update
      log_connection_yield(sql, conn, log_args){conn.execute_batch(sql, args)}
      conn.changes
    end
  end
rescue SQLite3::Exception => e
  raise_error(e)
end
```

**What it does:**
1. Type dispatch (select/insert/update)
2. Connection pooling via `synchronize`
3. Prepared statement support
4. Argument extraction
5. Logging/timing via `log_connection_yield`
6. Return appropriate values
7. Exception conversion via `raise_error`

**What it doesn't do:**
- ❌ Manual timing
- ❌ Custom logging
- ❌ Manual error classification
- ❌ Custom error messages
- ❌ Complex try/catch/ensure
- ❌ Manual connection management

### Error Classification (shared/sqlite.rb:372-404)

**The ENTIRE error handling system in 33 lines:**

```ruby
DATABASE_ERROR_REGEXPS = {
  /(is|are) not unique\z|PRIMARY KEY must be unique\z|UNIQUE constraint failed: .+\z/ => UniqueConstraintViolation,
  /foreign key constraint failed\z/i => ForeignKeyConstraintViolation,
  /\ASQLITE ERROR 3091/ => CheckConstraintViolation,
  /\A(SQLITE ERROR 275 \(CONSTRAINT_CHECK\) : )?CHECK constraint failed/ => CheckConstraintViolation,
  /\A(SQLITE ERROR 19 \(CONSTRAINT\) : )?constraint failed\z/ => ConstraintViolation,
  /\Acannot store [A-Z]+ value in [A-Z]+ column / => ConstraintViolation,
  /may not be NULL\z|NOT NULL constraint failed: .+\z/ => NotNullConstraintViolation,
  /\ASQLITE ERROR \d+ \(\) : CHECK constraint failed: / => CheckConstraintViolation
}.freeze

def database_error_regexps
  DATABASE_ERROR_REGEXPS
end

def database_specific_error_class(exception, opts)
  case sqlite_error_code(exception)
  when 1299 then NotNullConstraintViolation
  when 1555, 2067, 2579 then UniqueConstraintViolation
  when 787 then ForeignKeyConstraintViolation
  when 275 then CheckConstraintViolation
  when 19 then ConstraintViolation
  when 517 then SerializationFailure
  else
    super  # Falls back to regex matching
  end
end
```

**Pattern:**
1. Try error codes first (most reliable)
2. Fall back to regex patterns
3. Always call `super` for unmatched cases
4. Data-driven, not procedural

## DuckDB's Approach (Over-Engineered)

### File Structure

```
adapters/duckdb.rb (257 lines)
  - Connection: 30 lines
  - Disconnect: 10 lines
  - Validation: 10 lines
  - Comments: 207 lines (!!)

adapters/shared/duckdb.rb (2484 lines!)
  - DatabaseMethods: ~1350 lines
    - execute/execute_statement: 100 lines
    - Custom logging: 80 lines ❌ UNNECESSARY
    - Custom error handling: 80 lines ❌ UNNECESSARY
    - Transaction code: 190 lines ❌ MOSTLY UNNECESSARY
    - Schema introspection: 200 lines
    - Configuration: 85 lines
    - EXPLAIN: 50 lines
    - Schema create/drop: 130 lines
    - Type conversion: 50 lines
  - DatasetMethods: ~1130 lines
    - SQL generation: 330 lines ❌ MOSTLY UNNECESSARY
    - Performance "optimizations": 500 lines ❌ UNNECESSARY
    - Index hints: 150 lines ❌ UNNECESSARY
    - Streaming: 100 lines ❌ UNNECESSARY
    - Other: 50 lines

Total: ~2741 lines
```

### Problem #1: Custom Logging (Lines 975-1053, ~80 lines to delete)

**Current:**
```ruby
def log_sql_query(sql, params = [])
  return unless log_connection_info?
  if params && !params.empty?
    log_info("SQL Query: #{sql} -- Parameters: #{params.inspect}")
  else
    log_info("SQL Query: #{sql}")
  end
end

def log_sql_timing(sql, execution_time)
  return unless log_connection_info?
  time_ms = (execution_time * 1000).round(2)
  if execution_time > 1.0
    log_warn("SLOW SQL Query (#{time_ms}ms): #{sql}")
  else
    log_info("SQL Query completed in #{time_ms}ms")
  end
end

def log_sql_error(sql, params, error, execution_time)
  return unless log_connection_info?
  time_ms = (execution_time * 1000).round(2)
  if params && !params.empty?
    log_error("SQL Error after #{time_ms}ms: #{error.message} -- SQL: #{sql} -- Parameters: #{params.inspect}")
  else
    log_error("SQL Error after #{time_ms}ms: #{error.message} -- SQL: #{sql}")
  end
end

def log_connection_info?
  !loggers.empty?
end

def log_info(message)
  log_connection_yield(message, nil) { nil }
end

def log_warn(message)
  log_connection_yield("WARNING: #{message}", nil) { nil }
end

def log_error(message)
  log_connection_yield("ERROR: #{message}", nil) { nil }
end
```

**Problem:** Reimplements what `log_connection_yield` already does!

**Should Be:**
```ruby
# DELETE ALL OF THIS - just use log_connection_yield
```

**Lines Saved: 80**

### Problem #2: Custom Error Handling (Lines 160-238, ~80 lines to delete)

**Current:**
```ruby
def database_exception_class(exception, _opts)
  message = exception.message.to_s
  case message
  when /connection/i, /database.*not.*found/i, /cannot.*open/i
    Sequel::DatabaseConnectionError
  when /violates.*not.*null/i, /not.*null.*constraint/i, /null.*value.*not.*allowed/i
    Sequel::NotNullConstraintViolation
  when /unique.*constraint/i, /duplicate.*key/i, /already.*exists/i,
       /primary.*key.*constraint/i, /duplicate.*primary.*key/i
    Sequel::UniqueConstraintViolation
  when /foreign.*key.*constraint/i, /violates.*foreign.*key/i
    Sequel::ForeignKeyConstraintViolation
  when /check.*constraint/i, /violates.*check/i
    Sequel::CheckConstraintViolation
  when /constraint.*violation/i, /violates.*constraint/i
    Sequel::ConstraintViolation
  else
    Sequel::DatabaseError
  end
end

def database_exception_message(exception, opts)
  message = "DuckDB error: #{exception.message}"
  message += " -- SQL: #{opts[:sql]}" if opts[:sql]
  message += " -- Parameters: #{opts[:params].inspect}" if opts[:params] && !opts[:params].empty?
  message
end

def handle_constraint_violation(exception, opts = {})
  message = database_exception_message(exception, opts)
  exception_class = database_exception_class(exception, opts)
  exception_class.new(message)
end
```

**Should Be:**
```ruby
DATABASE_ERROR_REGEXPS = {
  /unique.*constraint|duplicate.*key|already.*exists/i => UniqueConstraintViolation,
  /foreign.*key.*constraint/i => ForeignKeyConstraintViolation,
  /not.*null.*constraint|null.*value.*not.*allowed/i => NotNullConstraintViolation,
  /check.*constraint/i => CheckConstraintViolation,
  /constraint.*violation|violates.*constraint/i => ConstraintViolation,
}.freeze

def database_error_regexps
  DATABASE_ERROR_REGEXPS
end
```

**Lines Saved: 73** (from 80 to 7)

### Problem #3: execute_statement Complexity (Lines 906-973, ~70 lines to delete)

**Current:**
```ruby
def execute_statement(conn, sql, params = [], _opts = {})
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

    end_time = Time.now
    execution_time = end_time - start_time
    log_sql_timing(sql, execution_time)

    if block_given?
      columns = result.columns
      result.each do |row_array|
        row_hash = {}
        columns.each_with_index do |column, index|
          column_name = column.respond_to?(:name) ? column.name : column.to_s
          row_hash[column_name.to_sym] = row_array[index]
        end
        yield row_hash
      end
    else
      result
    end
  rescue ::DuckDB::Error => e
    end_time = Time.now
    execution_time = end_time - start_time
    log_sql_error(sql, params, e, execution_time)

    error_opts = { sql: sql, params: params }
    exception_class = database_exception_class(e, error_opts)
    enhanced_message = database_exception_message(e, error_opts)

    raise exception_class, enhanced_message
  rescue StandardError => e
    end_time = Time.now
    execution_time = end_time - start_time
    log_sql_error(sql, params, e, execution_time)
    raise e
  end
end
```

**Should Be (in real adapter):**
```ruby
def _execute(type, sql, opts, &block)
  synchronize(opts[:server]) do |conn|
    case type
    when :select
      log_connection_yield(sql, conn) do
        result = conn.query(sql)
        block.call(result) if block
        result
      end
    when :insert
      log_connection_yield(sql, conn) { conn.query(sql) }
      nil
    when :update
      log_connection_yield(sql, conn) do
        result = conn.query(sql)
        result.rows_changed
      end
    end
  end
rescue ::DuckDB::Error => e
  raise_error(e, opts)
end
```

**Lines Saved: 50** (from 70 to 20)

### Problem #4: Performance "Optimization" Code (Lines 1973-2465, ~500 lines to delete)

**Current Implementation Has:**
- Custom batching in `each` (50 lines)
- Memory usage tracking (40 lines)
- Custom streaming (80 lines)
- Prepared statement wrappers (50 lines)
- Connection pooling wrappers (30 lines)
- Index hints (150 lines)
- Columnar optimization hints (50 lines)
- Parallel execution hints (50 lines)

**Problems:**
1. Premature optimization (no benchmarks proving benefit)
2. DuckDB handles parallelization automatically
3. Index hints don't actually do anything in DuckDB
4. Sequel already handles streaming/batching
5. Adds complexity without proven value
6. None of this is in SQLite adapter

**Should Be:**
```ruby
# Simple fetch_rows - let DuckDB handle optimization
def fetch_rows(sql)
  execute(sql) do |result|
    cols = result.columns.map{|c| output_identifier(c.name)}
    self.columns = cols
    result.each do |row_array|
      row = {}
      cols.each_with_index{|col, i| row[col] = row_array[i]}
      yield row
    end
  end
end
```

**Lines Saved: 450** (from 500 to 50)

### Problem #5: Transaction Code (Lines 632-822, ~190 lines to delete)

**Current:**
- `begin_transaction` - 15 lines
- `commit_transaction` - 5 lines
- `rollback_transaction` - 5 lines
- `savepoint_transaction` - 50 lines (DuckDB doesn't support savepoints!)
- `isolation_transaction` - 50 lines (DuckDB doesn't support isolation levels!)
- Feature detection - 30 lines
- Other - 35 lines

**Problem:** Implements features DuckDB doesn't support

**Should Be:**
```ruby
def supports_savepoints?
  false
end

def supports_transaction_isolation_level?(_level)
  false
end

# Let Sequel handle standard BEGIN/COMMIT/ROLLBACK
```

**Lines Saved: 180** (from 190 to 10)

### Problem #6: SQL Generation Bloat (Lines 1357-1690, ~330 lines)

**Current:** Completely reimplements:
- insert_sql (40 lines)
- update_sql (20 lines)
- delete_sql (15 lines)
- select_with_sql (30 lines)
- select_from_sql (15 lines)
- select_join_sql (100 lines)
- select_where_sql (10 lines)
- select_group_sql (15 lines)
- select_having_sql (10 lines)
- select_order_sql (30 lines)
- Other (45 lines)

**Problem:** Most of this is standard SQL that Sequel already generates correctly

**Should Be:**

Only override what's actually different in DuckDB:

```ruby
def complex_expression_sql_append(sql, op, args)
  case op
  when :ILIKE
    # DuckDB doesn't have ILIKE, emulate with UPPER
    sql << "(UPPER("
    literal_append(sql, args.first)
    sql << ") LIKE UPPER("
    literal_append(sql, args.last)
    sql << "))"
  when :~
    # DuckDB regex syntax if different
    sql << "(regexp_matches("
    literal_append(sql, args.first)
    sql << ", "
    literal_append(sql, args.last)
    sql << "))"
  else
    super
  end
end
```

**Lines Saved: 250** (from 330 to 80)

### Problem #7: Schema Management (Lines 1173-1301, ~130 lines)

**Current:** Full CREATE SCHEMA / DROP SCHEMA implementation

**Problem:** Only needed if DuckDB syntax differs from standard SQL

**Should Be:**

Test if DuckDB uses standard syntax. If yes, delete. If no, keep only the differences.

**Lines Saved: ~100** (likely)

### Problem #8: Configuration Bloat (Lines 367-450, ~85 lines)

**Current:**
- set_pragma (20 lines)
- configure_duckdb (15 lines)
- configure_parallel_execution (15 lines)
- configure_memory_optimization (10 lines)
- configure_columnar_optimization (10 lines)
- Other config methods (15 lines)

**Should Be (SQLite pattern):**
```ruby
def connection_pragmas
  ps = []
  ps << "PRAGMA threads = #{opts[:threads]}" if opts[:threads]
  ps << "PRAGMA memory_limit = '#{opts[:memory_limit]}'" if opts[:memory_limit]
  # ... other pragmas
  ps
end

# Then in connect:
connection_pragmas.each{|s| log_connection_yield(s, conn){conn.execute(s)}}
```

**Lines Saved: 70** (from 85 to 15)

## Summary of Lines to Delete

| Problem Area | Current Lines | Target Lines | Lines Saved |
|-------------|--------------|--------------|-------------|
| Custom Logging | 80 | 0 | **80** |
| Custom Error Handling | 80 | 7 | **73** |
| execute_statement | 70 | 0 | **70** |
| Public execute Method | 45 | 0 | **45** |
| Performance Code | 500 | 50 | **450** |
| Transaction Code | 190 | 10 | **180** |
| Schema Management | 130 | 30 | **100** |
| Type Conversion | 50 | 30 | **20** |
| Configuration | 85 | 15 | **70** |
| SQL Generation | 330 | 80 | **250** |
| **TOTAL** | **1560** | **222** | **1338** |

Plus ~600 lines of comments/docs for deleted features = **~1938 lines total reduction**

## Target Architecture

### Real Adapter (~300 lines)

```
Connection management: 30 lines
  - connect method
  - disconnect_connection
  - valid_connection?

Execution: 40 lines
  - execute
  - execute_dui
  - execute_insert
  - _execute (20 lines)

Error handling: 5 lines
  - database_error_classes

Type conversion: 50 lines (if needed for DuckDB types)

Dataset: 50 lines
  - fetch_rows

Comments/structure: 125 lines
```

### Shared Adapter (~500 lines)

```
Error classification: 10 lines
  - DATABASE_ERROR_REGEXPS hash
  - database_error_regexps method

Schema introspection: 150 lines
  - schema_parse_table
  - schema_parse_indexes
  - tables, indexes methods

Configuration: 15 lines
  - connection_pragmas

SQL generation overrides: 80 lines
  - Only what differs from standard SQL
  - complex_expression_sql_append
  - literal_* methods

Feature detection: 50 lines
  - supports_* methods

Helper methods: 50 lines
  - Type mapping
  - Value parsing

Comments/structure: 145 lines
```

### Total: ~800 lines (71% reduction)

## Refactoring Strategy

### Phase 1: Execution Simplification (High Priority)

1. **Move execution to real adapter** (adapters/duckdb.rb)
   - Implement `_execute` pattern
   - Add execute/execute_dui/execute_insert wrappers
   - Add database_error_classes

2. **Delete from shared adapter:**
   - execute method (45 lines)
   - execute_statement method (70 lines)
   - All custom logging methods (80 lines)
   - Custom error handling methods (80 lines)

**Lines Saved: 275**
**Risk: Low** (uses Sequel's standard patterns)

### Phase 2: Error Handling Simplification (High Priority)

1. **Add to shared adapter:**
   - DATABASE_ERROR_REGEXPS hash (10 lines)
   - database_error_regexps method (3 lines)

2. **Delete from shared adapter:**
   - database_exception_class (45 lines)
   - database_exception_message (10 lines)
   - handle_constraint_violation (7 lines)

**Lines Saved: 49**
**Risk: Low** (declarative pattern, easy to test)

### Phase 3: Remove Performance Code (Medium Priority)

1. **Simplify Dataset#fetch_rows** to standard pattern
2. **Delete:**
   - Custom batching (50 lines)
   - Memory tracking (40 lines)
   - Custom streaming (80 lines)
   - Prepared statement wrappers (50 lines)
   - Connection pooling wrappers (30 lines)
   - Index hints (150 lines)
   - Optimization hints (100 lines)

**Lines Saved: 500**
**Risk: Low** (premature optimization, no proven benefit)

### Phase 4: Simplify Transactions (Medium Priority)

1. **Keep:**
   - Feature detection methods (10 lines)

2. **Delete:**
   - savepoint_transaction (50 lines)
   - isolation_transaction (50 lines)
   - begin/commit/rollback overrides (25 lines)
   - Other transaction code (55 lines)

**Lines Saved: 180**
**Risk: Low** (DuckDB doesn't support these features)

### Phase 5: Simplify SQL Generation (Lower Priority)

1. **Audit each SQL generation method:**
   - Test if DuckDB accepts Sequel's default SQL
   - Delete if yes, keep only if DuckDB syntax differs

2. **Expected:**
   - Keep ~80 lines of DuckDB-specific syntax
   - Delete ~250 lines of standard SQL generation

**Lines Saved: 250**
**Risk: Medium** (requires testing each SQL type)

### Phase 6: Configuration Cleanup (Lower Priority)

1. **Implement connection_pragmas pattern** (15 lines)
2. **Delete custom config methods** (70 lines)

**Lines Saved: 70**
**Risk: Low** (SQLite pattern, well-tested)

### Phase 7: Schema Management (Lower Priority)

1. **Test if DuckDB uses standard CREATE/DROP SCHEMA syntax**
2. **Delete if standard** (~100 lines)

**Lines Saved: ~100**
**Risk: Medium** (requires testing)

## Expected Benefits

### 1. Maintainability
- Code matches SQLite adapter pattern
- Easier for other developers to understand
- Follows Sequel best practices
- Less code = fewer bugs

### 2. Correctness
- Uses Sequel's battle-tested features
- Proper error classification via regex
- Proper logging via log_connection_yield
- Proper connection pooling via synchronize

### 3. Performance
- No custom overhead
- Uses Sequel's optimized code paths
- DuckDB's native optimizations work better
- Simpler code = faster execution

### 4. Compatibility
- Works with all Sequel features
- Works with all Sequel plugins
- Proper Mock support
- Standard exception hierarchy

### 5. Testing
- Less code = easier to test
- Standard patterns = existing tests work
- Simpler execution path = easier debugging

## The Real Question

**Did the custom logging/error handling/optimization help develop the adapter?**

**No. It:**
- Created complexity that broke Mock support
- Reinvented what Sequel already provides (and provides better)
- Made the adapter harder to understand and maintain
- Didn't provide measurable value over Sequel's built-in features
- Added ~1900 lines of unnecessary code
- Made debugging harder (more code to trace through)
- Prevented use of Sequel's standard patterns

**If Jeremy Evans didn't need it for SQLite, we don't need it for DuckDB.**

## Conclusion

The DuckDB adapter should follow the SQLite adapter pattern:

1. **Use `log_connection_yield`** for all execution
2. **Use `raise_error`** for all error handling
3. **Use declarative error classification** (DATABASE_ERROR_REGEXPS)
4. **Use `synchronize`** for all connection access
5. **Override only what's different** in SQL generation
6. **Trust Sequel's features** - they're battle-tested and optimized

**Target: ~800 lines total (71% reduction from current 2741 lines)**

This isn't about being clever or adding features. It's about writing **maintainable, correct, simple** code that works like every other Sequel adapter.

---

See detailed engineering insights:
- [plans/engineering/Sequel.md](engineering/Sequel.md) - Sequel core architecture
- [plans/engineering/sqlite.md](engineering/sqlite.md) - SQLite adapter analysis
- [plans/engineering/duckdb.md](engineering/duckdb.md) - DuckDB refactoring details
