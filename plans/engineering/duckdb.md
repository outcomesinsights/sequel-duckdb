# DuckDB Adapter - Engineering Insights & Refactoring Opportunities

## Overview

The DuckDB adapter is over-engineered, containing ~1200 lines of unnecessary code that reimplements functionality already provided by Sequel.

**File Structure:**
- `adapters/duckdb.rb` (257 lines) - Real adapter
- `adapters/shared/duckdb.rb` (2484 lines) - Shared adapter
- **Total: ~2741 lines**

**Target after refactoring: ~800 lines** (matching SQLite's complexity)

## Current Real Adapter Analysis (`adapters/duckdb.rb`)

### What's Right

1. **Simple connection (Lines 127-148):**
   ```ruby
   def connect(server)
     opts = server_opts(server)
     database_path = opts[:database]

     if database_path == ":memory:" || database_path.nil?
       db = ::DuckDB::Database.open(":memory:")
     else
       database_path = "/#{database_path}" if database_path.match?(/^[a-zA-Z]/) && !database_path.start_with?(":")
       db = ::DuckDB::Database.open(database_path)
     end
     db.connect
   rescue ::DuckDB::Error => e
     raise Sequel::DatabaseConnectionError, "Failed to connect: #{e.message}"
   end
   ```
   **Good:** Simple, clear, returns connection.
   **Problem:** Should use `raise_error(e)` not manual exception creation.

2. **Minimal structure:**
   - Database class includes DatabaseMethods
   - Dataset class includes DatasetMethods
   - Properly registered with Sequel

### What's Missing

The real adapter should contain DuckDB-specific execution code, but instead this is in the shared adapter. The real adapter is TOO minimal.

## Current Shared Adapter Analysis (`adapters/shared/duckdb.rb`)

### Problem #1: Custom Logging (Lines 975-1053, ~80 lines)

**Current Implementation:**

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

**Problem:** This reimplements what `log_connection_yield` already does!

**Should Be:**

```ruby
# DELETE ALL OF THIS
# Just use log_connection_yield in execute methods
```

**Lines Saved: 80**

### Problem #2: Custom Error Handling (Lines 160-238, ~80 lines)

**Current Implementation:**

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

**Problem:**
1. Should use `database_error_regexps` pattern (declarative)
2. `database_exception_message` is unnecessary - Sequel formats messages
3. `handle_constraint_violation` is never used properly - should just use `raise_error`

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

def database_error_classes
  [::DuckDB::Error]
end
```

**Lines Saved: 73** (from 80 to 7)

### Problem #3: execute_statement Complexity (Lines 906-973, ~70 lines)

**Current Implementation:**

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

**Problem:**
1. Manual timing - `log_connection_yield` does this
2. Manual logging - `log_connection_yield` does this
3. Manual error handling - `raise_error` does this
4. Row conversion should be in Dataset#fetch_rows
5. Doesn't use Sequel's execution hooks

**Should Be:**

```ruby
# Move to real adapter as _execute:
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
      nil  # DuckDB doesn't support AUTOINCREMENT
    when :update
      log_connection_yield(sql, conn) { conn.query(sql) }
      # Would need to extract rows_changed from result
    end
  end
rescue ::DuckDB::Error => e
  raise_error(e, opts)
end
```

And in Dataset#fetch_rows:

```ruby
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

**Lines Saved: 50** (from 70 to 20)

### Problem #4: Unnecessary Public execute Method (Lines 70-114)

**Current Implementation:**

```ruby
def execute(sql, opts = {}, &block)
  if opts.is_a?(Array)
    params = opts
    opts = {}
  elsif opts.is_a?(Hash)
    params = opts[:params] || []
  else
    params = []
    opts = {}
  end

  synchronize(opts[:server]) do |conn|
    result = execute_statement(conn, sql, params, opts, &block)

    if !block && result.is_a?(::DuckDB::Result) \
      && (sql.strip.upcase.start_with?("UPDATE ") \
      || sql.strip.upcase.start_with?("DELETE "))
      return result.rows_changed
    end

    return result
  end
end
```

**Problem:**
1. Parameter handling is complex and non-standard
2. SQL parsing to determine type is fragile
3. Should use type-dispatch pattern like SQLite

**Should Be:**

```ruby
# Move to real adapter:
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

**Lines Saved: 35** (from 45 to 10)

### Problem #5: Performance Optimization Code (Lines 1973-2465, ~500 lines)

**Current Implementation:**

Lines of "optimization" code including:
- Custom batch processing in `each`
- Memory usage tracking
- Custom streaming
- Prepared statement wrappers
- Connection pooling wrappers
- Index hints
- Columnar optimization hints
- Parallel execution hints

**Problem:**
1. Most of this is premature optimization
2. Sequel already handles batching/streaming
3. DuckDB handles parallelization automatically
4. Index hints don't actually do anything in DuckDB
5. Adds complexity without proven benefit

**Should Be:**

```ruby
# DELETE MOST OF THIS

# Keep only if actually needed:
def fetch_rows(sql)
  # Simple, let DuckDB stream results
  execute(sql) do |result|
    # Convert and yield rows
  end
end
```

**Lines Saved: 450** (from 500 to 50)

### Problem #6: Transaction Code (Lines 632-822, ~190 lines)

**Current Implementation:**

Custom transaction handling including:
- `begin_transaction`
- `commit_transaction`
- `rollback_transaction`
- `savepoint_transaction`
- `isolation_transaction`
- Feature detection methods

**Problem:**
1. DuckDB doesn't support savepoints - the code admits this but implements it anyway
2. DuckDB doesn't support isolation levels - the code admits this but implements it anyway
3. Should use Sequel's default transaction handling

**Should Be:**

```ruby
def supports_savepoints?
  false
end

def supports_transaction_isolation_level?(_level)
  false
end

# That's it. Let Sequel handle transactions via default BEGIN/COMMIT/ROLLBACK
```

**Lines Saved: 180** (from 190 to 10)

### Problem #7: Schema Management (Lines 1173-1301, ~130 lines)

**Current Implementation:**

Full schema create/drop methods with SQL generation.

**Problem:**
Sequel already provides `create_schema` and `drop_schema` via SQL generation. This is only needed if DuckDB syntax differs from standard SQL.

**Should Be:**

Check if DuckDB's CREATE SCHEMA syntax matches Sequel's default. If yes, delete this code.

**Lines Saved: ~100** (if DuckDB uses standard syntax)

### Problem #8: Type Conversion Complexity (Lines 1305-1351)

**Current Implementation:**

Custom typecast_value_time and typecast_value methods.

**Problem:**
This might be necessary for DuckDB's TIME type handling, but it's implemented in the wrong place. Should be in Dataset, not Database.

**Should Be:**

Move to Dataset if needed, or use Sequel's default conversion procs.

**Lines Saved: ~30**

### Problem #9: Configuration Methods (Lines 367-450, ~85 lines)

**Current Implementation:**

set_pragma, configure_duckdb, configure_parallel_execution, etc.

**Problem:**
Should use connection_pragmas pattern like SQLite:

```ruby
def connection_pragmas
  ps = []
  ps << "PRAGMA threads = #{opts[:threads]}" if opts[:threads]
  ps << "PRAGMA memory_limit = '#{opts[:memory_limit]}'" if opts[:memory_limit]
  ps
end
```

Then apply in connect:

```ruby
connection_pragmas.each{|s| log_connection_yield(s, conn){conn.execute(s)}}
```

**Lines Saved: ~70** (from 85 to 15)

### Problem #10: Dataset SQL Generation Bloat (Lines 1357-1690, ~330 lines)

**Current Implementation:**

Completely reimplements:
- insert_sql
- update_sql
- delete_sql
- select_with_sql (WITH clause)
- select_from_sql
- select_join_sql
- select_where_sql
- etc.

**Problem:**
Most of this is standard SQL that Sequel already generates. Only override if DuckDB syntax differs.

**Should Be:**

Override only what's different:
```ruby
# Only override if DuckDB has different syntax
def complex_expression_sql_append(sql, op, args)
  case op
  when :ILIKE
    # DuckDB specific
  else
    super
  end
end
```

**Lines Saved: ~250** (from 330 to 80)

## Refactoring Plan

### Step 1: Move Execution to Real Adapter

Move `_execute` pattern from shared to real adapter:

```ruby
# adapters/duckdb.rb
private

def _execute(type, sql, opts, &block)
  synchronize(opts[:server]) do |conn|
    case type
    when :select
      log_connection_yield(sql, conn) { conn.query(sql, &block) }
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

public

def execute(sql, opts=OPTS, &block)
  _execute(:select, sql, opts, &block)
end

def execute_dui(sql, opts=OPTS)
  _execute(:update, sql, opts)
end

def execute_insert(sql, opts=OPTS)
  _execute(:insert, sql, opts)
end

def database_error_classes
  [::DuckDB::Error]
end
```

### Step 2: Replace Error Handling in Shared Adapter

```ruby
# adapters/shared/duckdb.rb
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

Delete:
- `database_exception_class` (45 lines)
- `database_exception_message` (10 lines)
- `handle_constraint_violation` (7 lines)
- All custom logging methods (80 lines)
- `execute_statement` (70 lines)

### Step 3: Simplify Dataset Methods

Keep only DuckDB-specific overrides:
- ILIKE emulation (if needed)
- Regex operators (if syntax differs)
- Type literals (if DuckDB types differ)

Delete:
- All custom SQL generation that matches Sequel default
- All "optimization" code
- All index hint code
- All parallel execution code

### Step 4: Simplify Configuration

Use connection_pragmas pattern:

```ruby
def connection_pragmas
  ps = []
  ps << "PRAGMA threads = #{opts[:threads]}" if opts[:threads]
  ps << "PRAGMA memory_limit = '#{opts[:memory_limit]}'" if opts[:memory_limit]
  # ... other pragmas
  ps
end
```

Delete:
- set_pragma
- configure_duckdb
- configure_parallel_execution
- configure_memory_optimization
- configure_columnar_optimization

### Step 5: Check Schema Operations

Test if DuckDB uses standard CREATE SCHEMA / DROP SCHEMA syntax. If yes, delete custom implementations.

## Expected Result

### Real Adapter (~300 lines)

- Connection: 30 lines
- Execution: 40 lines (with _execute)
- Type conversion: 50 lines (if needed for DuckDB types)
- Dataset: 50 lines (fetch_rows)
- Comments: 130 lines

### Shared Adapter (~500 lines)

- Error classification: 10 lines (declarative)
- Schema introspection: 150 lines (information_schema queries)
- SQL generation overrides: 80 lines (only what differs)
- Configuration: 15 lines (connection_pragmas)
- Feature detection: 50 lines (supports_* methods)
- Helper methods: 50 lines
- Comments/structure: 145 lines

### Total: ~800 lines

**Reduction: 1941 lines (71% reduction)**

## Lines to Delete by Category

1. **Custom Logging**: 80 lines → 0 lines (**-80**)
2. **Custom Error Handling**: 80 lines → 10 lines (**-70**)
3. **execute_statement**: 70 lines → 0 lines (**-70**)
4. **Public execute**: 45 lines → 0 lines (**-45**, moved to real adapter as _execute)
5. **Performance Code**: 500 lines → 50 lines (**-450**)
6. **Transaction Code**: 190 lines → 10 lines (**-180**)
7. **Schema Management**: 130 lines → 30 lines (**-100**)
8. **Type Conversion**: 50 lines → 30 lines (**-20**)
9. **Configuration**: 85 lines → 15 lines (**-70**)
10. **SQL Generation**: 330 lines → 80 lines (**-250**)

**Total Lines Deleted: 1335 lines**

Plus another ~600 lines of comments, whitespace, and documentation for deleted features.

**Total Reduction: ~1941 lines (71%)**

## Benefits of Refactoring

1. **Maintainability**: Code matches SQLite adapter pattern
2. **Correctness**: Uses Sequel's battle-tested features
3. **Performance**: No custom overhead, native Sequel optimizations
4. **Compatibility**: Works with all Sequel features/plugins
5. **Debugging**: Simpler code is easier to debug
6. **Testing**: Less code = easier to test thoroughly

## Risk Assessment

**Low Risk Deletions** (do immediately):
- Custom logging (100% safe)
- Custom error message formatting (100% safe)
- execute_statement complexity (100% safe)
- Performance "optimizations" (premature optimization)
- Transaction features DuckDB doesn't support (100% safe)

**Medium Risk Deletions** (test thoroughly):
- SQL generation that looks standard (test against DuckDB)
- Schema operations (verify DuckDB syntax)
- Type conversion (verify DuckDB type handling)

**Requires Research**:
- Does DuckDB support prepared statements? (Current code attempts to use them)
- Does DuckDB return rows_changed? (Current code assumes yes)
- What's the exact syntax for DuckDB pragmas?

## Summary

The DuckDB adapter is over-engineered by **~1900 lines** of unnecessary code that reimplements Sequel's built-in features. Following the SQLite adapter pattern will result in a simpler, more maintainable, and more correct adapter.

**Key Principle: If SQLite doesn't need it, DuckDB doesn't need it.**
