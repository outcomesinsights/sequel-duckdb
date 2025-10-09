# Sequel Core Architecture - Engineering Insights

## Overview

Sequel is a sophisticated Ruby ORM with a well-designed plugin architecture. Understanding its core mechanisms is essential for implementing adapters that leverage built-in functionality rather than reinventing the wheel.

## Key Design Principles

1. **Separation of Concerns**: Database vs Dataset responsibilities
2. **Extension Points**: Minimal required overrides, extensive optional hooks
3. **Battle-tested Utilities**: Logging, error handling, connection pooling built-in
4. **Declarative Configuration**: Prefer data structures over procedural code

## Core Components

### 1. Logging System (`database/logging.rb`)

**Primary Method: `log_connection_yield(sql, conn, args=nil)`**

This is the ONE method adapters should use for all SQL execution logging. It handles:

- Timing (via `Sequel.start_timer` and `Sequel.elapsed_seconds_since`)
- Connection info (if `log_connection_info` is enabled)
- Arguments logging
- Exception logging (via `log_exception`)
- Duration logging (via `log_duration`)
- Slow query warnings (via `log_warn_duration` threshold)

**Pattern:**
```ruby
log_connection_yield(sql, conn, log_args) do
  conn.execute(sql, args)
end
```

**Why Use It:**
- Consistent logging format across all adapters
- Automatic timing without manual Time.now calls
- Built-in slow query detection
- Respects user's log level configuration
- Zero code if logging is disabled

**Configuration:**
- `loggers` - Array of logger objects (empty = no logging)
- `sql_log_level` - :info (default) or :debug
- `log_warn_duration` - Numeric threshold for warnings
- `log_connection_info` - Boolean for connection ID in logs

**Helper Methods (rarely needed):**
- `log_info(message, args=nil)` - Log at info level
- `log_exception(exception, message)` - Log exceptions
- `log_duration(duration, message)` - Log with timing
- `log_connection_execute(conn, sql)` - For transaction commands

### 2. Error Handling System (`database/misc.rb`, `exceptions.rb`)

**Exception Hierarchy:**
```
Sequel::Error (base)
├── Sequel::DatabaseError (generic database errors)
│   ├── Sequel::DatabaseConnectionError
│   ├── Sequel::DatabaseDisconnectError
│   ├── Sequel::ConstraintViolation
│   │   ├── Sequel::CheckConstraintViolation
│   │   ├── Sequel::ForeignKeyConstraintViolation
│   │   ├── Sequel::NotNullConstraintViolation
│   │   └── Sequel::UniqueConstraintViolation
│   ├── Sequel::SerializationFailure
│   └── Sequel::DatabaseLockTimeout
```

**Primary Method: `raise_error(exception, opts=OPTS)`**

This converts driver exceptions to Sequel exceptions. Never raise Sequel exceptions directly.

**Pattern:**
```ruby
def _execute(type, sql, opts, &block)
  synchronize(opts[:server]) do |conn|
    log_connection_yield(sql, conn) do
      conn.execute(sql)
    end
  end
rescue DriverException => e
  raise_error(e, opts)
end
```

**Error Classification Methods (override these):**

**1. `database_error_regexps` - Declarative Regex Matching**

Return a Hash/Enumerable of `[regexp, exception_class]` pairs:

```ruby
DATABASE_ERROR_REGEXPS = {
  /unique.*constraint/i => UniqueConstraintViolation,
  /foreign.*key/i => ForeignKeyConstraintViolation,
  /not.*null/i => NotNullConstraintViolation,
  /check.*constraint/i => CheckConstraintViolation,
  /constraint/i => ConstraintViolation, # Generic, put last
}.freeze

def database_error_regexps
  DATABASE_ERROR_REGEXPS
end
```

**2. `database_specific_error_class(exception, opts)` - Error Code Matching**

For databases with numeric error codes:

```ruby
def database_specific_error_class(exception, opts)
  case error_code(exception)
  when 1299
    NotNullConstraintViolation
  when 1555, 2067, 2579
    UniqueConstraintViolation
  when 787
    ForeignKeyConstraintViolation
  else
    super  # Falls back to regex matching
  end
end
```

**3. `database_exception_sqlstate(exception, opts)` - SQL State Codes**

For databases supporting ANSI SQL state codes:

```ruby
def database_exception_sqlstate(exception, opts)
  exception.sqlstate if exception.respond_to?(:sqlstate)
end
```

Standard SQL State mappings are built-in:
- '23502' → NotNullConstraintViolation
- '23503', '23506', '23504' → ForeignKeyConstraintViolation
- '23505' → UniqueConstraintViolation
- '23513', '23514' → CheckConstraintViolation
- '40001' → SerializationFailure

**4. `disconnect_error?(exception, opts)` - Connection Failures**

Identifies errors that should trigger connection removal from pool:

```ruby
def disconnect_error?(exception, opts)
  opts[:disconnect] || # Explicit flag
    !conn.ping ||       # Connection test failed
    (exception.message =~ /connection.*closed/i)
end
```

**Best Practices:**
- Use error codes when available (most reliable)
- Fall back to SQLState codes
- Use regex matching as last resort
- Put specific patterns before generic ones
- Always call `super` if no match found

### 3. Connection Management (`database/connecting.rb`)

**Primary Method: `synchronize(server=nil, &block)`**

Acquires connection from pool, yields it, returns it automatically:

```ruby
synchronize(opts[:server]) do |conn|
  # Connection is automatically managed
  conn.execute(sql)
end
# Connection returned to pool here
```

**Never** access `@pool` directly. Always use `synchronize`.

**Methods to Override:**

**1. `connect(server)` - REQUIRED**
```ruby
def connect(server)
  opts = server_opts(server)
  # Return driver connection object
  DriverLib::Database.open(opts[:database])
end
```

**2. `disconnect_connection(conn)` - OPTIONAL**
```ruby
def disconnect_connection(conn)
  conn.close
end
# Default is conn.close, override if different
```

**3. `valid_connection?(conn)` - OPTIONAL**
```ruby
def valid_connection?(conn)
  conn.execute("SELECT 1")
  true
rescue
  false
end
# Default executes valid_connection_sql
```

**Lifecycle Hooks:**
- `new_connection(server)` - Wraps `connect`, adds initialization
- `:after_connect` proc - User-configurable hook
- `:connect_sqls` - Array of SQL to execute on new connections

### 4. Execution Pattern

**Standard `_execute` Pattern (SQLite example):**

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
    # Handle prepared statements if sql is Symbol
    return execute_prepared_statement(conn, type, sql, opts, &block) if sql.is_a?(Symbol)

    # Extract arguments
    log_args = opts[:arguments]
    args = {}
    opts.fetch(:arguments, OPTS).each{|k, v| args[k] = prepared_statement_argument(v)}

    # Execute based on type
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
rescue DriverException => e
  raise_error(e, opts)
end

def database_error_classes
  [DriverException]
end
```

**Key Points:**
- Use `synchronize` for connection pooling
- Use `log_connection_yield` for logging/timing
- Use `raise_error` for exception conversion
- Handle prepared statements (Symbol sql) if supported
- Return appropriate values (row count, insert ID, etc.)

## Adapter Responsibilities

### Database Class

**MUST Override:**
- `connect(server)` - Create and return connection
- `dataset_class_default` - Return Dataset subclass

**SHOULD Override:**
- `database_type` - Return :postgres, :mysql, :sqlite, :duckdb, etc.
- `database_error_classes` - Array of driver exception classes
- `database_error_regexps` - Hash of error patterns
- `database_specific_error_class(exception, opts)` - Error code mapping

**MAY Override:**
- `disconnect_connection(conn)` - Close connection (default: conn.close)
- `valid_connection?(conn)` - Test if alive (default: SELECT NULL)
- `begin_new_transaction(conn, opts)` - Custom BEGIN syntax
- `commit_transaction(conn, opts)` - Custom COMMIT syntax
- `rollback_transaction(conn, opts)` - Custom ROLLBACK syntax
- Feature detection: `supports_savepoints?`, `supports_returning?`, etc.

### Dataset Class

**MUST Override:**
- `fetch_rows(sql, &block)` - Execute SQL, yield hash rows

**Pattern:**
```ruby
def fetch_rows(sql)
  execute(sql) do |result|
    # Set self.columns with column names
    cols = result.columns.map{|c| output_identifier(c)}
    self.columns = cols

    # Yield hash rows
    result.each do |row_array|
      row_hash = {}
      cols.each_with_index{|col, i| row_hash[col] = row_array[i]}
      yield row_hash
    end
  end
end
```

**SHOULD Override:**
- `literal_*` methods - For database-specific literal formatting
- `select_sql`, `insert_sql`, `update_sql`, `delete_sql` - For custom SQL syntax

## Real vs Shared Adapters

### Real Adapter (`adapters/database_name.rb`)

**Purpose:** Driver-specific code that varies by Ruby driver

**Contents:**
- Driver gem require
- Type conversion procs (if needed)
- Database class with connection/execution
- Dataset class with fetch_rows
- Driver-specific workarounds

**Example:** SQLite has three real adapters:
- `sqlite.rb` - For sqlite3 gem
- `jdbc/sqlite.rb` - For JDBC on JRuby
- `tinytds.rb` - Different driver

### Shared Adapter (`adapters/shared/database_name.rb`)

**Purpose:** Database-specific code that applies to all drivers

**Contents:**
- DatabaseMethods module
- DatasetMethods module
- Schema operations
- SQL generation
- Feature detection
- Error classification

**Pattern:**
```ruby
module Sequel
  module DatabaseName
    Sequel::Database.set_shared_adapter_scheme(:database_name, self)

    module DatabaseMethods
      # Shared database features
    end

    module DatasetMethods
      # Shared SQL generation
    end
  end
end
```

## Anti-Patterns to Avoid

1. **Don't Reinvent Logging**
   - ❌ `log_info("SQL: #{sql}")`
   - ✅ `log_connection_yield(sql, conn) { execute }`

2. **Don't Manually Time Operations**
   - ❌ `start = Time.now; execute; Time.now - start`
   - ✅ `log_connection_yield` handles timing

3. **Don't Build Error Messages**
   - ❌ `raise DatabaseError, "Error: #{e.message} SQL: #{sql}"`
   - ✅ `raise_error(e, opts)` # Sequel formats it

4. **Don't Manually Classify Errors**
   - ❌ 45-line case statement in execute
   - ✅ `database_error_regexps` hash or `database_specific_error_class`

5. **Don't Access Pool Directly**
   - ❌ `@pool.hold { |conn| ... }`
   - ✅ `synchronize { |conn| ... }`

6. **Don't Put Driver Code in Shared Adapter**
   - Real adapter: Connection, execution, driver specifics
   - Shared adapter: SQL generation, schema operations, features

## Performance Considerations

Sequel's built-in mechanisms are highly optimized:

1. **Connection Pooling**: Thread-safe, tested, configurable
2. **Logging**: Skip-checks minimize overhead when disabled
3. **Error Handling**: Exception class checking is fast
4. **SQL Generation**: Cached where possible

Don't optimize prematurely. Use built-in features first, profile later.

## Testing Support

Sequel provides `Sequel.mock` for testing adapters without database:

```ruby
db = Sequel.mock(host: :duckdb)
```

Adapters should implement `mock_adapter_setup` in shared module:

```ruby
def self.mock_adapter_setup(db)
  db.instance_exec do
    def schema_parse_table(*)
      []
    end
    singleton_class.send(:private, :schema_parse_table)
  end
end
```

## Summary

**Use Sequel's Features:**
- `log_connection_yield` for all execution
- `raise_error` for all exception handling
- `synchronize` for all connection access
- `database_error_regexps` for error classification

**Implement Minimally:**
- `connect` and `dataset_class_default` (required)
- `_execute` pattern (20 lines max)
- Error classification (declarative, < 50 lines)
- Schema operations (database-specific)

**Result:** Simple, maintainable adapters that work like all other Sequel adapters.
