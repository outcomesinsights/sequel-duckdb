# SQLite Adapter - Engineering Insights

## Overview

The SQLite adapter is a masterclass in simplicity and proper use of Sequel's built-in features. Written by Jeremy Evans (Sequel's author), it demonstrates the "right way" to implement a Sequel adapter.

**File Structure:**
- `adapters/sqlite.rb` (462 lines) - Real adapter
- `adapters/shared/sqlite.rb` (1074 lines) - Shared adapter
- **Total: ~1536 lines**

## Real Adapter Analysis (`adapters/sqlite.rb`)

### Type Conversion System (Lines 1-79)

SQLite stores everything as strings/blobs, so type conversion is critical.

**Pattern:** Callable objects in a hash
```ruby
boolean = Object.new
def boolean.call(s)
  s = s.downcase if s.is_a?(String)
  !FALSE_VALUES.include?(s)
end

SQLITE_TYPES = {
  'bool' => boolean,
  'boolean' => boolean,
  'integer' => integer,
  'date' => date,
  # ...
}.freeze
```

**Why:** Fast lookup, easy to extend, frozen for thread-safety

### Database Class (Lines 85-354)

**Connection (Lines 128-154):**
```ruby
def connect(server)
  opts = server_opts(server)
  opts[:database] = ':memory:' if blank_object?(opts[:database])

  db = ::SQLite3::Database.new(opts[:database].to_s, sqlite3_opts)
  db.busy_timeout(typecast_value_integer(opts.fetch(:timeout, 5000)))
  db.extended_result_codes = true if USE_EXTENDED_RESULT_CODES

  # Apply connection pragmas using log_connection_yield
  connection_pragmas.each{|s| log_connection_yield(s, db){db.execute_batch(s)}}

  db
end
```

**Key Points:**
- Returns raw driver connection object
- Configuration via pragmas, not API calls
- Uses `log_connection_yield` even for setup
- No error handling here - let `raise_error` catch it

**Execution (Lines 169-270):**

The entire execution system is **ONE method**:

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

**What It Does:**
1. `synchronize` - Get connection from pool
2. Check for prepared statement (Symbol sql)
3. Extract arguments
4. Dispatch by type
5. Use `log_connection_yield` for ALL execution
6. Return type-appropriate value
7. Rescue and use `raise_error` for conversion

**What It Doesn't Do:**
- No manual timing (log_connection_yield does it)
- No manual logging (log_connection_yield does it)
- No manual error classification (raise_error does it)
- No try/catch/ensure complexity
- No connection management (synchronize does it)

**LOC: 20 lines of actual logic**

### Dataset Class (Lines 356-459)

**Type Conversion During Fetch (Lines 406-428):**
```ruby
def fetch_rows(sql)
  execute(sql) do |result|
    cps = db.conversion_procs
    type_procs = result.types.map{|t| cps[base_type_name(t)]}
    j = -1
    cols = result.columns.map{|c| [output_identifier(c), type_procs[(j+=1)]]}
    self.columns = cols.map(&:first)
    max = cols.length

    result.each do |values|
      row = {}
      i = -1
      while (i += 1) < max
        name, type_proc = cols[i]
        v = values[i]
        v = type_proc.call(v) if type_proc && v
        row[name] = v
      end
      yield row
    end
  end
end
```

**Pattern:**
1. Build type conversion proc array from column types
2. Build column name array with output identifiers
3. Set `self.columns` for Sequel
4. For each row: convert values and yield hash

**Why:** Efficient - builds conversion procs once, applies many times

## Shared Adapter Analysis (`adapters/shared/sqlite.rb`)

### DatabaseMethods (Lines 24-580)

**Configuration (Lines 24-65):**
- Transaction modes (deferred/immediate/exclusive)
- Integer booleans setting
- UTC timestamp setting
- All via accessors, no complex logic

**Schema Introspection (Lines 69-187):**
- Uses SQLite PRAGMA statements
- `foreign_key_list` - PRAGMA foreign_key_list
- `indexes` - PRAGMA index_list + index_info
- `tables` - Query sqlite_master
- Clean, simple queries

**Error Classification (Lines 372-404):**

**The Entire System:**

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

**That's it. 33 lines total. Handles all error cases.**

**Pattern:**
1. Try error codes first (most reliable)
2. Fall back to regex (for older sqlite3 gem versions)
3. Always call `super` for unmatched cases

**ALTER TABLE Support (Lines 237-294):**

SQLite has limited ALTER TABLE support, so the adapter implements `duplicate_table` pattern:
1. Rename table to backup
2. Create new table with changes
3. Copy data from backup to new table
4. Drop backup table

This is complex but isolated to one method. Good separation of concerns.

### DatasetMethods (Lines 582-1072)

**SQL Generation Customizations:**
- CURRENT_TIMESTAMP in UTC → convert to localtime
- LIKE operator → no ESCAPE clause needed
- Exponentiation → emulate with multiplication
- Extract → use strftime
- Multi-row VALUES → support since 3.7.11

**All handled via:**
- Override specific `_sql` methods
- Override `complex_expression_sql_append`
- Override `literal_*` methods

**No wholesale SQL generation rewrite.**

**INSERT conflict resolution (Lines 771-792):**

SQLite's unique INSERT OR IGNORE/REPLACE syntax:

```ruby
def insert_conflict(opts = :ignore)
  case opts
  when Symbol, String
    unless INSERT_CONFLICT_RESOLUTIONS.include?(opts.to_s.upcase)
      raise Error, "Invalid value..."
    end
    clone(:insert_conflict => opts)
  when Hash
    clone(:insert_on_conflict => opts)
  else
    raise Error, "Invalid value..."
  end
end
```

Then in SQL generation:

```ruby
def insert_conflict_sql(sql)
  if resolution = @opts[:insert_conflict]
    sql << " OR " << resolution.to_s.upcase
  end
end
```

**Clean separation:** Configuration via dataset options, generation via _sql methods.

## Key Architectural Patterns

### 1. Minimal Real Adapter

The real adapter contains ONLY:
- Type conversions (SQLite-specific)
- Connection management (sqlite3 gem API)
- Execution dispatch (sqlite3 gem methods)

No schema operations, no SQL generation.

### 2. Declarative Error Handling

Error classification is DATA:
```ruby
DATABASE_ERROR_REGEXPS = {
  /pattern/ => ExceptionClass,
  # ...
}.freeze
```

Not procedural code with 45-line case statements.

### 3. Single Execution Path

One `_execute` method handles all SQL types. Benefits:
- Single place for logging
- Single place for connection management
- Single place for error handling
- Easy to understand and debug

### 4. Trust Sequel

Don't reimplement:
- Logging → `log_connection_yield`
- Timing → `log_connection_yield`
- Connection pooling → `synchronize`
- Error conversion → `raise_error`

These are battle-tested and optimized.

### 5. Override Only What's Different

Dataset SQL generation:
- Inherit 90% from Sequel::Dataset
- Override only SQLite-specific syntax
- Use specific methods: `literal_date`, `complex_expression_sql_append`
- Don't rewrite `select_sql` wholesale

## Complexity Budget

**Real Adapter Complexity:**
- Connection: 15 lines
- Execution: 20 lines
- Dataset: 30 lines
- Type conversion: 100 lines (necessary for SQLite)

**Shared Adapter Complexity:**
- Error handling: 33 lines (declarative)
- Schema introspection: 150 lines (PRAGMA queries)
- SQL generation overrides: ~200 lines (only what differs)
- ALTER TABLE emulation: 250 lines (necessary for SQLite limitations)

**Total Unique Code:** ~800 lines
**Boilerplate/Comments:** ~700 lines

## Lessons for DuckDB Adapter

### What to Copy

1. **Execution pattern:**
   ```ruby
   def _execute(type, sql, opts, &block)
     synchronize(opts[:server]) do |conn|
       case type
       when :select
         log_connection_yield(sql, conn) { conn.query(sql, &block) }
       # ...
       end
     end
   rescue DuckDB::Error => e
     raise_error(e)
   end
   ```

2. **Error classification:**
   ```ruby
   DATABASE_ERROR_REGEXPS = {
     /unique.*constraint/i => UniqueConstraintViolation,
     # ...
   }.freeze

   def database_specific_error_class(exception, opts)
     case error_code(exception)
     when code then ExceptionClass
     else super
     end
   end
   ```

3. **Connection setup:**
   ```ruby
   def connect(server)
     opts = server_opts(server)
     db = DuckDB::Database.open(opts[:database])
     # Configure via SQL, not API:
     config_sqls.each{|s| log_connection_yield(s, db){db.execute(s)}}
     db
   end
   ```

### What NOT to Copy

1. **Duplicate table logic** - DuckDB likely supports proper ALTER TABLE
2. **Type conversion complexity** - DuckDB has better type support than SQLite
3. **Multi-row VALUES emulation** - DuckDB supports it natively

## File Size Breakdown

```
Real Adapter (462 lines):
  - Type conversion: 100 lines
  - Connection: 50 lines
  - Execution: 100 lines
  - Dataset: 100 lines
  - Comments/whitespace: 112 lines

Shared Adapter (1074 lines):
  - DatabaseMethods: 556 lines
    - Configuration: 50
    - Schema introspection: 150
    - ALTER TABLE: 250
    - Error handling: 33
    - Other: 73
  - DatasetMethods: 490 lines
    - SQL generation overrides: 300
    - Feature detection: 50
    - Helpers: 140
  - Comments/whitespace: 28 lines
```

## Summary

**SQLite adapter is simple because it:**
1. Uses `log_connection_yield` for all execution
2. Uses `raise_error` for all error handling
3. Uses declarative error classification
4. Overrides only database-specific SQL
5. Trusts Sequel's built-in features

**Total actual logic: ~800 lines**
**No wheel reinvention: 0 lines**

This is the gold standard for Sequel adapters.
