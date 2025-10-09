# Schema Management Implementation Plan

## Overview

Implement DuckDB schema management functionality in the sequel-duckdb adapter to support creating, dropping, and managing schemas. This plan addresses schema operations that are natively supported by DuckDB.

## Background Research

### DuckDB Schema Support

DuckDB supports SQL schema management with the following capabilities:

1. **CREATE SCHEMA**
   - Basic: `CREATE SCHEMA s1;`
   - Conditional: `CREATE SCHEMA IF NOT EXISTS s2;`
   - Replace: `CREATE OR REPLACE SCHEMA s2;`
   - Default schema is `main`

2. **DROP SCHEMA**
   - Basic: `DROP SCHEMA sch;`
   - Conditional: `DROP SCHEMA IF EXISTS sch;`
   - With cascade: `DROP SCHEMA sch CASCADE;`
   - Dependency tracking for tables, views, functions, indexes, sequences, types, macros
   - Note: Dependencies are NOT tracked for views (limitation)

3. **Schema Usage**
   - Tables can be created in schemas: `CREATE TABLE s1.t (id INTEGER);`
   - Cross-schema queries are supported
   - Schemas provide logical namespaces for organizing database objects

### DuckDB Database Support

**Important Finding**: DuckDB does NOT support traditional `CREATE DATABASE` or `DROP DATABASE` commands.

Instead, DuckDB uses:
- **ATTACH DATABASE**: Attach existing database files to current session
  - `ATTACH 'file.db' AS alias;`
  - `ATTACH 'file.db' (READ_ONLY);`
  - Not persisted between sessions
- **DETACH DATABASE**: Detach database files from current session
- Database creation happens implicitly when opening a connection to a file path

### Sequel Schema Support

Sequel provides schema management methods (primarily in PostgreSQL adapter):

1. **create_schema(name, opts={})**
   - Creates a schema
   - Options: `:if_not_exists`, `:owner` (PostgreSQL-specific)

2. **create_schema_sql(name, opts={})**
   - Generates CREATE SCHEMA SQL

3. **drop_schema(name, opts={})**
   - Drops a schema
   - Calls `remove_all_cached_schemas` to clear caches
   - Options: `:if_exists`, `:cascade`

4. **drop_schema_sql(name, opts={})**
   - Generates DROP SCHEMA SQL

5. **rename_schema(name, new_name)** (PostgreSQL-specific)
   - Renames a schema

6. **remove_all_cached_schemas**
   - Clears cached schema information
   - Resets `@primary_keys`, `@primary_key_sequences`, `@schemas`

## Implementation Plan

### Phase 1: Core Schema Operations

Implement basic schema creation and deletion in `DatabaseMethods` module:

#### 1.1 `create_schema(name, opts={})`
```ruby
def create_schema(name, opts=OPTS)
  self << create_schema_sql(name, opts)
end
```

**Supported options:**
- `:if_not_exists` - Add IF NOT EXISTS clause

**Not supported (PostgreSQL-specific):**
- `:owner` - DuckDB doesn't support AUTHORIZATION/OWNER syntax

#### 1.2 `create_schema_sql(name, opts={})`
```ruby
def create_schema_sql(name, opts=OPTS)
  sql = "CREATE"
  sql += " OR REPLACE" if opts[:or_replace]
  sql += " SCHEMA"
  sql += " IF NOT EXISTS" if opts[:if_not_exists]
  sql += " #{quote_identifier(name)}"
  sql
end
```

**DuckDB-specific considerations:**
- Support both `IF NOT EXISTS` and `OR REPLACE`
- Cannot use both simultaneously (mutually exclusive)

#### 1.3 `drop_schema(name, opts={})`
```ruby
def drop_schema(name, opts=OPTS)
  self << drop_schema_sql(name, opts)
  remove_all_cached_schemas
end
```

**Supported options:**
- `:if_exists` - Add IF EXISTS clause
- `:cascade` - Add CASCADE clause to drop dependent objects

#### 1.4 `drop_schema_sql(name, opts={})`
```ruby
def drop_schema_sql(name, opts=OPTS)
  sql = "DROP SCHEMA"
  sql += " IF EXISTS" if opts[:if_exists]
  sql += " #{quote_identifier(name)}"
  sql += " CASCADE" if opts[:cascade]
  sql
end
```

#### 1.5 `remove_all_cached_schemas`
```ruby
def remove_all_cached_schemas
  @schema_cache = {}
  @schemas = {}
  @primary_keys = {}
  @primary_key_sequences = {}
end
```

**Important**: All cache variables MUST be initialized as empty hashes (`{}`), not `nil`. Sequel's internal `remove_cached_schema` method expects `@schemas` to be a Hash and will call `.delete()` on it. Setting it to `nil` will cause a `NoMethodError` when `create_view` or other methods call `remove_cached_schema` internally.

**Note**: DuckDB adapter uses `@schema_cache` for caching schema information (see line 505-510 in shared/duckdb.rb). Need to clear this when schemas are modified.

### Phase 2: Schema Introspection

Implement methods to list and inspect schemas:

#### 2.1 `schemas(opts={})`
```ruby
def schemas(opts=OPTS)
  sql = "SELECT schema_name FROM information_schema.schemata"
  sql += " WHERE catalog_name = ?" if opts[:catalog]

  schemas = []
  execute(sql, opts[:catalog] ? [opts[:catalog]] : []) do |row|
    schemas << row[:schema_name].to_sym
  end

  schemas
end
```

#### 2.2 `schema_exists?(name, opts={})`
```ruby
def schema_exists?(name, opts=OPTS)
  sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name = ? LIMIT 1"

  result = nil
  execute(sql, [name.to_s]) do |_row|
    result = true
  end

  !!result
end
```

### Phase 3: Database File Operations (ATTACH/DETACH)

**Decision**: Do NOT implement `create_database` / `drop_database` because:
1. DuckDB doesn't support these SQL commands
2. Database creation is implicit (happens when connecting to a file path)
3. ATTACH/DETACH are session-specific, not DDL operations
4. These operations are outside the scope of typical Sequel adapter functionality

Instead, document that users should:
- Use connection strings to create/open database files
- Use ATTACH/DETACH via raw SQL if needed: `db.run("ATTACH 'other.db' AS other")`

### Phase 4: Testing

Create comprehensive tests for schema operations:

#### 4.1 Schema Creation Tests
- Test basic CREATE SCHEMA
- Test CREATE SCHEMA IF NOT EXISTS
- Test CREATE OR REPLACE SCHEMA
- Test error handling for duplicate schemas (without IF NOT EXISTS)

#### 4.2 Schema Deletion Tests
- Test basic DROP SCHEMA (empty schema)
- Test DROP SCHEMA IF EXISTS
- Test DROP SCHEMA CASCADE (with dependent objects)
- Test error handling for non-existent schemas (without IF EXISTS)
- Test dependency error (dropping schema with objects, without CASCADE)

#### 4.3 Schema Introspection Tests
- Test listing all schemas
- Test schema_exists? for existing and non-existing schemas
- Test that default 'main' schema exists

#### 4.4 Schema Usage Tests
- Test creating tables in custom schemas
- Test cross-schema queries
- Test that schema cache is properly cleared after drop

#### 4.5 Edge Cases
- Test schema names that require quoting (with special characters)
- Test schema names that are reserved words
- Test mutually exclusive options (:if_not_exists vs :or_replace)

### Phase 5: Documentation

#### 5.1 Update README
Add section on schema management:
```markdown
## Schema Management

sequel-duckdb supports DuckDB's schema functionality:

### Creating Schemas
db.create_schema(:analytics)
db.create_schema(:staging, if_not_exists: true)
db.create_schema(:temp, or_replace: true)

### Dropping Schemas
db.drop_schema(:analytics)
db.drop_schema(:staging, if_exists: true)
db.drop_schema(:temp, cascade: true)  # Drops all objects in schema

### Listing Schemas
db.schemas  # => [:main, :analytics, :staging]

### Using Schemas
db.create_table(Sequel[:analytics][:sales]) do
  primary_key :id
  String :product
  Numeric :amount
end

db[:analytics__sales].insert(product: 'Widget', amount: 99.99)
```

#### 5.2 Document Limitations
- DuckDB does not support schema ownership/authorization
- DuckDB does not support renaming schemas (no ALTER SCHEMA RENAME)
- View dependencies are not tracked by DuckDB

## Implementation Checklist

- [x] Phase 1: Core schema operations
  - [x] Implement `create_schema(name, opts={})`
  - [x] Implement `create_schema_sql(name, opts={})`
  - [x] Implement `drop_schema(name, opts={})`
  - [x] Implement `drop_schema_sql(name, opts={})`
  - [x] Implement `remove_all_cached_schemas`
  - [x] Add validation for mutually exclusive options

- [x] Phase 2: Schema introspection
  - [x] Implement `schemas(opts={})`
  - [x] Implement `schema_exists?(name, opts={})`

- [x] Phase 3: Document ATTACH/DETACH approach
  - [x] Add documentation for database file operations
  - [x] Explain why create_database/drop_database are not implemented

- [x] Phase 4: Testing
  - [x] Write schema creation tests (6 tests)
  - [x] Write schema deletion tests (6 tests)
  - [x] Write schema introspection tests (5 tests)
  - [x] Write schema usage tests (4 tests)
  - [x] Write edge case tests (3 tests)
  - [x] Write cache clearing tests (3 tests)
  - [x] Add regression test for `remove_cached_schema` compatibility

- [x] Phase 5: Documentation
  - [x] Update README with schema management section
  - [x] Document limitations
  - [x] Add examples for common use cases
  - [x] Document ATTACH/DETACH for multiple database files

## Implementation Summary

### Completed Features

All planned features have been implemented and tested:

1. **Core Schema Operations** (`lib/sequel/adapters/shared/duckdb.rb`)
   - `create_schema(name, opts={})` - Lines 1191-1193
   - `create_schema_sql(name, opts={})` - Lines 1202-1214
   - `drop_schema(name, opts={})` - Lines 1227-1230
   - `drop_schema_sql(name, opts={})` - Lines 1239-1245
   - `remove_all_cached_schemas` - Lines 1257-1262

2. **Schema Introspection** (`lib/sequel/adapters/shared/duckdb.rb`)
   - `schemas(opts={})` - Lines 1270-1280
   - `schema_exists?(name, opts={})` - Lines 1291-1300

3. **Comprehensive Test Suite** (`test/schema_management_test.rb`)
   - 38 tests covering all functionality
   - All tests passing
   - Regression tests to prevent cache-related bugs

4. **Documentation** (`README.md`)
   - Complete schema management section
   - Usage examples
   - Limitations documented
   - ATTACH/DETACH examples

### Bug Fixes

- **Cache initialization bug**: Fixed `remove_all_cached_schemas` to initialize cache variables as empty hashes instead of `nil`. This prevents `NoMethodError` when Sequel's `remove_cached_schema` is called internally by methods like `create_view`.

### Test Results

- **Schema management tests**: 38 tests, 73 assertions, 0 failures
- **Full test suite**: 539 tests, 2362 assertions, 0 failures

### Files Modified

- `lib/sequel/adapters/shared/duckdb.rb` - Added schema management methods
- `test/schema_management_test.rb` - New comprehensive test file
- `README.md` - Added schema management documentation section

## Related Files

- Implementation: `lib/sequel/adapters/shared/duckdb.rb` (DatabaseMethods module)
- Tests: `spec/sequel/adapters/duckdb_spec.rb` or new `spec/schema_management_spec.rb`
- Documentation: `README.md`

## References

- [DuckDB CREATE SCHEMA](https://duckdb.org/docs/stable/sql/statements/create_schema.html)
- [DuckDB DROP Statement](https://duckdb.org/docs/stable/sql/statements/drop.html)
- [DuckDB ATTACH/DETACH](https://duckdb.org/docs/stable/sql/statements/attach.html)
- [Sequel PostgreSQL Adapter](https://github.com/jeremyevans/sequel/blob/master/lib/sequel/adapters/shared/postgres.rb)
- [Sequel Schema Modification Docs](https://github.com/jeremyevans/sequel/blob/master/doc/schema_modification.rdoc)
