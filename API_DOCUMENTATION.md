# API Documentation - Sequel DuckDB Adapter

This document provides comprehensive API documentation for the Sequel DuckDB adapter, including all public methods, configuration options, and usage patterns.

## Table of Contents

1. [Version Compatibility](#version-compatibility)
2. [Database Class API](#database-class-api)
3. [Dataset Class API](#dataset-class-api)
4. [SQL Generation Patterns](#sql-generation-patterns)
5. [Configuration Options](#configuration-options)
6. [Error Handling](#error-handling)
7. [Data Type Mappings](#data-type-mappings)
8. [Performance Tuning](#performance-tuning)

## Version Compatibility

### Supported Versions

| Component | Minimum Version | Recommended Version | Notes |
|-----------|----------------|-------------------|-------|
| Ruby | 3.1.0 | 3.2.0+ | Required for modern syntax and performance |
| Sequel | 5.0.0 | 5.70.0+ | Core ORM functionality |
| DuckDB | 0.8.0 | 0.9.0+ | Database engine |
| ruby-duckdb | 1.0.0 | 1.0.0+ | Ruby client library |

### Ruby Version Support

- **Ruby 3.1.0+**: Full support with all features
- **Ruby 3.2.0+**: Recommended for best performance
- **Ruby 3.3.0+**: Latest features and optimizations

### Sequel Version Support

- **Sequel 5.0+**: Basic functionality
- **Sequel 5.50+**: Enhanced schema introspection
- **Sequel 5.70+**: Full feature compatibility

### DuckDB Version Support

- **DuckDB 0.8.0+**: Core functionality
- **DuckDB 0.9.0+**: Enhanced JSON and array support
- **DuckDB 0.10.0+**: Latest analytical features

## Database Class API

### Connection Methods

#### `Sequel.connect(connection_string)`

Connect to a DuckDB database using a connection string.

```ruby
# In-memory database
db = Sequel.connect('duckdb::memory:')

# File database
db = Sequel.connect('duckdb:///path/to/database.duckdb')
db = Sequel.connect('duckdb://relative/path/to/database.duckdb')

# With query parameters
db = Sequel.connect('duckdb:///path/to/database.duckdb?readonly=true')
```

**Parameters:**
- `connection_string` (String): DuckDB connection string

**Returns:** `Sequel::DuckDB::Database` instance

**Raises:** `Sequel::DatabaseConnectionError` if connection fails

#### `Sequel.connect(options_hash)`

Connect using a configuration hash.

```ruby
db = Sequel.connect(
  adapter: 'duckdb',
  database: '/path/to/database.duckdb',
  readonly: false,
  config: {
    memory_limit: '4GB',
    threads: 8,
    temp_directory: '/tmp/duckdb'
  }
)
```

**Parameters:**
- `options_hash` (Hash): Configuration options
  - `:adapter` (String): Must be 'duckdb'
  - `:database` (String): Database path or ':memory:'
  - `:readonly` (Boolean): Read-only mode (default: false)
  - `:config` (Hash): DuckDB-specific configuration

**Returns:** `Sequel::DuckDB::Database` instance

### Schema Introspection Methods

#### `#tables(options = {})`

Get list of all tables in the database.

```ruby
db.tables
# => [:users, :products, :orders]

# With schema specification
db.tables(schema: 'main')
# => [:users, :products, :orders]
```

**Parameters:**
- `options` (Hash): Optional parameters
  - `:schema` (String): Schema name (default: 'main')

**Returns:** Array of table names as symbols

#### `#schema(table_name, options = {})`

Get detailed schema information for a table.

```ruby
db.schema(:users)
# => [
#   [:id, {type: :integer, db_type: "INTEGER", primary_key: true, allow_null: false}],
#   [:name, {type: :string, db_type: "VARCHAR", primary_key: false, allow_null: false}],
#   [:email, {type: :string, db_type: "VARCHAR", primary_key: false, allow_null: true}]
# ]
```

**Parameters:**
- `table_name` (Symbol/String): Name of the table
- `options` (Hash): Optional parameters
  - `:schema` (String): Schema name (default: 'main')

**Returns:** Array of `[column_name, column_info]` pairs

**Column Info Hash:**
- `:type` (Symbol): Sequel type (:integer, :string, :boolean, etc.)
- `:db_type` (String): DuckDB native type
- `:primary_key` (Boolean): Whether column is part of primary key
- `:allow_null` (Boolean): Whether column allows NULL values
- `:default` (Object): Default value or nil
- `:max_length` (Integer): Maximum length for string types
- `:precision` (Integer): Precision for numeric types
- `:scale` (Integer): Scale for decimal types

#### `#indexes(table_name, options = {})`

Get index information for a table.

```ruby
db.indexes(:users)
# => {
#   :users_email_index => {
#     columns: [:email],
#     unique: true,
#     primary: false
#   }
# }
```

**Parameters:**
- `table_name` (Symbol/String): Name of the table
- `options` (Hash): Optional parameters

**Returns:** Hash of `index_name => index_info`

#### `#table_exists?(table_name, options = {})`

Check if a table exists.

```ruby
db.table_exists?(:users)  # => true
db.table_exists?(:nonexistent)  # => false
```

**Parameters:**
- `table_name` (Symbol/String): Name of the table
- `options` (Hash): Optional parameters

**Returns:** Boolean

### SQL Execution Methods

#### `#execute(sql, options = {})`

Execute raw SQL statement.

```ruby
# Simple query
result = db.execute("SELECT COUNT(*) FROM users")

# With parameters
result = db.execute("SELECT * FROM users WHERE age > ?", [25])

# With block for result processing
db.execute("SELECT * FROM users") do |row|
  puts row[:name]
end
```

**Parameters:**
- `sql` (String): SQL statement to execute
- `options` (Hash/Array): Parameters or options
  - If Array: Parameters for prepared statement
  - If Hash: Options including `:params` key

**Returns:** Query result or number of affected rows

#### `#execute_insert(sql, options = {})`

Execute INSERT statement.

```ruby
db.execute_insert("INSERT INTO users (name, email) VALUES (?, ?)", ['John', 'john@example.com'])
```

**Parameters:**
- `sql` (String): INSERT SQL statement
- `options` (Hash): Options for execution

**Returns:** Inserted record ID (if available) or nil

#### `#execute_update(sql, options = {})`

Execute UPDATE statement.

```ruby
affected_rows = db.execute_update("UPDATE users SET active = ? WHERE age > ?", [true, 25])
```

**Parameters:**
- `sql` (String): UPDATE SQL statement
- `options` (Hash): Options for execution

**Returns:** Number of affected rows

### Transaction Methods

#### `#transaction(options = {}, &block)`

Execute a block within a database transaction.

```ruby
# Basic transaction
db.transaction do
  db[:users].insert(name: 'Alice', email: 'alice@example.com')
  db[:profiles].insert(user_id: db[:users].max(:id), bio: 'Developer')
end

# With rollback
db.transaction do
  db[:users].insert(name: 'Bob', email: 'bob@example.com')
  raise Sequel::Rollback if some_condition
end

# With savepoint (nested transaction)
db.transaction do
  db[:users].insert(name: 'Charlie', email: 'charlie@example.com')

  db.transaction(savepoint: true) do
    # This can be rolled back independently
    db[:audit_log].insert(action: 'user_created')
  end
end
```

**Parameters:**
- `options` (Hash): Transaction options
  - `:savepoint` (Boolean): Use savepoint for nested transaction
  - `:isolation` (Symbol): Transaction isolation level
  - `:server` (Symbol): Server/connection to use

**Returns:** Result of the block

**Raises:**
- `Sequel::Rollback`: To rollback transaction
- `Sequel::DatabaseError`: On transaction errors

### Connection Management

#### `#disconnect`

Close all database connections.

```ruby
db.disconnect
```

#### `#test_connection`

Test if the database connection is working.

```ruby
db.test_connection  # => true
```

**Returns:** Boolean indicating connection status

## Dataset Class API

### Query Building Methods

#### `#where(conditions)`

Add WHERE clause to query.

```ruby
users = db[:users]

# Hash conditions
users.where(active: true, age: 25)

# Block conditions
users.where { age > 25 }

# String conditions with parameters
users.where("name LIKE ?", 'John%')

# Complex conditions
users.where(Sequel.like(:name, 'John%') & (Sequel[:age] > 25))
```

**Parameters:**
- `conditions`: Various condition formats (Hash, String, Block, Sequel expressions)

**Returns:** New Dataset with WHERE clause added

#### `#select(*columns)`

Specify columns to select.

```ruby
users = db[:users]

# Select specific columns
users.select(:id, :name, :email)

# Select with aliases
users.select(:id, Sequel[:name].as(:full_name))

# Select with functions
users.select(:id, Sequel.function(:upper, :name).as(:name_upper))
```

**Parameters:**
- `columns`: Column names, expressions, or functions

**Returns:** New Dataset with SELECT clause

#### `#order(*columns)`

Add ORDER BY clause.

```ruby
users = db[:users]

# Simple ordering
users.order(:name)

# Multiple columns
users.order(:name, :created_at)

# Descending order
users.order(Sequel.desc(:created_at))

# Mixed ordering
users.order(:name, Sequel.desc(:created_at))
```

**Parameters:**
- `columns`: Column names or ordering expressions

**Returns:** New Dataset with ORDER BY clause

#### `#limit(count, offset = nil)`

Add LIMIT and optional OFFSET.

```ruby
users = db[:users]

# Limit only
users.limit(10)

# Limit with offset
users.limit(10, 20)

# Pagination helper
users.paginate(page: 2, per_page: 10)
```

**Parameters:**
- `count` (Integer): Maximum number of rows
- `offset` (Integer): Number of rows to skip

**Returns:** New Dataset with LIMIT clause

#### `#group(*columns)`

Add GROUP BY clause.

```ruby
orders = db[:orders]

# Group by single column
orders.group(:status)

# Group by multiple columns
orders.group(:status, :user_id)

# With aggregation
orders.group(:status).select(:status, Sequel.count(:id).as(:count))
```

**Parameters:**
- `columns`: Column names to group by

**Returns:** New Dataset with GROUP BY clause

#### `#having(conditions)`

Add HAVING clause (used with GROUP BY).

```ruby
orders = db[:orders]

orders.group(:user_id)
      .select(:user_id, Sequel.sum(:total).as(:total_spent))
      .having { sum(:total) > 1000 }
```

**Parameters:**
- `conditions`: Conditions for HAVING clause

**Returns:** New Dataset with HAVING clause

### Join Methods

#### `#join(table, conditions = nil, options = {})`

Add INNER JOIN.

```ruby
users = db[:users]

# Simple join
users.join(:orders, user_id: :id)

# Join with table aliases
users.join(:orders___o, user_id: :id)

# Complex join conditions
users.join(:orders, Sequel[:orders][:user_id] => Sequel[:users][:id])
```

**Parameters:**
- `table`: Table to join (Symbol/String)
- `conditions`: Join conditions (Hash or Sequel expression)
- `options`: Join options

**Returns:** New Dataset with JOIN clause

#### `#left_join(table, conditions = nil, options = {})`

Add LEFT OUTER JOIN.

```ruby
users.left_join(:profiles, user_id: :id)
```

#### `#right_join(table, conditions = nil, options = {})`

Add RIGHT OUTER JOIN.

```ruby
users.right_join(:orders, user_id: :id)
```

#### `#full_join(table, conditions = nil, options = {})`

Add FULL OUTER JOIN.

```ruby
users.full_join(:profiles, user_id: :id)
```

### Data Retrieval Methods

#### `#all`

Retrieve all matching records.

```ruby
users = db[:users].where(active: true).all
# => [{id: 1, name: 'John', ...}, {id: 2, name: 'Jane', ...}]
```

**Returns:** Array of record hashes

#### `#first`

Retrieve first matching record.

```ruby
user = db[:users].where(email: 'john@example.com').first
# => {id: 1, name: 'John', email: 'john@example.com', ...}
```

**Returns:** Record hash or nil if not found

#### `#last`

Retrieve last matching record (requires ORDER BY).

```ruby
user = db[:users].order(:created_at).last
```

**Returns:** Record hash or nil if not found

#### `#count`

Count matching records.

```ruby
count = db[:users].where(active: true).count
# => 42
```

**Returns:** Integer count

#### `#each(&block)`

Iterate over all matching records.

```ruby
db[:users].where(active: true).each do |user|
  puts user[:name]
end
```

**Parameters:**
- `block`: Block to execute for each record

**Returns:** Dataset (for chaining)

#### `#paged_each(options = {}, &block)`

Iterate over records in batches for memory efficiency.

```ruby
db[:large_table].paged_each(rows_per_fetch: 1000) do |row|
  process_row(row)
end
```

**Parameters:**
- `options` (Hash): Paging options
  - `:rows_per_fetch` (Integer): Batch size (default: 1000)
- `block`: Block to execute for each record

### Data Modification Methods

#### `#insert(values)`

Insert a single record.

```ruby
user_id = db[:users].insert(
  name: 'John Doe',
  email: 'john@example.com',
  created_at: Time.now
)
```

**Parameters:**
- `values` (Hash): Column values to insert

**Returns:** Inserted record ID (if available)

#### `#multi_insert(array)`

Insert multiple records efficiently.

```ruby
db[:users].multi_insert([
  {name: 'Alice', email: 'alice@example.com'},
  {name: 'Bob', email: 'bob@example.com'},
  {name: 'Charlie', email: 'charlie@example.com'}
])
```

**Parameters:**
- `array` (Array): Array of record hashes

**Returns:** Number of inserted records

#### `#update(values)`

Update matching records.

```ruby
affected_rows = db[:users]
  .where(active: false)
  .update(active: true, updated_at: Time.now)
```

**Parameters:**
- `values` (Hash): Column values to update

**Returns:** Number of affected rows

#### `#delete`

Delete matching records.

```ruby
deleted_count = db[:users].where { created_at < Date.today - 365 }.delete
```

**Returns:** Number of deleted rows

### Analytical Methods (DuckDB-Specific)

#### Window Functions

```ruby
# Ranking within groups
db[:sales].select(
  :product_id,
  :amount,
  Sequel.function(:rank).over(
    partition: :category_id,
    order: Sequel.desc(:amount)
  ).as(:rank)
)

# Running totals
db[:sales].select(
  :date,
  :amount,
  Sequel.function(:sum, :amount).over(
    order: :date
  ).as(:running_total)
)
```

#### Common Table Expressions (CTEs)

```ruby
# Simple CTE
db.with(:high_spenders,
  db[:orders].group(:user_id).having { sum(:total) > 1000 }.select(:user_id)
).from(:high_spenders).join(:users, id: :user_id)

# Recursive CTE
db.with_recursive(:category_tree,
  db[:categories].where(parent_id: nil),
  db[:categories].join(:category_tree, parent_id: :id)
).from(:category_tree)
```

## SQL Generation Patterns

The sequel-duckdb adapter generates SQL optimized for DuckDB's analytical capabilities while maintaining compatibility with Sequel conventions. Understanding these patterns helps developers write efficient queries and troubleshoot issues.

### Key SQL Pattern Features

- **Clean LIKE clauses** without unnecessary ESCAPE clauses
- **ILIKE conversion** to UPPER() LIKE UPPER() for case-insensitive matching
- **Regex support** using DuckDB's regexp_matches() function
- **Qualified column references** using standard dot notation
- **Automatic recursive CTE detection** for WITH RECURSIVE syntax
- **Proper expression parentheses** for correct operator precedence

### Quick Reference

```ruby
# LIKE patterns (clean syntax)
dataset.where(Sequel.like(:name, "%John%"))
# SQL: SELECT * FROM users WHERE (name LIKE '%John%')

# ILIKE patterns (case-insensitive)
dataset.where(Sequel.ilike(:name, "%john%"))
# SQL: SELECT * FROM users WHERE (UPPER(name) LIKE UPPER('%john%'))

# Regex patterns
dataset.where(name: /^John/)
# SQL: SELECT * FROM users WHERE (regexp_matches(name, '^John'))

# Qualified column references
dataset.join(:profiles, user_id: :id)
# SQL: SELECT * FROM users INNER JOIN profiles ON (profiles.user_id = users.id)

# Recursive CTEs (auto-detected)
base_case = db.select(Sequel.as(1, :n))
recursive_case = db[:t].select(Sequel.lit("n + 1")).where { n < 10 }
combined = base_case.union(recursive_case, all: true)
dataset.with(:t, combined).from(:t)
# SQL: WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE (n < 10)) SELECT * FROM t
```

### Detailed Documentation

For comprehensive documentation of all SQL patterns, including design decisions and troubleshooting tips, see [DUCKDB_SQL_PATTERNS.md](DUCKDB_SQL_PATTERNS.md).

## Configuration Options

### Database Configuration

```ruby
db = Sequel.connect(
  adapter: 'duckdb',
  database: '/path/to/database.duckdb',

  # Connection options
  readonly: false,

  # DuckDB-specific configuration
  config: {
    # Memory management
    memory_limit: '4GB',          # Maximum memory usage
    max_memory: '8GB',            # Memory limit before spilling to disk
    temp_directory: '/tmp/duckdb', # Temporary file location

    # Performance tuning
    threads: 8,                   # Number of threads for parallel processing
    enable_optimizer: true,       # Enable query optimizer
    enable_profiling: false,      # Enable query profiling

    # Behavioral settings
    default_order: 'ASC',         # Default sort order
    preserve_insertion_order: false, # Preserve insertion order

    # Extension settings
    autoload_known_extensions: true, # Auto-load known extensions
    autoinstall_known_extensions: false # Auto-install extensions
  },

  # Sequel connection pool options
  max_connections: 10,            # Connection pool size
  pool_timeout: 5,                # Connection timeout in seconds
  pool_sleep_time: 0.001,         # Sleep time between connection retries
  pool_connection_validation: true # Validate connections before use
)
```

### Runtime Configuration

```ruby
# Change settings at runtime
db.run "SET memory_limit='2GB'"
db.run "SET threads=4"
db.run "SET enable_profiling=true"

# Check current settings
db.fetch("SELECT * FROM duckdb_settings()").all
```

## Error Handling

### Exception Hierarchy

The adapter maps DuckDB errors to appropriate Sequel exception types:

```ruby
begin
  db[:users].insert(name: nil)  # NOT NULL violation
rescue Sequel::NotNullConstraintViolation => e
  puts "Cannot insert null name: #{e.message}"
rescue Sequel::UniqueConstraintViolation => e
  puts "Duplicate value: #{e.message}"
rescue Sequel::ForeignKeyConstraintViolation => e
  puts "Foreign key violation: #{e.message}"
rescue Sequel::CheckConstraintViolation => e
  puts "Check constraint failed: #{e.message}"
rescue Sequel::ConstraintViolation => e
  puts "Constraint violation: #{e.message}"
rescue Sequel::DatabaseConnectionError => e
  puts "Connection error: #{e.message}"
rescue Sequel::DatabaseError => e
  puts "Database error: #{e.message}"
end
```

### Error Types

| Sequel Exception | DuckDB Error Patterns | Description |
|------------------|----------------------|-------------|
| `NotNullConstraintViolation` | `violates not null`, `null value not allowed` | NOT NULL constraint violations |
| `UniqueConstraintViolation` | `unique constraint`, `duplicate key` | UNIQUE constraint violations |
| `ForeignKeyConstraintViolation` | `foreign key constraint`, `violates foreign key` | Foreign key violations |
| `CheckConstraintViolation` | `check constraint`, `violates check` | CHECK constraint violations |
| `ConstraintViolation` | `constraint violation` | Generic constraint violations |
| `DatabaseConnectionError` | `connection`, `cannot open`, `database not found` | Connection-related errors |
| `DatabaseError` | `syntax error`, `parse error`, `table does not exist` | General database errors |

## Data Type Mappings

### Ruby to DuckDB Type Mapping

| Ruby Type | DuckDB Type | Notes |
|-----------|-------------|-------|
| `String` | `VARCHAR` | Default string type |
| `String` (large) | `TEXT` | For long text content |
| `Integer` | `INTEGER` | 32-bit signed integer |
| `Integer` (large) | `BIGINT` | 64-bit signed integer |
| `Float` | `DOUBLE` | Double precision floating point |
| `BigDecimal` | `DECIMAL` | Exact numeric with precision/scale |
| `TrueClass/FalseClass` | `BOOLEAN` | Native boolean type |
| `Date` | `DATE` | Date without time |
| `Time/DateTime` | `TIMESTAMP` | Date and time |
| `Time` (time-only) | `TIME` | Time without date |
| `String` (binary) | `BLOB` | Binary data |
| `Array` | `ARRAY` | DuckDB array types |
| `Hash` | `JSON` | JSON data type |

### DuckDB to Ruby Type Mapping

| DuckDB Type | Ruby Type | Conversion Notes |
|-------------|-----------|------------------|
| `INTEGER`, `INT4` | `Integer` | 32-bit integer |
| `BIGINT`, `INT8` | `Integer` | 64-bit integer |
| `SMALLINT`, `INT2` | `Integer` | 16-bit integer |
| `TINYINT`, `INT1` | `Integer` | 8-bit integer |
| `REAL`, `FLOAT4` | `Float` | Single precision |
| `DOUBLE`, `FLOAT8` | `Float` | Double precision |
| `DECIMAL`, `NUMERIC` | `BigDecimal` | Exact numeric |
| `VARCHAR`, `TEXT` | `String` | Text data |
| `BOOLEAN` | `TrueClass/FalseClass` | Boolean values |
| `DATE` | `Date` | Date only |
| `TIMESTAMP` | `Time` | Date and time |
| `TIME` | `Time` | Time only |
| `BLOB`, `BYTEA` | `String` | Binary data as string |
| `JSON` | `String` | JSON as string (parse manually) |
| `ARRAY` | `Array` | Native array support |
| `UUID` | `String` | UUID as string |

### Custom Type Handling

```ruby
# Register custom type conversion
db.conversion_procs[DuckDB::Type::UUID] = proc { |value|
  UUID.parse(value) if value
}

# Handle JSON columns
class Product < Sequel::Model
  def metadata
    JSON.parse(super) if super
  end

  def metadata=(value)
    super(value.to_json)
  end
end
```

## Performance Tuning

### Query Optimization

```ruby
# Use EXPLAIN to analyze queries
puts db[:users].join(:orders, user_id: :id).explain

# Create appropriate indexes
db.add_index :users, :email
db.add_index :orders, [:user_id, :status]
db.add_index :products, [:category_id, :active]

# Use partial indexes for filtered queries
db.add_index :products, :price, where: { active: true }
```

### Memory Management

```ruby
# Configure memory limits
db.run "SET memory_limit='4GB'"
db.run "SET max_memory='8GB'"

# Use streaming for large result sets
db[:large_table].paged_each(rows_per_fetch: 1000) do |row|
  process_row(row)
end
```

### Bulk Operations

```ruby
# Efficient bulk insert
data = 10000.times.map { |i| {name: "User #{i}", email: "user#{i}@example.com"} }
db[:users].multi_insert(data)

# Batch processing
data.each_slice(1000) do |batch|
  db.transaction do
    db[:users].multi_insert(batch)
  end
end
```

### Connection Pooling

```ruby
# Optimize connection pool
db = Sequel.connect(
  'duckdb:///database.duckdb',
  max_connections: 20,
  pool_timeout: 10,
  pool_sleep_time: 0.001
)
```

This comprehensive API documentation covers all major aspects of using the Sequel DuckDB adapter. For the most up-to-date information, refer to the inline YARD documentation in the source code.