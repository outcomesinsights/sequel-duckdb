# Performance Optimization Guide for Sequel-DuckDB

This guide provides comprehensive strategies for optimizing performance when using Sequel with DuckDB, leveraging DuckDB's unique strengths as an analytical database engine.

## Table of Contents

1. [Understanding DuckDB's Architecture](#understanding-duckdbs-architecture)
2. [Query Optimization](#query-optimization)
3. [Schema Design](#schema-design)
4. [Bulk Operations](#bulk-operations)
5. [Memory Management](#memory-management)
6. [Connection Optimization](#connection-optimization)
7. [Monitoring and Profiling](#monitoring-and-profiling)
8. [Best Practices](#best-practices)

## Understanding DuckDB's Architecture

DuckDB is designed as an analytical database with several key characteristics that affect performance optimization:

### Columnar Storage
- Data is stored column-wise, making analytical queries very efficient
- SELECT queries that access few columns are much faster
- Aggregations and analytical functions are highly optimized

### Vectorized Execution
- Operations are performed on batches of data (vectors) rather than row-by-row
- This reduces function call overhead and improves CPU cache utilization
- Particularly beneficial for analytical workloads

### In-Memory Processing
- DuckDB can efficiently process data that fits in memory
- Automatic memory management with spill-to-disk for larger datasets
- Memory-mapped files for efficient file-based database access

## Query Optimization

### 1. Column Selection Optimization

**Always select only the columns you need:**

```ruby
# ❌ Inefficient - selects all columns
users = db[:users].where(active: true).all

# ✅ Efficient - selects only needed columns
users = db[:users].select(:id, :name, :email).where(active: true).all

# ✅ Even better for large result sets
db[:users].select(:id, :name, :email).where(active: true).each do |user|
  # Process user
end
```

### 2. Predicate Pushdown

**Apply filters as early as possible:**

```ruby
# ❌ Less efficient - filtering after join
result = db[:users]
  .join(:orders, user_id: :id)
  .where(users__active: true, orders__status: 'completed')

# ✅ More efficient - filter before join when possible
active_users = db[:users].where(active: true)
completed_orders = db[:orders].where(status: 'completed')
result = active_users.join(completed_orders, user_id: :id)
```

### 3. Index Utilization

**Create indexes for frequently queried columns:**

```ruby
# Create indexes for common query patterns
db.add_index :users, :email
db.add_index :orders, [:user_id, :status]
db.add_index :products, [:category_id, :active]

# Composite indexes for multi-column queries
db.add_index :order_items, [:order_id, :product_id]

# Partial indexes for filtered queries
db.add_index :products, :price, where: { active: true }
```

### 4. Query Plan Analysis

**Use EXPLAIN to understand query execution:**

```ruby
# Analyze query performance
query = db[:users].join(:orders, user_id: :id).where(status: 'completed')
puts query.explain

# Look for:
# - Index usage
# - Join algorithms
# - Filter pushdown
# - Estimated row counts
```

### 5. Analytical Query Optimization

**Leverage DuckDB's analytical capabilities:**

```ruby
# ✅ Efficient analytical queries
sales_summary = db[:sales]
  .select(
    :product_category,
    Sequel.function(:sum, :amount).as(:total_sales),
    Sequel.function(:avg, :amount).as(:avg_sale),
    Sequel.function(:count, :id).as(:transaction_count),
    Sequel.function(:percentile_cont, 0.5).within_group(:amount).as(:median_sale)
  )
  .group(:product_category)
  .order(Sequel.desc(:total_sales))

# ✅ Window functions for advanced analytics
monthly_trends = db[:sales]
  .select(
    :month,
    :amount,
    Sequel.function(:lag, :amount, 1).over(order: :month).as(:prev_month),
    Sequel.function(:sum, :amount).over(order: :month).as(:running_total),
    Sequel.function(:rank).over(partition: :category, order: Sequel.desc(:amount)).as(:category_rank)
  )
```

## Schema Design

### 1. Optimal Data Types

**Choose appropriate data types for performance:**

```ruby
# ✅ Efficient data types
db.create_table :products do
  primary_key :id                    # INTEGER is efficient
  String :name, size: 255           # Fixed-size strings when possible
  Decimal :price, size: [10, 2]     # Precise for monetary values
  Integer :stock_quantity           # INTEGER for counts
  Boolean :active                   # BOOLEAN is very efficient
  Date :created_date               # DATE for date-only values
  DateTime :created_at             # TIMESTAMP for full datetime

  # DuckDB-specific optimized types
  column :tags, 'VARCHAR[]'        # Arrays for multi-value attributes
  column :metadata, 'JSON'         # JSON for flexible data
end

# ❌ Avoid oversized types
# String :description, size: 10000  # Use TEXT instead
# Float :price                      # Use DECIMAL for money
```

### 2. Partitioning Strategy

**Design tables for analytical workloads:**

```ruby
# ✅ Time-based partitioning pattern
db.create_table :sales_2024_q1 do
  primary_key :id
  foreign_key :product_id, :products
  Decimal :amount, size: [10, 2]
  Date :sale_date
  DateTime :created_at

  # Constraint to enforce partition bounds
  constraint(:date_range) { (sale_date >= '2024-01-01') & (sale_date < '2024-04-01') }
end

# Create view for unified access
db.run <<~SQL
  CREATE VIEW sales AS
  SELECT * FROM sales_2024_q1
  UNION ALL
  SELECT * FROM sales_2024_q2
  -- Add more partitions as needed
SQL
```

### 3. Denormalization for Analytics

**Consider denormalization for read-heavy analytical workloads:**

```ruby
# ✅ Denormalized table for analytics
db.create_table :order_analytics do
  primary_key :id
  Integer :order_id
  Integer :user_id
  String :user_name              # Denormalized from users table
  String :user_email             # Denormalized from users table
  Integer :product_id
  String :product_name           # Denormalized from products table
  String :category_name          # Denormalized from categories table
  Decimal :unit_price, size: [10, 2]
  Integer :quantity
  Decimal :total_amount, size: [10, 2]
  Date :order_date
  DateTime :created_at
end

# Populate with materialized view pattern
db.run <<~SQL
  INSERT INTO order_analytics
  SELECT
    oi.id,
    o.id as order_id,
    u.id as user_id,
    u.name as user_name,
    u.email as user_email,
    p.id as product_id,
    p.name as product_name,
    c.name as category_name,
    oi.unit_price,
    oi.quantity,
    oi.unit_price * oi.quantity as total_amount,
    o.created_at::DATE as order_date,
    o.created_at
  FROM order_items oi
  JOIN orders o ON oi.order_id = o.id
  JOIN users u ON o.user_id = u.id
  JOIN products p ON oi.product_id = p.id
  JOIN categories c ON p.category_id = c.id
SQL
```

## Bulk Operations

### 1. Efficient Bulk Inserts

**Use multi_insert for large data loads:**

```ruby
# ✅ Efficient bulk insert
data = []
1000.times do |i|
  data << {
    name: "User #{i}",
    email: "user#{i}@example.com",
    created_at: Time.now
  }
end

# Single transaction for all inserts
db.transaction do
  db[:users].multi_insert(data)
end

# ✅ For very large datasets, use batching
def bulk_insert_batched(db, table, data, batch_size = 1000)
  data.each_slice(batch_size) do |batch|
    db.transaction do
      db[table].multi_insert(batch)
    end
  end
end

bulk_insert_batched(db, :users, large_dataset, 5000)
```

### 2. Bulk Updates

**Efficient bulk update patterns:**

```ruby
# ✅ Single UPDATE statement for bulk changes
db[:products].where(category_id: 1).update(
  active: false,
  updated_at: Time.now
)

# ✅ Conditional bulk updates
db.run <<~SQL
  UPDATE products
  SET
    status = CASE
      WHEN stock_quantity = 0 THEN 'out_of_stock'
      WHEN stock_quantity < 10 THEN 'low_stock'
      ELSE 'in_stock'
    END,
    updated_at = NOW()
  WHERE status != CASE
    WHEN stock_quantity = 0 THEN 'out_of_stock'
    WHEN stock_quantity < 10 THEN 'low_stock'
    ELSE 'in_stock'
  END
SQL
```

### 3. Data Loading from Files

**Leverage DuckDB's file reading capabilities:**

```ruby
# ✅ Direct CSV import (very fast)
db.run <<~SQL
  CREATE TABLE temp_sales AS
  SELECT * FROM read_csv_auto('sales_data.csv')
SQL

# ✅ Parquet files (excellent for analytics)
db.run <<~SQL
  CREATE TABLE sales_archive AS
  SELECT * FROM read_parquet('sales_archive.parquet')
SQL

# ✅ JSON files
db.run <<~SQL
  CREATE TABLE user_events AS
  SELECT * FROM read_json_auto('user_events.json')
SQL
```

## Memory Management

### 1. Connection Configuration

**Optimize DuckDB memory settings:**

```ruby
# ✅ Configure memory limits
db = Sequel.connect(
  adapter: 'duckdb',
  database: '/path/to/database.duckdb',
  config: {
    memory_limit: '4GB',           # Set appropriate memory limit
    threads: 8,                    # Use multiple threads
    max_memory: '8GB',             # Maximum memory before spilling
    temp_directory: '/tmp/duckdb'  # Temporary file location
  }
)

# ✅ Runtime memory configuration
db.run "SET memory_limit='2GB'"
db.run "SET threads=4"
```

### 2. Result Set Management

**Handle large result sets efficiently:**

```ruby
# ❌ Loads entire result set into memory
all_orders = db[:orders].all

# ✅ Process results in batches
db[:orders].paged_each(rows_per_fetch: 1000) do |order|
  # Process each order
  process_order(order)
end

# ✅ Use streaming for very large datasets
db[:large_table].use_cursor.each do |row|
  # Process row by row without loading all into memory
  process_row(row)
end

# ✅ Limit result sets when possible
recent_orders = db[:orders]
  .where { created_at > Date.today - 30 }
  .order(Sequel.desc(:created_at))
  .limit(1000)
```

### 3. Connection Pooling

**Optimize connection management:**

```ruby
# ✅ Connection pool configuration
db = Sequel.connect(
  adapter: 'duckdb',
  database: '/path/to/database.duckdb',
  max_connections: 10,           # Pool size
  pool_timeout: 5,               # Connection timeout
  pool_sleep_time: 0.001,        # Sleep between retries
  pool_connection_validation: true
)

# ✅ Proper connection cleanup
begin
  db.transaction do
    # Database operations
  end
ensure
  db.disconnect if db
end
```

## Connection Optimization

### 1. Connection Reuse

**Minimize connection overhead:**

```ruby
# ✅ Reuse connections
class DatabaseManager
  def self.connection
    @connection ||= Sequel.connect('duckdb:///app.duckdb')
  end

  def self.disconnect
    @connection&.disconnect
    @connection = nil
  end
end

# Use throughout application
db = DatabaseManager.connection
```

### 2. Transaction Management

**Optimize transaction usage:**

```ruby
# ✅ Group related operations in transactions
db.transaction do
  user_id = db[:users].insert(name: 'John', email: 'john@example.com')
  profile_id = db[:profiles].insert(user_id: user_id, bio: 'Developer')
  db[:preferences].insert(user_id: user_id, theme: 'dark')
end

# ✅ Use savepoints for nested operations
db.transaction do
  user_id = db[:users].insert(name: 'Jane', email: 'jane@example.com')

  begin
    db.transaction(savepoint: true) do
      # Risky operation that might fail
      db[:audit_log].insert(user_id: user_id, action: 'created')
    end
  rescue Sequel::DatabaseError
    # Continue even if audit logging fails
  end
end
```

## Monitoring and Profiling

### 1. Query Logging

**Enable comprehensive logging:**

```ruby
# ✅ Enable SQL logging
require 'logger'
db.loggers << Logger.new($stdout)

# ✅ Custom logger with timing
class PerformanceLogger < Logger
  def initialize(*args)
    super
    @start_times = {}
  end

  def info(message)
    if message.include?('SELECT') || message.include?('INSERT') || message.include?('UPDATE')
      @start_time = Time.now
      super("SQL: #{message}")
    end
  end

  def debug(message)
    if @start_time && message.include?('rows')
      duration = Time.now - @start_time
      super("Duration: #{duration.round(3)}s - #{message}")
      @start_time = nil
    else
      super
    end
  end
end

db.loggers << PerformanceLogger.new($stdout)
```

### 2. Performance Monitoring

**Monitor key performance metrics:**

```ruby
# ✅ Query performance monitoring
class QueryMonitor
  def self.monitor_query(description, &block)
    start_time = Time.now
    result = yield
    duration = Time.now - start_time

    if duration > 1.0  # Log slow queries
      puts "SLOW QUERY (#{duration.round(3)}s): #{description}"
    end

    result
  end
end

# Usage
users = QueryMonitor.monitor_query("Fetch active users") do
  db[:users].where(active: true).all
end
```

### 3. Database Statistics

**Monitor database performance:**

```ruby
# ✅ Check database statistics
def print_db_stats(db)
  # Table sizes
  puts "Table Statistics:"
  db.tables.each do |table|
    count = db[table].count
    puts "  #{table}: #{count} rows"
  end

  # Index usage (if available)
  puts "\nIndex Information:"
  db.tables.each do |table|
    indexes = db.indexes(table)
    puts "  #{table}: #{indexes.keys.join(', ')}" if indexes.any?
  end
end

print_db_stats(db)
```

## Best Practices

### 1. Query Design Patterns

```ruby
# ✅ Efficient analytical query pattern
def sales_report(db, start_date, end_date)
  db[:order_analytics]
    .where(order_date: start_date..end_date)
    .select(
      :category_name,
      Sequel.function(:sum, :total_amount).as(:revenue),
      Sequel.function(:count, :order_id).as(:order_count),
      Sequel.function(:avg, :total_amount).as(:avg_order_value)
    )
    .group(:category_name)
    .order(Sequel.desc(:revenue))
end

# ✅ Efficient pagination pattern
def paginated_orders(db, page = 1, per_page = 50)
  offset = (page - 1) * per_page

  db[:orders]
    .select(:id, :user_id, :total, :status, :created_at)
    .order(Sequel.desc(:created_at), :id)  # Stable sort
    .limit(per_page)
    .offset(offset)
end
```

### 2. Caching Strategies

```ruby
# ✅ Application-level caching
class CachedQueries
  def self.user_stats(db, user_id)
    @cache ||= {}
    cache_key = "user_stats_#{user_id}"

    @cache[cache_key] ||= db[:orders]
      .where(user_id: user_id)
      .select(
        Sequel.function(:count, :id).as(:order_count),
        Sequel.function(:sum, :total).as(:total_spent),
        Sequel.function(:avg, :total).as(:avg_order)
      )
      .first
  end

  def self.clear_cache
    @cache = {}
  end
end
```

### 3. Error Handling and Retry Logic

```ruby
# ✅ Robust error handling
def execute_with_retry(db, max_retries = 3)
  retries = 0

  begin
    yield
  rescue Sequel::DatabaseConnectionError => e
    retries += 1
    if retries <= max_retries
      sleep(0.1 * retries)  # Exponential backoff
      retry
    else
      raise e
    end
  end
end

# Usage
result = execute_with_retry(db) do
  db[:users].where(active: true).count
end
```

### 4. Development vs Production Optimization

```ruby
# ✅ Environment-specific configuration
class DatabaseConfig
  def self.connection_options
    if ENV['RAILS_ENV'] == 'production'
      {
        adapter: 'duckdb',
        database: '/var/lib/app/production.duckdb',
        config: {
          memory_limit: '8GB',
          threads: 16,
          max_memory: '16GB'
        },
        max_connections: 20,
        pool_timeout: 10
      }
    else
      {
        adapter: 'duckdb',
        database: ':memory:',
        config: {
          memory_limit: '1GB',
          threads: 4
        },
        max_connections: 5
      }
    end
  end
end

db = Sequel.connect(DatabaseConfig.connection_options)
```

## Performance Testing

### 1. Benchmarking Queries

```ruby
require 'benchmark'

# ✅ Query performance testing
def benchmark_query(description, iterations = 100)
  puts "Benchmarking: #{description}"

  time = Benchmark.measure do
    iterations.times { yield }
  end

  puts "  Total time: #{time.real.round(3)}s"
  puts "  Average: #{(time.real / iterations * 1000).round(3)}ms per query"
  puts "  Queries/sec: #{(iterations / time.real).round(1)}"
end

# Usage
benchmark_query("User lookup by email") do
  db[:users].where(email: 'test@example.com').first
end

benchmark_query("Sales aggregation") do
  db[:orders].where(status: 'completed').sum(:total)
end
```

### 2. Load Testing

```ruby
# ✅ Concurrent load testing
require 'thread'

def load_test(db, concurrent_users = 10, queries_per_user = 100)
  threads = []
  results = Queue.new

  concurrent_users.times do |user_id|
    threads << Thread.new do
      start_time = Time.now

      queries_per_user.times do
        # Simulate user queries
        db[:users].where(id: rand(1000)).first
        db[:orders].where(user_id: rand(1000)).count
      end

      duration = Time.now - start_time
      results << { user_id: user_id, duration: duration }
    end
  end

  threads.each(&:join)

  # Collect results
  total_queries = concurrent_users * queries_per_user
  total_time = results.size.times.map { results.pop[:duration] }.max

  puts "Load test results:"
  puts "  #{concurrent_users} concurrent users"
  puts "  #{total_queries} total queries"
  puts "  #{total_time.round(3)}s total time"
  puts "  #{(total_queries / total_time).round(1)} queries/sec"
end

load_test(db)
```

This comprehensive performance optimization guide should help you get the most out of DuckDB's analytical capabilities while using Sequel. Remember that DuckDB excels at analytical workloads, so design your queries and schema to take advantage of its columnar storage and vectorized execution engine.