# Sequel::DuckDB

A Ruby database adapter that enables [Sequel](https://sequel.jeremyevans.net/) to work with [DuckDB](https://duckdb.org/) databases. This gem provides full integration between Sequel's powerful ORM and query building capabilities with DuckDB's high-performance analytical database engine.

## Features

- **Complete Sequel Integration**: Full compatibility with Sequel's Database and Dataset APIs
- **Connection Management**: Support for both file-based and in-memory DuckDB databases
- **Schema Introspection**: Automatic discovery of tables, columns, indexes, and constraints
- **SQL Generation**: DuckDB-optimized SQL generation for all standard operations
- **Data Type Mapping**: Seamless conversion between Ruby and DuckDB data types
- **Transaction Support**: Full transaction handling with commit/rollback capabilities
- **Error Handling**: Comprehensive error mapping to appropriate Sequel exceptions
- **Performance Optimized**: Leverages DuckDB's columnar storage and parallel processing

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sequel-duckdb'
```

And then execute:

```bash
bundle install
```

Or install it yourself as:

```bash
gem install sequel-duckdb
```

## Quick Start

### Basic Connection

```ruby
require 'sequel'

# Connect to an in-memory database
db = Sequel.connect('duckdb::memory:')

# Connect to a file database
db = Sequel.connect('duckdb:///path/to/database.duckdb')

# Alternative connection syntax
db = Sequel.connect(
  adapter: 'duckdb',
  database: '/path/to/database.duckdb'
)
```

### Basic Usage

```ruby
# Create a table
db.create_table :users do
  primary_key :id
  String :name, null: false
  String :email
  Integer :age
  DateTime :created_at
end

# Insert data
users = db[:users]
users.insert(name: 'John Doe', email: 'john@example.com', age: 30, created_at: Time.now)
users.insert(name: 'Jane Smith', email: 'jane@example.com', age: 25, created_at: Time.now)

# Query data
puts users.where(age: 30).first
# => {:id=>1, :name=>"John Doe", :email=>"john@example.com", :age=>30, :created_at=>...}

puts users.where { age > 25 }.all
# => [{:id=>1, :name=>"John Doe", ...}, ...]

# Update data
users.where(name: 'John Doe').update(age: 31)

# Delete data
users.where(age: 25).delete
```

## Development

After checking out the repo, run `bin/setup` to install dependencies:

```bash
git clone https://github.com/aguynamedryan/sequel-duckdb.git
cd sequel-duckdb
bin/setup
```

### Running Tests

The test suite uses Minitest and includes both unit tests (using Sequel's mock database) and integration tests (using real DuckDB databases):

```bash
# Run all tests
bundle exec rake test

# Run specific test file
ruby test/database_test.rb

# Run tests with verbose output
ruby test/all.rb -v
```

### Development Console

You can run `bin/console` for an interactive prompt that will allow you to experiment:

```bash
bin/console
```

This will start an IRB session with the gem loaded and a test database available.

### Code Quality

The project uses RuboCop for code style enforcement:

```bash
# Check code style
bundle exec rubocop

# Auto-fix style issues
bundle exec rubocop -a
```

### Building and Installing

To install this gem onto your local machine:

```bash
bundle exec rake install
```

To build the gem:

```bash
bundle exec rake build
```

### Release Process

To release a new version:

1. Update the version number in `lib/sequel/duckdb/version.rb`
2. Update `CHANGELOG.md` with the new version details
3. Run the tests to ensure everything works
4. Commit the changes
5. Run `bundle exec rake release`

This will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/aguynamedryan/sequel-duckdb.

### Development Guidelines

1. **Follow TDD**: Write tests before implementing features
2. **Code Style**: Follow the existing RuboCop configuration
3. **Documentation**: Update README and code documentation for new features
4. **Compatibility**: Ensure compatibility with supported Ruby and Sequel versions
5. **Performance**: Consider performance implications, especially for analytical workloads

### Reporting Issues

When reporting issues, please include:

- Ruby version
- Sequel version
- DuckDB version
- Operating system
- Minimal code example that reproduces the issue
- Full error message and stack trace

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Acknowledgments

- [Jeremy Evans](https://github.com/jeremyevans) for creating and maintaining Sequel
- The [DuckDB team](https://duckdb.org/docs/api/ruby) for the excellent database engine and Ruby client
- Contributors to [sequel-hexspace](https://github.com/hexspace/sequel-hexspace) and other Sequel adapters for implementation patterns
## Connection Options

### Connection Strings

```ruby
# In-memory database (data lost when connection closes)
db = Sequel.connect('duckdb::memory:')

# File database (persistent storage)
db = Sequel.connect('duckdb:///absolute/path/to/database.duckdb')
db = Sequel.connect('duckdb://relative/path/to/database.duckdb')

# With connection options
db = Sequel.connect('duckdb:///path/to/database.duckdb?readonly=true')
```

### Connection Hash

```ruby
db = Sequel.connect(
  adapter: 'duckdb',
  database: '/path/to/database.duckdb',
  # DuckDB-specific options
  readonly: false,
  config: {
    threads: 4,
    memory_limit: '1GB'
  }
)
```

## Schema Operations

### Table Management

```ruby
# Create table with various column types
db.create_table :products do
  primary_key :id
  String :name, size: 255, null: false
  Text :description
  Decimal :price, size: [10, 2]
  Integer :stock_quantity
  Boolean :active, default: true
  Date :release_date
  DateTime :created_at
  Time :daily_update_time
  column :metadata, 'JSON'  # DuckDB-specific type
end

# Add columns
db.alter_table :products do
  add_column :category_id, Integer
  add_index :category_id
end

# Drop table
db.drop_table :products
```

### Schema Introspection

```ruby
# List all tables
db.tables
# => [:users, :products, :orders]

# Get table schema
db.schema(:users)
# => [[:id, {:type=>:integer, :db_type=>"INTEGER", :primary_key=>true, ...}],
#     [:name, {:type=>:string, :db_type=>"VARCHAR", :allow_null=>false, ...}], ...]

# Check if table exists
db.table_exists?(:users)
# => true

# Get indexes
db.indexes(:users)
# => {:users_name_index => {:columns=>[:name], :unique=>false, :primary=>false}}
```

## Data Types

### Supported Type Mappings

| Ruby Type | DuckDB Type | Notes |
|-----------|-------------|-------|
| String | VARCHAR/TEXT | Configurable size |
| Integer | INTEGER/BIGINT | Auto-sized based on value |
| Float | REAL/DOUBLE | Precision preserved |
| BigDecimal | DECIMAL/NUMERIC | Precision and scale supported |
| TrueClass/FalseClass | BOOLEAN | Native boolean support |
| Date | DATE | Date-only values |
| Time/DateTime | TIMESTAMP | Full datetime with timezone |
| Time (time-only) | TIME | Time-only values |
| String (binary) | BLOB | Binary data storage |

### Type Conversion Examples

```ruby
# Automatic type conversion
users.insert(
  name: 'Alice',                    # String -> VARCHAR
  age: 28,                         # Integer -> INTEGER
  salary: BigDecimal('75000.50'),  # BigDecimal -> DECIMAL
  active: true,                    # Boolean -> BOOLEAN
  birth_date: Date.new(1995, 5, 15), # Date -> DATE
  created_at: Time.now,            # Time -> TIMESTAMP
  profile_data: '{"key": "value"}' # String -> JSON (if column defined as JSON)
)

# Retrieved data is automatically converted back to Ruby types
user = users.first
user[:birth_date].class  # => Date
user[:created_at].class  # => Time
user[:active].class      # => TrueClass
```

## Query Building

### Basic Queries

```ruby
users = db[:users]

# SELECT with conditions
users.where(active: true)
users.where { age > 25 }
users.where(Sequel.like(:name, 'John%'))

# Ordering and limiting
users.order(:name).limit(10)
users.order(Sequel.desc(:created_at)).first

# Aggregation
users.count
users.avg(:age)
users.group(:department).select(:department, Sequel.count(:id).as(:user_count))
```

### Advanced Queries

```ruby
# JOINs
db[:users]
  .join(:orders, user_id: :id)
  .select(:users__name, :orders__total)
  .where { orders__total > 100 }

# Subqueries
high_value_users = db[:orders]
  .group(:user_id)
  .having { sum(:total) > 1000 }
  .select(:user_id)

db[:users].where(id: high_value_users)

# Window functions (DuckDB-specific optimization)
db[:sales]
  .select(
    :product_id,
    :amount,
    Sequel.function(:row_number).over(partition: :product_id, order: :amount).as(:rank)
  )

# Common Table Expressions (CTEs)
db.with(:high_spenders,
  db[:orders].group(:user_id).having { sum(:total) > 1000 }.select(:user_id)
).from(:high_spenders)
 .join(:users, id: :user_id)
 .select(:users__name)
```

## Transactions

### Basic Transactions

```ruby
db.transaction do
  users.insert(name: 'Alice', email: 'alice@example.com')
  orders.insert(user_id: users.max(:id), total: 100.00)
  # Automatically commits if no exceptions
end

# Manual rollback
db.transaction do
  users.insert(name: 'Bob', email: 'bob@example.com')
  raise Sequel::Rollback if some_condition
  # Transaction will be rolled back
end
```

### Error Handling

```ruby
begin
  db.transaction do
    # Some database operations
    users.insert(name: nil)  # This will fail due to NOT NULL constraint
  end
rescue Sequel::NotNullConstraintViolation => e
  puts "Cannot insert user with null name: #{e.message}"
rescue Sequel::DatabaseError => e
  puts "Database error: #{e.message}"
end
```

## DuckDB-Specific Features

### Analytical Queries

```ruby
# DuckDB excels at analytical workloads
sales_summary = db[:sales]
  .select(
    :product_category,
    Sequel.function(:sum, :amount).as(:total_sales),
    Sequel.function(:avg, :amount).as(:avg_sale),
    Sequel.function(:count, :id).as(:transaction_count)
  )
  .group(:product_category)
  .order(Sequel.desc(:total_sales))

# Window functions for analytics
monthly_trends = db[:sales]
  .select(
    :month,
    :amount,
    Sequel.function(:lag, :amount, 1).over(order: :month).as(:prev_month),
    Sequel.function(:sum, :amount).over(order: :month).as(:running_total)
  )
```

### Performance Optimizations

```ruby
# Bulk inserts (more efficient than individual inserts)
users_data = [
  {name: 'User 1', email: 'user1@example.com'},
  {name: 'User 2', email: 'user2@example.com'},
  # ... many more records
]

db[:users].multi_insert(users_data)

# Use DuckDB's columnar storage advantages
# Query only needed columns for better performance
db[:large_table].select(:id, :name, :created_at).where(active: true)
```

## Model Integration

### Sequel::Model Usage

```ruby
class User < Sequel::Model
  # Sequel automatically introspects the users table schema

  # Associations work normally
  one_to_many :orders

  # Validations
  def validate
    super
    errors.add(:email, 'must be present') if !email || email.empty?
    errors.add(:email, 'must be valid') unless email =~ /@/
  end

  # Custom methods
  def full_name
    "#{first_name} #{last_name}"
  end
end

class Order < Sequel::Model
  many_to_one :user

  def total_with_tax(tax_rate = 0.08)
    total * (1 + tax_rate)
  end
end

# Usage
user = User.create(name: 'John Doe', email: 'john@example.com')
order = user.add_order(total: 99.99, status: 'pending')

# Associations work seamlessly
user.orders.where(status: 'completed').sum(:total)
```

## Error Handling

The adapter maps DuckDB errors to appropriate Sequel exception types:

```ruby
begin
  # Various operations that might fail
  db[:users].insert(name: nil)  # NOT NULL violation
rescue Sequel::NotNullConstraintViolation => e
  # Handle null constraint violation
rescue Sequel::UniqueConstraintViolation => e
  # Handle unique constraint violation
rescue Sequel::ForeignKeyConstraintViolation => e
  # Handle foreign key violation
rescue Sequel::CheckConstraintViolation => e
  # Handle check constraint violation
rescue Sequel::DatabaseConnectionError => e
  # Handle connection issues
rescue Sequel::DatabaseError => e
  # Handle other database errors
end
```

## Troubleshooting

### Common Issues

#### Connection Problems

```ruby
# Issue: Cannot connect to database file
# Solution: Check file path and permissions
begin
  db = Sequel.connect('duckdb:///path/to/database.duckdb')
rescue Sequel::DatabaseConnectionError => e
  puts "Connection failed: #{e.message}"
  # Check if directory exists and is writable
  # Ensure DuckDB gem is properly installed
end
```

#### Memory Issues

```ruby
# Issue: Out of memory with large datasets
# Solution: Use streaming or limit result sets
db[:large_table].limit(1000).each do |row|
  # Process row by row instead of loading all at once
end

# Or use DuckDB's memory configuration
db = Sequel.connect(
  adapter: 'duckdb',
  database: ':memory:',
  config: { memory_limit: '2GB' }
)
```

#### Performance Issues

```ruby
# Issue: Slow queries
# Solution: Add appropriate indexes
db.add_index :users, :email
db.add_index :orders, [:user_id, :created_at]

# Use EXPLAIN to analyze query plans
puts db[:users].where(email: 'john@example.com').explain
```

### Debugging

```ruby
# Enable SQL logging to see generated queries
require 'logger'
db.loggers << Logger.new($stdout)

# This will now log all SQL queries
db[:users].where(active: true).all
# Logs: SELECT * FROM users WHERE (active = true)
```

### Version Compatibility

- **Ruby**: 3.1.0 or higher
- **Sequel**: 5.0 or higher
- **DuckDB**: 0.8.0 or higher
- **ruby-duckdb**: 1.0.0 or higher

### Getting Help

- **Documentation**: [Sequel Documentation](https://sequel.jeremyevans.net/documentation.html)
- **DuckDB Docs**: [DuckDB Documentation](https://duckdb.org/docs/)
- **Issues**: Report bugs on [GitHub Issues](https://github.com/aguynamedryan/sequel-duckdb/issues)
- **Discussions**: Join discussions on [GitHub Discussions](https://github.com/aguynamedryan/sequel-duckdb/discussions)

## Performance Tips

### Query Optimization

1. **Select only needed columns**: DuckDB's columnar storage makes this very efficient
   ```ruby
   # Good
   db[:users].select(:id, :name).where(active: true)

   # Less efficient
   db[:users].where(active: true)  # Selects all columns
   ```

2. **Use appropriate indexes**: Especially for frequently queried columns
   ```ruby
   db.add_index :users, :email
   db.add_index :orders, [:user_id, :status]
   ```

3. **Leverage DuckDB's analytical capabilities**: Use window functions and aggregations
   ```ruby
   # Efficient analytical query
   db[:sales]
     .select(
       :product_id,
       Sequel.function(:sum, :amount).as(:total),
       Sequel.function(:rank).over(order: Sequel.desc(:amount)).as(:rank)
     )
     .group(:product_id)
   ```

### Memory Management

1. **Use streaming for large result sets**:
   ```ruby
   db[:large_table].paged_each(rows_per_fetch: 1000) do |row|
     # Process row by row
   end
   ```

2. **Configure DuckDB memory limits**:
   ```ruby
   db = Sequel.connect(
     adapter: 'duckdb',
     database: '/path/to/db.duckdb',
     config: {
       memory_limit: '4GB',
       threads: 8
     }
   )
   ```

### Bulk Operations

1. **Use multi_insert for bulk data loading**:
   ```ruby
   # Efficient bulk insert
   data = 1000.times.map { |i| {name: "User #{i}", email: "user#{i}@example.com"} }
   db[:users].multi_insert(data)
   ```

2. **Use transactions for multiple operations**:
   ```ruby
   db.transaction do
     # Multiple related operations
     user_id = db[:users].insert(name: 'John', email: 'john@example.com')
     db[:profiles].insert(user_id: user_id, bio: 'Software developer')
     db[:preferences].insert(user_id: user_id, theme: 'dark')
   end
   ```