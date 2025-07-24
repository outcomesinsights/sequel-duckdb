# Sequel Migration Examples for DuckDB

This document provides comprehensive examples of using Sequel migrations with DuckDB, covering common patterns, best practices, and DuckDB-specific considerations.

## Basic Migration Structure

### Creating a Migration

```ruby
# db/migrate/001_create_users.rb
Sequel.migration do
  up do
    create_table(:users) do
      primary_key :id
      String :name, null: false, size: 255
      String :email, unique: true, null: false
      Integer :age
      Boolean :active, default: true
      DateTime :created_at, null: false
      DateTime :updated_at, null: false
    end
  end

  down do
    drop_table(:users)
  end
end
```

### Running Migrations

```ruby
# Setup database connection
require 'sequel'
db = Sequel.connect('duckdb:///path/to/database.duckdb')

# Run migrations
Sequel::Migrator.run(db, 'db/migrate')

# Run migrations to specific version
Sequel::Migrator.run(db, 'db/migrate', target: 5)

# Check current migration version
puts "Current version: #{db[:schema_info].first[:version]}"
```

## Table Operations

### Creating Tables with Various Column Types

```ruby
# db/migrate/002_create_products.rb
Sequel.migration do
  up do
    create_table(:products) do
      primary_key :id

      # String types
      String :name, null: false, size: 255
      String :sku, size: 50, unique: true
      Text :description

      # Numeric types
      Integer :stock_quantity, default: 0
      Decimal :price, size: [10, 2], null: false
      Float :weight

      # Date/time types
      Date :release_date
      DateTime :created_at, null: false
      DateTime :updated_at, null: false
      Time :daily_update_time

      # Boolean
      Boolean :active, default: true
      Boolean :featured, default: false

      # JSON (DuckDB-specific)
      column :metadata, 'JSON'
      column :tags, 'VARCHAR[]'  # Array type

      # Constraints
      constraint(:positive_price) { price > 0 }
      constraint(:valid_stock) { stock_quantity >= 0 }
    end
  end

  down do
    drop_table(:products)
  end
end
```

### Altering Tables

```ruby
# db/migrate/003_add_category_to_products.rb
Sequel.migration do
  up do
    alter_table(:products) do
      add_column :category_id, Integer
      add_column :brand, String, size: 100
      add_column :discontinued_at, DateTime

      # Add foreign key constraint
      add_foreign_key [:category_id], :categories, key: [:id]

      # Add index
      add_index :category_id
      add_index [:brand, :active]
    end
  end

  down do
    alter_table(:products) do
      drop_foreign_key [:category_id]
      drop_index [:brand, :active]
      drop_index :category_id
      drop_column :discontinued_at
      drop_column :brand
      drop_column :category_id
    end
  end
end
```

### Modifying Columns

```ruby
# db/migrate/004_modify_user_columns.rb
Sequel.migration do
  up do
    alter_table(:users) do
      # Change column type
      set_column_type :age, Integer

      # Change column default
      set_column_default :active, false

      # Add/remove null constraint
      set_column_allow_null :email, false

      # Rename column
      rename_column :name, :full_name
    end
  end

  down do
    alter_table(:users) do
      rename_column :full_name, :name
      set_column_allow_null :email, true
      set_column_default :active, true
      set_column_type :age, String
    end
  end
end
```

## Index Management

### Creating Indexes

```ruby
# db/migrate/005_add_indexes.rb
Sequel.migration do
  up do
    # Single column index
    add_index :users, :email

    # Multi-column index
    add_index :products, [:category_id, :active]

    # Unique index
    add_index :products, :sku, unique: true

    # Named index
    add_index :users, :created_at, name: :idx_users_created_at

    # Partial index (with WHERE clause)
    add_index :products, :name, where: { active: true }
  end

  down do
    drop_index :products, :name, where: { active: true }
    drop_index :users, :created_at, name: :idx_users_created_at
    drop_index :products, :sku, unique: true
    drop_index :products, [:category_id, :active]
    drop_index :users, :email
  end
end
```

## Constraints and Relationships

### Foreign Key Constraints

```ruby
# db/migrate/006_create_orders_with_relationships.rb
Sequel.migration do
  up do
    create_table(:categories) do
      primary_key :id
      String :name, null: false, unique: true
      String :description
      DateTime :created_at, null: false
    end

    create_table(:orders) do
      primary_key :id
      foreign_key :user_id, :users, null: false, on_delete: :cascade

      Decimal :total, size: [10, 2], null: false
      String :status, default: 'pending'
      DateTime :created_at, null: false
      DateTime :updated_at, null: false

      # Composite foreign key example
      # foreign_key [:user_id, :product_id], :user_products
    end

    create_table(:order_items) do
      primary_key :id
      foreign_key :order_id, :orders, null: false, on_delete: :cascade
      foreign_key :product_id, :products, null: false

      Integer :quantity, null: false, default: 1
      Decimal :unit_price, size: [10, 2], null: false

      # Ensure positive values
      constraint(:positive_quantity) { quantity > 0 }
      constraint(:positive_price) { unit_price > 0 }
    end
  end

  down do
    drop_table(:order_items)
    drop_table(:orders)
    drop_table(:categories)
  end
end
```

### Check Constraints

```ruby
# db/migrate/007_add_check_constraints.rb
Sequel.migration do
  up do
    alter_table(:users) do
      # Email format validation
      add_constraint(:valid_email) { email.like('%@%') }

      # Age range validation
      add_constraint(:valid_age) { (age >= 0) & (age <= 150) }
    end

    alter_table(:products) do
      # Price must be positive
      add_constraint(:positive_price) { price > 0 }

      # Stock quantity must be non-negative
      add_constraint(:non_negative_stock) { stock_quantity >= 0 }

      # SKU format validation (example pattern)
      add_constraint(:valid_sku_format) { sku.like('SKU-%') }
    end
  end

  down do
    alter_table(:products) do
      drop_constraint(:valid_sku_format)
      drop_constraint(:non_negative_stock)
      drop_constraint(:positive_price)
    end

    alter_table(:users) do
      drop_constraint(:valid_age)
      drop_constraint(:valid_email)
    end
  end
end
```

## DuckDB-Specific Features

### JSON and Array Columns

```ruby
# db/migrate/008_add_json_and_array_columns.rb
Sequel.migration do
  up do
    alter_table(:products) do
      # JSON column for flexible metadata
      add_column :specifications, 'JSON'

      # Array columns
      add_column :tags, 'VARCHAR[]'
      add_column :category_path, 'INTEGER[]'

      # Map column (key-value pairs)
      add_column :attributes, 'MAP(VARCHAR, VARCHAR)'
    end

    # Example of populating JSON data
    run <<~SQL
      UPDATE products
      SET specifications = '{"weight": "1.5kg", "dimensions": "10x20x5cm"}'
      WHERE specifications IS NULL
    SQL

    # Example of populating array data
    run <<~SQL
      UPDATE products
      SET tags = ARRAY['electronics', 'gadget']
      WHERE tags IS NULL
    SQL
  end

  down do
    alter_table(:products) do
      drop_column :attributes
      drop_column :category_path
      drop_column :tags
      drop_column :specifications
    end
  end
end
```

### Views and Materialized Views

```ruby
# db/migrate/009_create_views.rb
Sequel.migration do
  up do
    # Create a view for active products with category info
    run <<~SQL
      CREATE VIEW active_products_view AS
      SELECT
        p.id,
        p.name,
        p.price,
        p.stock_quantity,
        c.name as category_name
      FROM products p
      LEFT JOIN categories c ON p.category_id = c.id
      WHERE p.active = true
    SQL

    # Create a view for order summaries
    run <<~SQL
      CREATE VIEW order_summaries AS
      SELECT
        o.id as order_id,
        u.full_name as customer_name,
        o.total,
        o.status,
        COUNT(oi.id) as item_count,
        o.created_at
      FROM orders o
      JOIN users u ON o.user_id = u.id
      LEFT JOIN order_items oi ON o.id = oi.order_id
      GROUP BY o.id, u.full_name, o.total, o.status, o.created_at
    SQL
  end

  down do
    run "DROP VIEW IF EXISTS order_summaries"
    run "DROP VIEW IF EXISTS active_products_view"
  end
end
```

### Sequences (Alternative to Auto-increment)

```ruby
# db/migrate/010_create_sequences.rb
Sequel.migration do
  up do
    # Create custom sequence for order numbers
    run "CREATE SEQUENCE order_number_seq START 1000"

    alter_table(:orders) do
      add_column :order_number, String, unique: true
    end

    # Set default to use sequence
    run <<~SQL
      UPDATE orders
      SET order_number = 'ORD-' || LPAD(nextval('order_number_seq')::TEXT, 6, '0')
      WHERE order_number IS NULL
    SQL
  end

  down do
    alter_table(:orders) do
      drop_column :order_number
    end

    run "DROP SEQUENCE IF EXISTS order_number_seq"
  end
end
```

## Data Migration Patterns

### Populating Initial Data

```ruby
# db/migrate/011_populate_initial_data.rb
Sequel.migration do
  up do
    # Insert default categories
    categories_data = [
      { name: 'Electronics', description: 'Electronic devices and gadgets' },
      { name: 'Books', description: 'Books and publications' },
      { name: 'Clothing', description: 'Apparel and accessories' }
    ]

    categories_data.each do |category|
      run <<~SQL
        INSERT INTO categories (name, description, created_at)
        VALUES ('#{category[:name]}', '#{category[:description]}', NOW())
      SQL
    end

    # Or use Sequel's dataset methods
    # self[:categories].multi_insert(categories_data.map { |c| c.merge(created_at: Time.now) })
  end

  down do
    run "DELETE FROM categories WHERE name IN ('Electronics', 'Books', 'Clothing')"
  end
end
```

### Data Transformation

```ruby
# db/migrate/012_transform_user_data.rb
Sequel.migration do
  up do
    # Split full_name into first_name and last_name
    alter_table(:users) do
      add_column :first_name, String, size: 100
      add_column :last_name, String, size: 100
    end

    # Transform existing data
    run <<~SQL
      UPDATE users
      SET
        first_name = SPLIT_PART(full_name, ' ', 1),
        last_name = CASE
          WHEN ARRAY_LENGTH(STRING_SPLIT(full_name, ' ')) > 1
          THEN ARRAY_TO_STRING(ARRAY_SLICE(STRING_SPLIT(full_name, ' '), 2, NULL), ' ')
          ELSE ''
        END
      WHERE full_name IS NOT NULL
    SQL
  end

  down do
    alter_table(:users) do
      drop_column :last_name
      drop_column :first_name
    end
  end
end
```

## Performance Optimization Migrations

### Adding Indexes for Query Performance

```ruby
# db/migrate/013_optimize_query_performance.rb
Sequel.migration do
  up do
    # Index for common WHERE clauses
    add_index :orders, [:status, :created_at]
    add_index :products, [:active, :category_id, :price]

    # Index for JOIN operations
    add_index :order_items, [:order_id, :product_id]

    # Index for sorting operations
    add_index :users, [:created_at, :id]  # Composite for pagination

    # Partial indexes for common filtered queries
    add_index :products, :price, where: { active: true }
    add_index :orders, :created_at, where: { status: 'completed' }
  end

  down do
    drop_index :orders, :created_at, where: { status: 'completed' }
    drop_index :products, :price, where: { active: true }
    drop_index :users, [:created_at, :id]
    drop_index :order_items, [:order_id, :product_id]
    drop_index :products, [:active, :category_id, :price]
    drop_index :orders, [:status, :created_at]
  end
end
```

## Migration Best Practices

### 1. Reversible Migrations

Always provide both `up` and `down` methods:

```ruby
Sequel.migration do
  up do
    # Forward migration
    create_table(:example) do
      primary_key :id
      String :name
    end
  end

  down do
    # Reverse migration
    drop_table(:example)
  end
end
```

### 2. Safe Column Additions

When adding columns with NOT NULL constraints:

```ruby
Sequel.migration do
  up do
    # Step 1: Add column as nullable
    alter_table(:users) do
      add_column :phone, String
    end

    # Step 2: Populate with default values
    run "UPDATE users SET phone = 'N/A' WHERE phone IS NULL"

    # Step 3: Make it NOT NULL
    alter_table(:users) do
      set_column_allow_null :phone, false
    end
  end

  down do
    alter_table(:users) do
      drop_column :phone
    end
  end
end
```

### 3. Large Data Migrations

For large datasets, use batched operations:

```ruby
Sequel.migration do
  up do
    # Process in batches to avoid memory issues
    batch_size = 1000
    offset = 0

    loop do
      batch_processed = run <<~SQL
        UPDATE products
        SET updated_at = NOW()
        WHERE id IN (
          SELECT id FROM products
          WHERE updated_at IS NULL
          LIMIT #{batch_size} OFFSET #{offset}
        )
      SQL

      break if batch_processed == 0
      offset += batch_size
    end
  end

  down do
    # Reverse operation if needed
  end
end
```

### 4. Testing Migrations

```ruby
# test/migration_test.rb
require_relative 'spec_helper'

class MigrationTest < SequelDuckDBTest::TestCase
  def test_migration_001_creates_users_table
    # Run specific migration
    Sequel::Migrator.run(@db, 'db/migrate', target: 1)

    # Verify table exists
    assert @db.table_exists?(:users)

    # Verify schema
    schema = @db.schema(:users)
    assert schema.any? { |col| col[0] == :id && col[1][:primary_key] }
    assert schema.any? { |col| col[0] == :name && !col[1][:allow_null] }
  end

  def test_migration_rollback
    # Run migration
    Sequel::Migrator.run(@db, 'db/migrate', target: 1)
    assert @db.table_exists?(:users)

    # Rollback
    Sequel::Migrator.run(@db, 'db/migrate', target: 0)
    refute @db.table_exists?(:users)
  end
end
```

## Migration Runner Script

Create a script to manage migrations:

```ruby
#!/usr/bin/env ruby
# bin/migrate

require 'sequel'
require_relative '../lib/sequel/duckdb'

# Configuration
DB_URL = ENV['DATABASE_URL'] || 'duckdb:///db/development.duckdb'
MIGRATIONS_DIR = File.expand_path('../db/migrate', __dir__)

# Connect to database
db = Sequel.connect(DB_URL)

# Parse command line arguments
command = ARGV[0]
target = ARGV[1]&.to_i

case command
when 'up', nil
  puts "Running migrations..."
  if target
    Sequel::Migrator.run(db, MIGRATIONS_DIR, target: target)
    puts "Migrated to version #{target}"
  else
    Sequel::Migrator.run(db, MIGRATIONS_DIR)
    puts "Migrated to latest version"
  end

when 'down'
  target ||= 0
  puts "Rolling back to version #{target}..."
  Sequel::Migrator.run(db, MIGRATIONS_DIR, target: target)
  puts "Rolled back to version #{target}"

when 'version'
  version = db[:schema_info].first[:version] rescue 0
  puts "Current migration version: #{version}"

when 'create'
  name = ARGV[1]
  unless name
    puts "Usage: bin/migrate create migration_name"
    exit 1
  end

  # Find next migration number
  existing = Dir[File.join(MIGRATIONS_DIR, '*.rb')]
  next_num = existing.map { |f| File.basename(f)[/^\d+/].to_i }.max.to_i + 1

  # Create migration file
  filename = format('%03d_%s.rb', next_num, name)
  filepath = File.join(MIGRATIONS_DIR, filename)

  File.write(filepath, <<~RUBY)
    Sequel.migration do
      up do
        # Add your migration code here
      end

      down do
        # Add your rollback code here
      end
    end
  RUBY

  puts "Created migration: #{filepath}"

else
  puts <<~USAGE
    Usage: bin/migrate [command] [options]

    Commands:
      up [version]    - Run migrations up to specified version (or latest)
      down [version]  - Roll back to specified version (default: 0)
      version         - Show current migration version
      create <name>   - Create a new migration file

    Examples:
      bin/migrate                    # Run all pending migrations
      bin/migrate up 5               # Migrate to version 5
      bin/migrate down 3             # Roll back to version 3
      bin/migrate version            # Show current version
      bin/migrate create add_users   # Create new migration
  USAGE
end
```

Make the script executable:

```bash
chmod +x bin/migrate
```

## Usage Examples

```bash
# Run all pending migrations
./bin/migrate

# Migrate to specific version
./bin/migrate up 5

# Roll back to previous version
./bin/migrate down 4

# Check current version
./bin/migrate version

# Create new migration
./bin/migrate create add_user_preferences
```

This comprehensive guide covers the most common migration patterns and DuckDB-specific considerations when using Sequel migrations. Remember to always test your migrations thoroughly and keep them reversible for safe deployment practices.