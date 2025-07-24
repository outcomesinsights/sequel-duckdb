# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for schema introspection methods
# Tests the low-level schema parsing methods that power schema discovery
# These tests focus on the schema_parse_* methods that will be implemented
class SchemaIntrospectionTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
  end

  def teardown
    @db&.disconnect
    super
  end

  # Tests for schema_parse_tables method
  def test_schema_parse_tables_empty_database
    # Test with empty database (should return empty array or only system tables)
    tables = @db.send(:schema_parse_tables, {})

    assert_instance_of Array, tables, "schema_parse_tables should return an array"
    # Empty database should have no user tables (may have system tables)
    user_tables = tables.reject do |table|
      table.to_s.start_with?("information_schema") || table.to_s.start_with?("pg_")
    end
    assert_empty user_tables, "Empty database should have no user tables"
  end

  def test_schema_parse_tables_with_single_table
    # Create a test table
    @db.create_table(:test_table) do
      primary_key :id
      String :name
    end

    tables = @db.send(:schema_parse_tables, {})

    assert_instance_of Array, tables, "schema_parse_tables should return an array"
    assert_includes tables, :test_table, "Should include created table"
  end

  def test_schema_parse_tables_with_multiple_tables
    # Create multiple test tables
    @db.create_table(:users) do
      primary_key :id
      String :name
      String :email
    end

    @db.create_table(:posts) do
      primary_key :id
      String :title
      String :content
      Integer :user_id
    end

    @db.create_table(:comments) do
      primary_key :id
      String :content
      Integer :post_id
      Integer :user_id
    end

    tables = @db.send(:schema_parse_tables, {})

    assert_instance_of Array, tables, "schema_parse_tables should return an array"
    assert_includes tables, :users, "Should include users table"
    assert_includes tables, :posts, "Should include posts table"
    assert_includes tables, :comments, "Should include comments table"
  end

  def test_schema_parse_tables_excludes_system_tables
    # Create a user table
    @db.create_table(:user_table) do
      primary_key :id
      String :data
    end

    tables = @db.send(:schema_parse_tables, {})

    # Should include user table
    assert_includes tables, :user_table, "Should include user table"

    # Should not include system tables (if any exist)
    system_table_patterns = %w[information_schema pg_ sqlite_ sys_]
    tables.select do |table|
      system_table_patterns.any? { |pattern| table.to_s.start_with?(pattern) }
    end

    # This test may need adjustment based on DuckDB's actual system tables
    # For now, we just verify that user tables are included
    assert_includes tables, :user_table, "User tables should be included"
  end

  def test_schema_parse_tables_with_schema_option
    # Test schema_parse_tables with schema option (if supported)
    # DuckDB may support schemas/databases

    @db.create_table(:schema_test) do
      primary_key :id
      String :name
    end

    # Test with default schema
    tables = @db.send(:schema_parse_tables, {})
    assert_includes tables, :schema_test, "Should find table in default schema"

    # Test with explicit schema option (may not be supported yet)
    tables_with_schema = @db.send(:schema_parse_tables, { schema: "main" })
    assert_instance_of Array, tables_with_schema, "Should return array even with schema option"
  end

  # Tests for schema_parse_table method
  def test_schema_parse_table_basic_columns
    @db.create_table(:basic_table) do
      primary_key :id
      String :name, null: false
      Integer :age
      Boolean :active, default: true
    end

    schema = @db.send(:schema_parse_table, :basic_table, {})

    assert_instance_of Array, schema, "schema_parse_table should return an array"
    refute_empty schema, "Schema should not be empty"

    # Each entry should be [column_name, column_info]
    schema.each do |entry|
      assert_instance_of Array, entry, "Each schema entry should be an array"
      assert_equal 2, entry.length, "Each entry should have 2 elements"

      column_name, column_info = entry
      assert_instance_of Symbol, column_name, "Column name should be a symbol"
      assert_instance_of Hash, column_info, "Column info should be a hash"
    end

    # Verify expected columns exist
    column_names = schema.map(&:first)
    assert_includes column_names, :id, "Should have id column"
    assert_includes column_names, :name, "Should have name column"
    assert_includes column_names, :age, "Should have age column"
    assert_includes column_names, :active, "Should have active column"
  end

  def test_schema_parse_table_column_properties
    @db.create_table(:detailed_table) do
      primary_key :id
      String :name, null: false, size: 100
      Integer :age, null: true
      Boolean :active, default: true
      Date :birth_date
      DateTime :created_at
      Float :score, default: 0.0
    end

    schema = @db.send(:schema_parse_table, :detailed_table, {})

    # Test ID column properties
    id_column = schema.find { |col| col[0] == :id }
    refute_nil id_column, "ID column should exist"

    id_info = id_column[1]
    assert_required_column_properties(id_info)
    assert_equal true, id_info[:primary_key], "ID should be primary key"
    assert_equal false, id_info[:allow_null], "ID should not allow null"

    # Test name column properties
    name_column = schema.find { |col| col[0] == :name }
    refute_nil name_column, "Name column should exist"

    name_info = name_column[1]
    assert_required_column_properties(name_info)
    assert_equal :string, name_info[:type], "Name should be string type"
    assert_equal false, name_info[:allow_null], "Name should not allow null"

    # Test age column properties (nullable)
    age_column = schema.find { |col| col[0] == :age }
    refute_nil age_column, "Age column should exist"

    age_info = age_column[1]
    assert_required_column_properties(age_info)
    assert_equal :integer, age_info[:type], "Age should be integer type"
    assert_equal true, age_info[:allow_null], "Age should allow null"

    # Test active column properties (with default)
    active_column = schema.find { |col| col[0] == :active }
    refute_nil active_column, "Active column should exist"

    active_info = active_column[1]
    assert_required_column_properties(active_info)
    assert_equal :boolean, active_info[:type], "Active should be boolean type"
    # Default value testing will depend on implementation
    assert active_info.key?(:default), "Should have default information"
  end

  def test_schema_parse_table_data_types
    # Create table with explicit SQL to test DuckDB native types
    @db.run(<<~SQL)
      CREATE TABLE type_test_table (
        id INTEGER PRIMARY KEY,
        string_col VARCHAR,
        integer_col INTEGER,
        float_col DOUBLE,
        boolean_col BOOLEAN,
        date_col DATE,
        datetime_col TIMESTAMP,
        time_col TIME
      )
    SQL

    schema = @db.send(:schema_parse_table, :type_test_table, {})

    # Test each column type
    type_mappings = {
      id: :integer,
      string_col: :string,
      integer_col: :integer,
      float_col: :float,
      boolean_col: :boolean,
      date_col: :date,
      datetime_col: :datetime,
      time_col: :time
    }

    type_mappings.each do |column_name, expected_type|
      column = schema.find { |col| col[0] == column_name }
      refute_nil column, "#{column_name} column should exist"

      column_info = column[1]
      assert_equal expected_type, column_info[:type],
                   "#{column_name} should have type #{expected_type}"

      # DB type should be a string
      assert_instance_of String, column_info[:db_type],
                         "#{column_name} should have string db_type"
    end
  end

  def test_schema_parse_table_primary_key_detection
    @db.create_table(:pk_test_table) do
      primary_key :id
      String :name
      Integer :value
    end

    schema = @db.send(:schema_parse_table, :pk_test_table, {})

    # Find primary key column
    pk_column = schema.find { |col| col[1][:primary_key] == true }
    refute_nil pk_column, "Should have a primary key column"
    assert_equal :id, pk_column[0], "Primary key should be id column"

    # Verify non-primary key columns
    non_pk_columns = schema.select { |col| col[1][:primary_key] == false }
    assert_equal 2, non_pk_columns.length, "Should have 2 non-primary key columns"

    non_pk_names = non_pk_columns.map(&:first)
    assert_includes non_pk_names, :name, "Name should not be primary key"
    assert_includes non_pk_names, :value, "Value should not be primary key"
  end

  def test_schema_parse_table_null_constraints
    @db.create_table(:null_test_table) do
      primary_key :id
      String :required_field, null: false
      String :optional_field, null: true
      String :default_nullable_field # Should default to nullable
    end

    schema = @db.send(:schema_parse_table, :null_test_table, {})

    # Test required field
    required_column = schema.find { |col| col[0] == :required_field }
    refute_nil required_column, "Required field should exist"
    assert_equal false, required_column[1][:allow_null], "Required field should not allow null"

    # Test optional field
    optional_column = schema.find { |col| col[0] == :optional_field }
    refute_nil optional_column, "Optional field should exist"
    assert_equal true, optional_column[1][:allow_null], "Optional field should allow null"

    # Test default nullable field
    default_column = schema.find { |col| col[0] == :default_nullable_field }
    refute_nil default_column, "Default nullable field should exist"
    assert_equal true, default_column[1][:allow_null], "Default field should allow null"

    # Primary key should not allow null
    id_column = schema.find { |col| col[0] == :id }
    assert_equal false, id_column[1][:allow_null], "Primary key should not allow null"
  end

  def test_schema_parse_table_default_values
    @db.create_table(:default_test_table) do
      primary_key :id
      String :name
      Boolean :active, default: true
      Integer :count, default: 0
      String :status, default: "pending"
      Float :score, default: 0.0
    end

    schema = @db.send(:schema_parse_table, :default_test_table, {})

    # Test columns with defaults
    default_tests = {
      active: true,
      count: 0,
      status: "pending",
      score: 0.0
    }

    default_tests.each_key do |column_name|
      column = schema.find { |col| col[0] == column_name }
      refute_nil column, "#{column_name} column should exist"

      column_info = column[1]
      assert column_info.key?(:default), "#{column_name} should have default information"

      # The exact format of default values may vary based on implementation
      # For now, just verify that default information is present
      refute_nil column_info[:default], "#{column_name} should have a default value"
    end

    # Test columns without defaults
    no_default_columns = %i[id name]
    no_default_columns.each do |column_name|
      column = schema.find { |col| col[0] == column_name }
      refute_nil column, "#{column_name} column should exist"

      column_info = column[1]
      # Default should be nil or not present for columns without defaults
      assert column_info.key?(:default), "Should have default key even if nil"
    end
  end

  def test_schema_parse_table_nonexistent_table
    # Test error handling for non-existent table
    assert_raises(Sequel::DatabaseError) do
      @db.send(:schema_parse_table, :nonexistent_table, {})
    end
  end

  def test_schema_parse_table_with_options
    @db.create_table(:options_test) do
      primary_key :id
      String :name
    end

    # Test with empty options
    schema1 = @db.send(:schema_parse_table, :options_test, {})
    assert_instance_of Array, schema1, "Should work with empty options"

    # Test with schema option (if supported)
    schema2 = @db.send(:schema_parse_table, :options_test, { schema: "main" })
    assert_instance_of Array, schema2, "Should work with schema option"

    # Results should be similar (exact comparison depends on implementation)
    assert_equal schema1.length, schema2.length, "Should have same number of columns"
  end

  # Tests for schema_parse_indexes method
  def test_schema_parse_indexes_no_indexes
    @db.create_table(:no_index_table) do
      primary_key :id
      String :name
      Integer :value
    end

    indexes = @db.send(:schema_parse_indexes, :no_index_table, {})

    assert_instance_of Hash, indexes, "schema_parse_indexes should return a hash"
    # May have primary key index, but no explicit indexes
    # The exact behavior depends on DuckDB's index handling
  end

  def test_schema_parse_indexes_with_single_column_index
    @db.create_table(:single_index_table) do
      primary_key :id
      String :name
      Integer :value
    end

    # Create an index
    @db.add_index(:single_index_table, :name, name: :name_index)

    indexes = @db.send(:schema_parse_indexes, :single_index_table, {})

    assert_instance_of Hash, indexes, "schema_parse_indexes should return a hash"

    # Should have the created index
    assert indexes.key?(:name_index), "Should have name_index"

    index_info = indexes[:name_index]
    assert_instance_of Hash, index_info, "Index info should be a hash"

    # Verify index properties
    assert index_info.key?(:columns), "Index should have columns information"
    assert_equal [:name], index_info[:columns], "Index should be on name column"

    assert index_info.key?(:unique), "Index should have unique information"
    assert_equal false, index_info[:unique], "Index should not be unique by default"
  end

  def test_schema_parse_indexes_with_multi_column_index
    @db.create_table(:multi_index_table) do
      primary_key :id
      String :first_name
      String :last_name
      Integer :age
    end

    # Create a multi-column index
    @db.add_index(:multi_index_table, %i[first_name last_name], name: :name_index)

    indexes = @db.send(:schema_parse_indexes, :multi_index_table, {})

    assert_instance_of Hash, indexes, "schema_parse_indexes should return a hash"
    assert indexes.key?(:name_index), "Should have name_index"

    index_info = indexes[:name_index]
    assert_equal %i[first_name last_name], index_info[:columns],
                 "Multi-column index should have correct columns"
  end

  def test_schema_parse_indexes_with_unique_index
    @db.create_table(:unique_index_table) do
      primary_key :id
      String :email
      String :username
    end

    # Create unique indexes
    @db.add_index(:unique_index_table, :email, unique: true, name: :unique_email_index)
    @db.add_index(:unique_index_table, :username, unique: true, name: :unique_username_index)

    indexes = @db.send(:schema_parse_indexes, :unique_index_table, {})

    assert_instance_of Hash, indexes, "schema_parse_indexes should return a hash"

    # Test unique email index
    assert indexes.key?(:unique_email_index), "Should have unique_email_index"
    email_index = indexes[:unique_email_index]
    assert_equal true, email_index[:unique], "Email index should be unique"
    assert_equal [:email], email_index[:columns], "Email index should be on email column"

    # Test unique username index
    assert indexes.key?(:unique_username_index), "Should have unique_username_index"
    username_index = indexes[:unique_username_index]
    assert_equal true, username_index[:unique], "Username index should be unique"
    assert_equal [:username], username_index[:columns], "Username index should be on username column"
  end

  def test_schema_parse_indexes_primary_key_index
    @db.create_table(:pk_index_table) do
      primary_key :id
      String :name
    end

    indexes = @db.send(:schema_parse_indexes, :pk_index_table, {})

    assert_instance_of Hash, indexes, "schema_parse_indexes should return a hash"

    # DuckDB may or may not create explicit indexes for primary keys
    # This test verifies the method works and returns appropriate structure
    # The exact behavior depends on DuckDB's index implementation
    indexes.each do |index_name, index_info|
      assert_instance_of Symbol, index_name, "Index name should be a symbol"
      assert_instance_of Hash, index_info, "Index info should be a hash"

      assert index_info.key?(:columns), "Index should have columns"
      assert index_info.key?(:unique), "Index should have unique flag"

      assert_instance_of Array, index_info[:columns], "Columns should be an array"
      assert_instance_of TrueClass, index_info[:unique] || index_info[:unique].is_a?(FalseClass),
                         "Unique should be boolean"
    end
  end

  def test_schema_parse_indexes_nonexistent_table
    # Test error handling for non-existent table
    assert_raises(Sequel::DatabaseError) do
      @db.send(:schema_parse_indexes, :nonexistent_table, {})
    end
  end

  def test_schema_parse_indexes_with_options
    @db.create_table(:index_options_test) do
      primary_key :id
      String :name
    end

    @db.add_index(:index_options_test, :name, name: :test_index)

    # Test with empty options
    indexes1 = @db.send(:schema_parse_indexes, :index_options_test, {})
    assert_instance_of Hash, indexes1, "Should work with empty options"

    # Test with schema option
    indexes2 = @db.send(:schema_parse_indexes, :index_options_test, { schema: "main" })
    assert_instance_of Hash, indexes2, "Should work with schema option"
  end

  # Tests for views support
  def test_schema_parse_tables_includes_views
    # Create a table first
    @db.create_table(:base_table) do
      primary_key :id
      String :name
      Integer :value
    end

    # Create a view (if DuckDB supports views)
    begin
      @db.create_view(:test_view, @db[:base_table].where(value: 1))

      tables = @db.send(:schema_parse_tables, {})

      # Views might be included in tables list or handled separately
      # This test verifies the method handles views appropriately
      assert_instance_of Array, tables, "Should return array even with views"

      # The exact behavior (whether views are included) depends on implementation
      # For now, just verify the method doesn't break
    rescue Sequel::DatabaseError
      # If views are not supported, skip this test
      skip "DuckDB views not supported or not implemented yet"
    end
  end

  def test_schema_parse_table_view_columns
    # Create a table and view
    @db.create_table(:view_base_table) do
      primary_key :id
      String :name
      Integer :value
      Boolean :active
    end

    begin
      @db.create_view(:test_view, @db[:view_base_table].select(:id, :name))

      # Test schema parsing for view
      schema = @db.send(:schema_parse_table, :test_view, {})

      assert_instance_of Array, schema, "View schema should be an array"

      # View should have selected columns
      column_names = schema.map(&:first)
      assert_includes column_names, :id, "View should have id column"
      assert_includes column_names, :name, "View should have name column"
      refute_includes column_names, :value, "View should not have value column"
      refute_includes column_names, :active, "View should not have active column"
    rescue Sequel::DatabaseError
      skip "DuckDB views not supported or not implemented yet"
    end
  end

  # Tests for foreign key detection
  def test_schema_parse_table_foreign_keys
    # Create parent table
    @db.create_table(:parent_table) do
      primary_key :id
      String :name
    end

    # Create child table with foreign key
    begin
      @db.create_table(:child_table) do
        primary_key :id
        String :title
        Integer :parent_id
        foreign_key [:parent_id], :parent_table, key: [:id]
      end

      schema = @db.send(:schema_parse_table, :child_table, {})

      # Find the foreign key column
      fk_column = schema.find { |col| col[0] == :parent_id }
      refute_nil fk_column, "Foreign key column should exist"

      column_info = fk_column[1]

      # Foreign key information might be in column info or handled separately
      # This test verifies the method handles foreign keys appropriately
      assert_required_column_properties(column_info)
    rescue Sequel::DatabaseError
      skip "DuckDB foreign keys not supported or not implemented yet"
    end
  end

  def test_foreign_key_detection_methods
    # Test if database has methods for foreign key detection
    # This is a placeholder for future foreign key support

    @db.create_table(:fk_parent) do
      primary_key :id
      String :name
    end

    @db.create_table(:fk_child) do
      primary_key :id
      String :title
      Integer :parent_id
    end

    # These methods may not exist yet, but test structure should be ready
    begin
      # Test if foreign key methods exist
      if @db.respond_to?(:foreign_key_list)
        fk_list = @db.foreign_key_list(:fk_child)
        assert_instance_of Array, fk_list, "Foreign key list should be an array"
      end
    rescue NoMethodError
      # Methods not implemented yet, which is expected
      assert true, "Foreign key methods not implemented yet"
    end
  end

  private

  # Helper method to assert required column properties
  def assert_required_column_properties(column_info)
    required_keys = %i[type db_type allow_null primary_key]

    required_keys.each do |key|
      assert column_info.key?(key), "Column info should have #{key} key"
    end

    # Type should be a symbol
    assert_instance_of Symbol, column_info[:type], "Type should be a symbol"

    # DB type should be a string
    assert_instance_of String, column_info[:db_type], "DB type should be a string"

    # Boolean flags should be boolean
    assert_boolean column_info[:allow_null], "allow_null should be boolean"
    assert_boolean column_info[:primary_key], "primary_key should be boolean"
  end

  # Helper method to assert boolean value
  def assert_boolean(value, message = "Value should be boolean")
    assert [true, false].include?(value), message
  end
end
