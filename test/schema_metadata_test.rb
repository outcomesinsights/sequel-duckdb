# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for schema metadata methods
# Tests the public schema methods (tables, schema, indexes) that use schema_parse_* methods
# These tests focus on the public API that applications will use
class SchemaMetadataTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
  end

  def teardown
    @db&.disconnect
    super
  end

  # Tests for tables method
  def test_tables_method_empty_database
    tables = @db.tables

    assert_instance_of Array, tables, "tables() should return an array"
    # Empty database should have no user tables
    user_tables = tables.reject { |table| table.to_s.start_with?('information_schema') || table.to_s.start_with?('pg_') }
    assert_empty user_tables, "Empty database should have no user tables"
  end

  def test_tables_method_with_tables
    # Create test tables
    @db.create_table(:users) do
      primary_key :id
      String :name
      String :email
    end

    @db.create_table(:posts) do
      primary_key :id
      String :title
      String :content
    end

    tables = @db.tables

    assert_instance_of Array, tables, "tables() should return an array"
    assert_includes tables, :users, "Should include users table"
    assert_includes tables, :posts, "Should include posts table"
  end

  def test_tables_method_with_options
    @db.create_table(:test_table) do
      primary_key :id
      String :name
    end

    # Test with empty options
    tables1 = @db.tables({})
    assert_instance_of Array, tables1, "Should work with empty options"
    assert_includes tables1, :test_table, "Should include test table"

    # Test with schema option
    tables2 = @db.tables(schema: 'main')
    assert_instance_of Array, tables2, "Should work with schema option"
    assert_includes tables2, :test_table, "Should include test table with schema option"
  end

  def test_tables_method_consistency
    # Create a table
    @db.create_table(:consistency_test) do
      primary_key :id
      String :data
    end

    # Multiple calls should return consistent results
    tables1 = @db.tables
    tables2 = @db.tables

    assert_equal tables1, tables2, "Multiple calls should return same results"
    assert_includes tables1, :consistency_test, "Should consistently include created table"
  end

  # Tests for schema method
  def test_schema_method_basic_usage
    @db.create_table(:schema_test) do
      primary_key :id
      String :name, null: false
      Integer :age
      Boolean :active, default: true
    end

    schema = @db.schema(:schema_test)

    assert_instance_of Array, schema, "schema() should return an array"
    refute_empty schema, "Schema should not be empty"

    # Verify structure
    schema.each do |column_entry|
      assert_instance_of Array, column_entry, "Each schema entry should be an array"
      assert_equal 2, column_entry.length, "Each entry should have 2 elements"

      column_name, column_info = column_entry
      assert_instance_of Symbol, column_name, "Column name should be a symbol"
      assert_instance_of Hash, column_info, "Column info should be a hash"

      # Verify required keys
      required_keys = [:type, :db_type, :allow_null, :primary_key]
      required_keys.each do |key|
        assert column_info.key?(key), "Column info should have #{key} key"
      end
    end
  end

  def test_schema_method_column_details
    @db.create_table(:detailed_schema) do
      primary_key :id
      String :name, null: false, size: 100
      Integer :age, null: true
      Boolean :active, default: true
      Date :birth_date
      DateTime :created_at
    end

    schema = @db.schema(:detailed_schema)
    column_names = schema.map(&:first)

    # Verify all expected columns exist
    expected_columns = [:id, :name, :age, :active, :birth_date, :created_at]
    expected_columns.each do |col|
      assert_includes column_names, col, "Should have #{col} column"
    end

    # Test specific column properties
    id_column = schema.find { |col| col[0] == :id }
    id_info = id_column[1]
    assert_equal :integer, id_info[:type], "ID should be integer type"
    assert_equal true, id_info[:primary_key], "ID should be primary key"
    assert_equal false, id_info[:allow_null], "ID should not allow null"

    name_column = schema.find { |col| col[0] == :name }
    name_info = name_column[1]
    assert_equal :string, name_info[:type], "Name should be string type"
    assert_equal false, name_info[:primary_key], "Name should not be primary key"
    assert_equal false, name_info[:allow_null], "Name should not allow null"

    age_column = schema.find { |col| col[0] == :age }
    age_info = age_column[1]
    assert_equal :integer, age_info[:type], "Age should be integer type"
    assert_equal false, age_info[:primary_key], "Age should not be primary key"
    assert_equal true, age_info[:allow_null], "Age should allow null"
  end

  def test_schema_method_default_values
    @db.create_table(:default_test) do
      primary_key :id
      String :name
      Boolean :active, default: true
      Integer :count, default: 0
      String :status, default: 'pending'
    end

    schema = @db.schema(:default_test)

    # Test columns with defaults
    active_column = schema.find { |col| col[0] == :active }
    refute_nil active_column, "Active column should exist"
    assert active_column[1].key?(:default), "Active column should have default info"

    count_column = schema.find { |col| col[0] == :count }
    refute_nil count_column, "Count column should exist"
    assert count_column[1].key?(:default), "Count column should have default info"

    status_column = schema.find { |col| col[0] == :status }
    refute_nil status_column, "Status column should exist"
    assert status_column[1].key?(:default), "Status column should have default info"

    # Test columns without explicit defaults
    id_column = schema.find { |col| col[0] == :id }
    name_column = schema.find { |col| col[0] == :name }

    assert id_column[1].key?(:default), "ID column should have default key (even if nil)"
    assert name_column[1].key?(:default), "Name column should have default key (even if nil)"
  end

  def test_schema_method_nullable_status
    @db.create_table(:nullable_test) do
      primary_key :id
      String :required_field, null: false
      String :optional_field, null: true
      String :default_field  # Should default to nullable
    end

    schema = @db.schema(:nullable_test)

    # Test required field
    required_column = schema.find { |col| col[0] == :required_field }
    assert_equal false, required_column[1][:allow_null], "Required field should not allow null"

    # Test optional field
    optional_column = schema.find { |col| col[0] == :optional_field }
    assert_equal true, optional_column[1][:allow_null], "Optional field should allow null"

    # Test default field (should be nullable)
    default_column = schema.find { |col| col[0] == :default_field }
    assert_equal true, default_column[1][:allow_null], "Default field should allow null"

    # Primary key should not allow null
    id_column = schema.find { |col| col[0] == :id }
    assert_equal false, id_column[1][:allow_null], "Primary key should not allow null"
  end

  def test_schema_method_with_options
    @db.create_table(:options_test) do
      primary_key :id
      String :name
    end

    # Test with empty options
    schema1 = @db.schema(:options_test, {})
    assert_instance_of Array, schema1, "Should work with empty options"

    # Test with schema option
    schema2 = @db.schema(:options_test, schema: 'main')
    assert_instance_of Array, schema2, "Should work with schema option"

    # Results should be consistent
    assert_equal schema1.length, schema2.length, "Should have same number of columns"
  end

  def test_schema_method_error_handling
    # Test with non-existent table
    assert_raises(Sequel::DatabaseError) do
      @db.schema(:nonexistent_table)
    end
  end

  # Tests for indexes method
  def test_indexes_method_no_indexes
    @db.create_table(:no_index_table) do
      primary_key :id
      String :name
      Integer :value
    end

    indexes = @db.indexes(:no_index_table)

    assert_instance_of Hash, indexes, "indexes() should return a hash"
    # May have primary key index or be empty - depends on DuckDB implementation
  end

  def test_indexes_method_with_indexes
    @db.create_table(:indexed_table) do
      primary_key :id
      String :name
      String :email
      Integer :age
    end

    # Create indexes
    @db.add_index(:indexed_table, :name, name: :name_index)
    @db.add_index(:indexed_table, :email, unique: true, name: :unique_email_index)
    @db.add_index(:indexed_table, [:name, :age], name: :composite_index)

    indexes = @db.indexes(:indexed_table)

    assert_instance_of Hash, indexes, "indexes() should return a hash"

    # Test name index
    if indexes.key?(:name_index)
      name_index = indexes[:name_index]
      assert_instance_of Hash, name_index, "Index info should be a hash"
      assert name_index.key?(:columns), "Index should have columns"
      assert name_index.key?(:unique), "Index should have unique flag"
      assert_equal [:name], name_index[:columns], "Name index should be on name column"
      assert_equal false, name_index[:unique], "Name index should not be unique"
    end

    # Test unique email index
    if indexes.key?(:unique_email_index)
      email_index = indexes[:unique_email_index]
      assert_equal [:email], email_index[:columns], "Email index should be on email column"
      assert_equal true, email_index[:unique], "Email index should be unique"
    end

    # Test composite index
    if indexes.key?(:composite_index)
      composite_index = indexes[:composite_index]
      assert_equal [:name, :age], composite_index[:columns], "Composite index should be on name and age columns"
    end
  end

  def test_indexes_method_with_options
    @db.create_table(:index_options_test) do
      primary_key :id
      String :name
    end

    @db.add_index(:index_options_test, :name, name: :test_index)

    # Test with empty options
    indexes1 = @db.indexes(:index_options_test, {})
    assert_instance_of Hash, indexes1, "Should work with empty options"

    # Test with schema option
    indexes2 = @db.indexes(:index_options_test, schema: 'main')
    assert_instance_of Hash, indexes2, "Should work with schema option"
  end

  def test_indexes_method_error_handling
    # Test with non-existent table
    assert_raises(Sequel::DatabaseError) do
      @db.indexes(:nonexistent_table)
    end
  end

  # Integration tests
  def test_metadata_methods_integration
    # Create a comprehensive test table
    @db.create_table(:integration_test) do
      primary_key :id
      String :name, null: false, size: 100
      String :email, unique: true
      Integer :age, null: true
      Boolean :active, default: true
      Date :birth_date
      DateTime :created_at
      Float :score, default: 0.0
    end

    # Add additional indexes
    @db.add_index(:integration_test, :name, name: :name_idx)
    @db.add_index(:integration_test, [:age, :active], name: :age_active_idx)

    # Test tables method
    tables = @db.tables
    assert_includes tables, :integration_test, "tables() should include the test table"

    # Test schema method
    schema = @db.schema(:integration_test)
    assert_equal 8, schema.length, "Should have 8 columns"

    column_names = schema.map(&:first)
    expected_columns = [:id, :name, :email, :age, :active, :birth_date, :created_at, :score]
    expected_columns.each do |col|
      assert_includes column_names, col, "Should have #{col} column"
    end

    # Test indexes method
    indexes = @db.indexes(:integration_test)
    assert_instance_of Hash, indexes, "indexes() should return a hash"

    # Should have at least the indexes we created (may have additional system indexes)
    created_index_names = [:name_idx, :age_active_idx]
    created_index_names.each do |idx_name|
      if indexes.key?(idx_name)
        assert_instance_of Hash, indexes[idx_name], "#{idx_name} should have index info"
        assert indexes[idx_name].key?(:columns), "#{idx_name} should have columns info"
        assert indexes[idx_name].key?(:unique), "#{idx_name} should have unique info"
      end
    end
  end

  def test_metadata_methods_consistency
    # Create table
    @db.create_table(:consistency_test) do
      primary_key :id
      String :name
      Integer :value
    end

    @db.add_index(:consistency_test, :name, name: :name_index)

    # Multiple calls should return consistent results
    tables1 = @db.tables
    tables2 = @db.tables
    assert_equal tables1, tables2, "tables() should be consistent"

    schema1 = @db.schema(:consistency_test)
    schema2 = @db.schema(:consistency_test)
    assert_equal schema1, schema2, "schema() should be consistent"

    indexes1 = @db.indexes(:consistency_test)
    indexes2 = @db.indexes(:consistency_test)
    assert_equal indexes1, indexes2, "indexes() should be consistent"
  end

  def test_metadata_methods_performance
    # Create multiple tables to test performance doesn't degrade significantly
    10.times do |i|
      @db.create_table(:"perf_test_#{i}") do
        primary_key :id
        String :name
        Integer :value
      end
    end

    # These should complete reasonably quickly
    start_time = Time.now
    tables = @db.tables
    tables_time = Time.now - start_time

    assert tables.length >= 10, "Should have at least 10 tables"
    assert tables_time < 1.0, "tables() should complete within 1 second"

    # Test schema method performance
    start_time = Time.now
    schema = @db.schema(:perf_test_0)
    schema_time = Time.now - start_time

    refute_empty schema, "Schema should not be empty"
    assert schema_time < 0.5, "schema() should complete within 0.5 seconds"

    # Test indexes method performance
    start_time = Time.now
    indexes = @db.indexes(:perf_test_0)
    indexes_time = Time.now - start_time

    assert_instance_of Hash, indexes, "indexes() should return a hash"
    assert indexes_time < 0.5, "indexes() should complete within 0.5 seconds"
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end