# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for schema operations and introspection
# Tests table creation, modification, and schema discovery functionality
class SchemaTest < SequelDuckDBTest::TestCase
  def test_table_creation
    db = create_db

    assert_nothing_raised("Should be able to create table") do
      db.create_table(:users) do
        primary_key :id
        String :name, null: false
        Integer :age
        Boolean :active, default: true
      end
    end

    assert_table_exists(db, :users)
  end

  def test_table_creation_with_various_column_types
    db = create_db

    assert_nothing_raised("Should support various column types") do
      db.create_table(:test_types) do
        primary_key :id
        String :text_col
        Integer :int_col
        Float :float_col
        Boolean :bool_col
        Date :date_col
        DateTime :datetime_col
        Time :time_col
        # NOTE: BLOB/binary support will be tested when implemented
      end
    end

    assert_table_exists(db, :test_types)
  end

  def test_table_creation_with_constraints
    db = create_db

    assert_nothing_raised("Should support column constraints") do
      db.create_table(:constrained_table) do
        primary_key :id
        String :name, null: false, unique: true
        Integer :age, null: false
        String :email, unique: true
        Boolean :active, default: true
      end
    end

    assert_table_exists(db, :constrained_table)
  end

  def test_tables_method
    db = create_db

    # Initially should have no tables (or only system tables)
    initial_tables = db.tables

    assert_instance_of Array, initial_tables, "tables() should return an array"

    # Create a table and verify it appears in tables list
    create_test_table(db, :schema_test_table)

    tables = db.tables

    assert_includes tables, :schema_test_table, "tables() should include created table"
  end

  def test_schema_method_basic
    db = create_db
    create_test_table(db, :schema_test)

    schema = db.schema(:schema_test)

    assert_instance_of Array, schema, "schema() should return an array"
    refute_empty schema, "Schema should not be empty"

    # Each schema entry should be [column_name, column_info]
    schema.each do |column_entry|
      assert_instance_of Array, column_entry, "Each schema entry should be an array"
      assert_equal 2, column_entry.length, "Each schema entry should have 2 elements"

      column_name, column_info = column_entry

      assert_instance_of Symbol, column_name, "Column name should be a symbol"
      assert_instance_of Hash, column_info, "Column info should be a hash"
    end
  end

  def test_schema_method_column_details
    db = create_db
    create_test_table(db, :detailed_schema_test)

    schema = db.schema(:detailed_schema_test)
    column_names = schema.map(&:first)

    # Verify expected columns exist
    assert_includes column_names, :id, "Should have id column"
    assert_includes column_names, :name, "Should have name column"
    assert_includes column_names, :age, "Should have age column"
    assert_includes column_names, :active, "Should have active column"

    # Test specific column properties
    id_column = schema.find { |col| col[0] == :id }

    refute_nil id_column, "ID column should exist"

    id_info = id_column[1]

    assert id_info.key?(:type), "Column info should have type"
    assert id_info.key?(:db_type), "Column info should have db_type"
    assert id_info.key?(:allow_null), "Column info should have allow_null"
    assert id_info.key?(:primary_key), "Column info should have primary_key"
  end

  def test_schema_method_primary_key_detection
    db = create_db
    create_test_table(db, :pk_test)

    schema = db.schema(:pk_test)
    id_column = schema.find { |col| col[0] == :id }

    refute_nil id_column, "ID column should exist"
    id_info = id_column[1]

    # Primary key detection will be implemented when schema parsing is added
    # For now, just verify the column exists
    assert id_info.key?(:primary_key), "Should have primary_key information"
  end

  def test_schema_method_null_constraints
    db = create_db

    db.create_table(:null_test) do
      primary_key :id
      String :required_field, null: false
      String :optional_field, null: true
    end

    schema = db.schema(:null_test)

    required_column = schema.find { |col| col[0] == :required_field }
    optional_column = schema.find { |col| col[0] == :optional_field }

    refute_nil required_column, "Required field column should exist"
    refute_nil optional_column, "Optional field column should exist"

    # Null constraint detection will be implemented when schema parsing is added
    assert required_column[1].key?(:allow_null), "Should have allow_null information"
    assert optional_column[1].key?(:allow_null), "Should have allow_null information"
  end

  def test_schema_method_default_values
    db = create_db

    db.create_table(:default_test) do
      primary_key :id
      String :name
      Boolean :active, default: true
      Integer :count, default: 0
    end

    schema = db.schema(:default_test)

    active_column = schema.find { |col| col[0] == :active }
    count_column = schema.find { |col| col[0] == :count }

    refute_nil active_column, "Active column should exist"
    refute_nil count_column, "Count column should exist"

    # Default value detection will be implemented when schema parsing is added
    assert active_column[1].key?(:default), "Should have default information"
    assert count_column[1].key?(:default), "Should have default information"
  end

  def test_schema_method_data_types
    db = create_db

    db.create_table(:type_test) do
      primary_key :id
      String :string_col
      Integer :integer_col
      Float :float_col
      Boolean :boolean_col
      Date :date_col
      DateTime :datetime_col
    end

    schema = db.schema(:type_test)

    # Verify each column has type information
    schema.each do |column_name, column_info|
      assert column_info.key?(:type), "Column #{column_name} should have type"
      assert column_info.key?(:db_type), "Column #{column_name} should have db_type"

      # Type should be a symbol
      assert_instance_of Symbol, column_info[:type], "Type should be a symbol"

      # DB type should be a string
      assert_instance_of String, column_info[:db_type], "DB type should be a string"
    end
  end

  def test_table_drop
    db = create_db
    create_test_table(db, :drop_test)

    # Verify table exists
    assert_table_exists(db, :drop_test)

    # Drop the table
    assert_nothing_raised("Should be able to drop table") do
      db.drop_table(:drop_test)
    end

    # Verify table no longer exists
    refute_includes db.tables, :drop_test, "Dropped table should not exist"
  end

  def test_table_exists_method
    db = create_db

    # Test non-existent table
    refute db.table_exists?(:nonexistent), "Non-existent table should return false"

    # Create table and test
    create_test_table(db, :exists_test)

    assert db.table_exists?(:exists_test), "Existing table should return true"
  end

  def test_multiple_tables
    db = create_db

    # Create multiple tables
    db.create_table(:table1) do
      primary_key :id
      String :name
    end

    db.create_table(:table2) do
      primary_key :id
      Integer :value
    end

    tables = db.tables

    assert_includes tables, :table1, "Should include table1"
    assert_includes tables, :table2, "Should include table2"

    # Test schema for each table
    schema1 = db.schema(:table1)
    schema2 = db.schema(:table2)

    refute_empty schema1, "Table1 schema should not be empty"
    refute_empty schema2, "Table2 schema should not be empty"

    # Verify different schemas
    table1_columns = schema1.map(&:first)
    table2_columns = schema2.map(&:first)

    assert_includes table1_columns, :name, "Table1 should have name column"
    assert_includes table2_columns, :value, "Table2 should have value column"
  end

  def test_schema_error_handling
    db = create_db

    # Test schema for non-existent table
    assert_database_error do
      db.schema(:nonexistent_table)
    end
  end

  def test_table_creation_error_handling
    db = create_db

    # Create a table
    create_test_table(db, :duplicate_test)

    # Try to create table with same name (should raise error)
    assert_database_error do
      create_test_table(db, :duplicate_test)
    end
  end

  def test_complex_table_structure
    db = create_db

    assert_nothing_raised("Should handle complex table structure") do
      db.create_table(:complex_table) do
        primary_key :id
        String :first_name, null: false
        String :last_name, null: false
        String :email, unique: true
        Integer :age
        Boolean :active, default: true
        Date :birth_date
        DateTime :created_at
        DateTime :updated_at
        Float :score, default: 0.0
      end
    end

    schema = db.schema(:complex_table)
    column_names = schema.map(&:first)

    expected_columns = %i[id first_name last_name email age active
                          birth_date created_at updated_at score]

    expected_columns.each do |col|
      assert_includes column_names, col, "Should have #{col} column"
    end
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end
