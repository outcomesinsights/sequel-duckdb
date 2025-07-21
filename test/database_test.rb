# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for Sequel::DuckDB::Database class
# Tests database connection, basic functionality, and database-level operations
class DatabaseTest < SequelDuckDBTest::TestCase
  def test_database_class_exists
    assert defined?(Sequel::DuckDB::Database), "Database class should be defined"
    assert Sequel::DuckDB::Database < Sequel::Database, "Database should inherit from Sequel::Database"
  end

  def test_database_includes_database_methods
    assert Sequel::DuckDB::Database.included_modules.include?(Sequel::DuckDB::DatabaseMethods),
           "Database should include DatabaseMethods module"
  end

  def test_adapter_scheme_registration
    assert_equal :duckdb, Sequel::DuckDB::Database.adapter_scheme,
                 "Database should have :duckdb adapter scheme"
  end

  def test_dataset_class_default
    db = create_db
    assert_equal Sequel::DuckDB::Dataset, db.dataset_class_default,
                 "Database should return DuckDB::Dataset as default dataset class"
  end

  def test_memory_database_connection
    db = nil
    assert_nothing_raised("Should be able to connect to in-memory database") do
      db = Sequel.connect("duckdb::memory:")
    end

    assert_connection_valid(db)
    assert_instance_of Sequel::DuckDB::Database, db, "Should create DuckDB::Database instance"
  end

  def test_file_database_connection
    require "tempfile"

    Tempfile.create(["test_db", ".duckdb"]) do |tmpfile|
      db_path = tmpfile.path
      tmpfile.close
      File.unlink(db_path) # Remove the file so DuckDB can create it

      db = nil
      assert_nothing_raised("Should be able to connect to file database") do
        db = Sequel.connect("duckdb://#{db_path}")
      end

      assert_connection_valid(db)
      assert File.exist?(db_path), "Database file should be created"
    end
  end

  def test_connection_error_handling
    # Test with invalid database path (directory that doesn't exist)
    invalid_path = "/nonexistent/directory/test.duckdb"

    assert_connection_error do
      Sequel.connect("duckdb://#{invalid_path}")
    end
  end

  def test_database_test_connection
    db = create_db
    assert db.test_connection, "test_connection should return true for valid connection"
  end

  def test_database_disconnect
    db = create_db

    # Should be able to disconnect without error
    assert_nothing_raised("Should be able to disconnect") do
      db.disconnect
    end
  end

  def test_database_uri_parsing
    # Test various URI formats
    db1 = Sequel.connect("duckdb::memory:")
    assert_instance_of Sequel::DuckDB::Database, db1

    db2 = Sequel.connect("duckdb:///:memory:")
    assert_instance_of Sequel::DuckDB::Database, db2
  end

  def test_database_supports_transactions
    db = create_db

    # Basic transaction support test
    result = nil
    assert_nothing_raised("Should support transactions") do
      result = db.transaction do
        "transaction_result"
      end
    end

    assert_equal "transaction_result", result, "Transaction should return block result"
  end

  def test_database_table_creation
    db = create_db

    assert_nothing_raised("Should be able to create tables") do
      create_test_table(db)
    end

    assert_table_exists(db, :test_table)
  end

  def test_database_basic_operations
    db = create_db
    create_test_table(db)

    # Test basic insert
    assert_nothing_raised("Should be able to insert data") do
      db[:test_table].insert(name: "Test User", age: 25)
    end

    # Test basic select
    count = nil
    assert_nothing_raised("Should be able to count records") do
      count = db[:test_table].count
    end

    assert_equal 1, count, "Should have one record after insert"
  end

  def test_database_schema_introspection
    db = create_db
    create_test_table(db)

    # Test tables method
    tables = nil
    assert_nothing_raised("Should be able to list tables") do
      tables = db.tables
    end

    assert_includes tables, :test_table, "Should list created table"

    # Test schema method
    schema = nil
    assert_nothing_raised("Should be able to get table schema") do
      schema = db.schema(:test_table)
    end

    refute_empty schema, "Schema should not be empty"
    assert_instance_of Array, schema, "Schema should be an array"

    # Verify some expected columns exist
    column_names = schema.map(&:first)
    assert_includes column_names, :id, "Should have id column"
    assert_includes column_names, :name, "Should have name column"
    assert_includes column_names, :age, "Should have age column"
  end

  def test_database_error_handling
    db = create_db

    # Test SQL syntax error
    assert_database_error do
      db.run("INVALID SQL SYNTAX")
    end

    # Test table not found error
    assert_database_error do
      db[:nonexistent_table].count
    end
  end

  def test_database_concurrent_access
    db = create_db
    create_test_table(db)

    # Test that multiple operations can be performed
    assert_nothing_raised("Should handle multiple operations") do
      db[:test_table].insert(name: "User 1", age: 20)
      db[:test_table].insert(name: "User 2", age: 30)
      count = db[:test_table].count
      assert_equal 2, count
    end
  end

  def test_database_connection_pooling
    # Test that connection pooling works (basic test)
    db = create_db

    # Multiple operations should work with connection pooling
    10.times do |i|
      assert_nothing_raised("Operation #{i} should work") do
        db.test_connection
      end
    end
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end