# frozen_string_literal: true

# Test configuration and setup for sequel-duckdb adapter
# Following sequel-hexspace pattern for test infrastructure

require "minitest/autorun"
require "minitest/pride"
require "sequel"

# Load the adapter
require_relative "../lib/sequel/adapters/duckdb"

# Test configuration module
module SequelDuckDBTest
  # Create a fresh mock database for each test to avoid state interference
  def self.create_mock_db
    Sequel.mock(host: "duckdb", quote_identifiers: true)
  end

  # Create an in-memory DuckDB database for integration testing
  def self.create_test_db
    Sequel.connect("duckdb::memory:")
  end

  # Base test class for all sequel-duckdb tests
  class TestCase < Minitest::Test
    # Get a fresh mock database for each test
    def mock_db
      @mock_db ||= SequelDuckDBTest.create_mock_db
    end

    # Setup method run before each test
    def setup
      # Clear the cached mock database to ensure fresh state
      @mock_db = nil
    end

    # Teardown method run after each test
    def teardown
      # Clean up any test resources
    end

    # Helper method to create a mock dataset for SQL generation testing
    def mock_dataset(table_name = :test_table)
      mock_db[table_name]
    end

    # Helper method to create an in-memory database for integration testing
    def create_db
      SequelDuckDBTest.create_test_db
    end

    # Helper method to assert SQL generation
    def assert_sql(expected_sql, dataset)
      actual_sql = dataset.sql

      assert_equal expected_sql, actual_sql, "Generated SQL does not match expected"
    end

    # Helper method to assert SQL contains specific patterns
    def assert_sql_includes(pattern, dataset)
      actual_sql = dataset.sql

      assert_includes actual_sql, pattern, "Generated SQL does not contain expected pattern"
    end

    # Helper method to assert SQL matches regex
    def assert_sql_match(regex, dataset)
      actual_sql = dataset.sql

      assert_match regex, actual_sql, "Generated SQL does not match expected pattern"
    end

    # Helper method to create a test table in a database
    def create_test_table(db, table_name = :test_table)
      db.create_table(table_name) do
        Integer :id, primary_key: true
        String :name, null: false
        Integer :age
        Date :birth_date
        Boolean :active, default: true
        DateTime :created_at
        Float :score
      end
    end

    # Helper method to insert test data
    def insert_test_data(db, table_name = :test_table)
      db[table_name].insert(
        id: 1,
        name: "John Doe",
        age: 30,
        birth_date: Date.new(1993, 5, 15),
        active: true,
        created_at: Time.now,
        score: 85.5
      )
      db[table_name].insert(
        id: 2,
        name: "Jane Smith",
        age: 25,
        birth_date: Date.new(1998, 8, 22),
        active: false,
        created_at: Time.now,
        score: 92.3
      )
    end

    # Helper method to assert database connection is working
    def assert_connection_valid(db)
      refute_nil db, "Database connection should not be nil"
      assert db.test_connection, "Database connection should be valid"
    end

    # Helper method to assert table exists
    def assert_table_exists(db, table_name)
      assert_includes db.tables, table_name, "Table #{table_name} should exist"
    end

    # Helper method to assert column exists in table schema
    def assert_column_exists(db, table_name, column_name)
      schema = db.schema(table_name)
      column_names = schema.map(&:first)

      assert_includes column_names, column_name, "Column #{column_name} should exist in table #{table_name}"
    end

    # Helper method to assert specific column properties
    def assert_column_properties(db, table_name, column_name, expected_properties)
      schema = db.schema(table_name)
      column_info = schema.find { |col| col[0] == column_name }

      refute_nil column_info, "Column #{column_name} should exist"

      properties = column_info[1]
      expected_properties.each do |key, expected_value|
        actual_value = properties[key]

        assert_equal expected_value, actual_value,
                     "Column #{column_name} property #{key} should be #{expected_value}, got #{actual_value}"
      end
    end

    # Helper method to assert record count
    def assert_record_count(dataset, expected_count)
      actual_count = dataset.count

      assert_equal expected_count, actual_count,
                   "Expected #{expected_count} records, got #{actual_count}"
    end

    # Helper method to assert record exists with specific attributes
    def assert_record_exists(dataset, attributes)
      record = dataset.where(attributes).first

      refute_nil record, "Record with attributes #{attributes} should exist"
      record
    end

    # Helper method to assert exception is raised
    def assert_database_error(error_class = Sequel::DatabaseError, &)
      assert_raises(error_class, &)
    end

    # Helper method to assert connection error
    def assert_connection_error(&)
      assert_database_error(Sequel::DatabaseConnectionError, &)
    end

    # Helper method to assert that no exception is raised
    def assert_nothing_raised(message = nil)
      yield
    rescue StandardError => e
      flunk "#{message || "Expected no exception"}, but got #{e.class}: #{e.message}"
    end
  end
end

# Configure Sequel for testing
Sequel.extension :migration

# Ensure proper error handling during tests
Sequel::Database.extension :error_sql

# puts "sequel-duckdb test infrastructure loaded"
# puts "Mock database created fresh for each test via mock_db method"
# puts "Use SequelDuckDBTest.create_test_db for integration testing"
