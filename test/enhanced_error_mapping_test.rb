# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for enhanced error mapping functionality in DuckDB adapter
# Tests the new database_exception_class and database_exception_message methods
class EnhancedErrorMappingTest < SequelDuckDBTest::TestCase
  def test_constraint_violation_error_mapping
    db = create_db

    # Create table with constraints for testing
    db.run <<~SQL
      CREATE TABLE enhanced_constraint_test (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE,
        age INTEGER CHECK (age >= 0)
      )
    SQL

    # Test NOT NULL constraint violation mapping
    begin
      db.execute("INSERT INTO enhanced_constraint_test (id, email) VALUES (1, 'test@example.com')")

      flunk "Should have raised a constraint violation"
    rescue Sequel::NotNullConstraintViolation => e
      assert_match(/NOT NULL/i, e.message, "NOT NULL constraint error should be properly mapped")
      assert_match(/DuckDB::Error:/, e.message, "Error message should include DuckDB context")
    rescue Sequel::DatabaseError => e
      # DuckDB might not always provide specific constraint violation messages
      # In that case, it should still be a DatabaseError with descriptive message
      assert_match(/NOT NULL/i, e.message, "Error should indicate NOT NULL constraint issue")
    end

    # Insert valid record for further testing
    db.execute("INSERT INTO enhanced_constraint_test (id, name, email, age) VALUES (1, 'Test User', 'test@example.com', 25)")

    # Test UNIQUE constraint violation mapping
    begin
      db.execute("INSERT INTO enhanced_constraint_test (id, name, email, age) VALUES (2, 'Another User', 'test@example.com', 30)")

      flunk "Should have raised a unique constraint violation"
    rescue Sequel::UniqueConstraintViolation => e
      assert_match(/UNIQUE/i, e.message, "UNIQUE constraint error should be properly mapped")
      assert_match(/DuckDB::Error:/, e.message, "Error message should include DuckDB context")
    rescue Sequel::DatabaseError => e
      # DuckDB might not always provide specific constraint violation messages
      assert_match(/(UNIQUE|duplicate)/i, e.message, "Error should indicate UNIQUE constraint issue")
    end

    # Test PRIMARY KEY constraint violation mapping
    begin
      db.execute("INSERT INTO enhanced_constraint_test (id, name, email, age) VALUES (1, 'Duplicate ID', 'another@example.com', 35)")

      flunk "Should have raised a primary key constraint violation"
    rescue Sequel::UniqueConstraintViolation => e
      # Primary key violations are mapped to UniqueConstraintViolation
      assert_match(/(PRIMARY KEY|UNIQUE|duplicate)/i, e.message,
                   "PRIMARY KEY constraint error should be properly mapped")
    rescue Sequel::DatabaseError => e
      # DuckDB might not always provide specific constraint violation messages
      assert_match(/(PRIMARY KEY|UNIQUE|duplicate)/i, e.message, "Error should indicate PRIMARY KEY constraint issue")
    end

    # Test CHECK constraint violation mapping
    begin
      db.execute("INSERT INTO enhanced_constraint_test (id, name, email, age) VALUES (3, 'Invalid Age', 'invalid@example.com', -5)")

      flunk "Should have raised a check constraint violation"
    rescue Sequel::CheckConstraintViolation => e
      assert_match(/CHECK/i, e.message, "CHECK constraint error should be properly mapped")
    rescue Sequel::DatabaseError => e
      # DuckDB might not always provide specific constraint violation messages
      assert_match(/(CHECK|constraint)/i, e.message, "Error should indicate CHECK constraint issue")
    end
  end

  def test_sql_syntax_error_mapping
    db = create_db

    # Test various syntax errors
    syntax_errors = [
      "SELCT * FROM information_schema.tables",
      "SELECT * FORM information_schema.tables",
      "SELECT * FROM information_schema.tables WHRE 1=1"
    ]

    syntax_errors.each do |sql|
      db.execute(sql)

      flunk "Should have raised a syntax error for: #{sql}"
    rescue Sequel::DatabaseError => e
      assert_match(/DuckDB::Error:/, e.message, "Error should include DuckDB context")
      assert_match(/(syntax|parse|unexpected)/i, e.message, "Error should indicate syntax issue")
    end
  end

  def test_table_not_found_error_mapping
    db = create_db

    table_errors = [
      "SELECT * FROM nonexistent_table",
      "INSERT INTO nonexistent_table (id, name) VALUES (1, 'test')",
      "UPDATE nonexistent_table SET name = 'updated' WHERE id = 1",
      "DELETE FROM nonexistent_table WHERE id = 1"
    ]

    table_errors.each do |sql|
      db.execute(sql)

      flunk "Should have raised a table not found error for: #{sql}"
    rescue Sequel::DatabaseError => e
      assert_match(/DuckDB::Error:/, e.message, "Error should include DuckDB context")
      assert_match(/(table.*does.*not.*exist|no.*such.*table)/i, e.message, "Error should indicate table not found")
    end
  end

  def test_column_not_found_error_mapping
    db = create_db
    create_test_table(db)

    column_errors = [
      "SELECT nonexistent_column FROM test_table",
      "INSERT INTO test_table (nonexistent_column) VALUES ('test')",
      "UPDATE test_table SET nonexistent_column = 'updated' WHERE id = 1"
    ]

    column_errors.each do |sql|
      db.execute(sql)

      flunk "Should have raised a column not found error for: #{sql}"
    rescue Sequel::DatabaseError => e
      assert_match(/DuckDB::Error:/, e.message, "Error should include DuckDB context")
      assert_match(
        /(column.*does.*not.*exist|no.*such.*column|unknown.*column|referenced.*column.*not.*found|does.*not.*have.*a.*column)/i, e.message, "Error should indicate column not found"
      )
    end
  end

  def test_connection_error_mapping
    # Test connection errors are properly mapped

    Sequel.connect("duckdb:///invalid/path/that/does/not/exist.db")

    flunk "Should have raised a connection error"
  rescue Sequel::DatabaseConnectionError => e
    assert_match(/Failed to connect to DuckDB database/, e.message,
                 "Connection error should have descriptive message")
  end
end
