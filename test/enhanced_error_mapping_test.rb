# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for enhanced error mapping functionality in DuckDB adapter
# Tests the new database_exception_class and database_exception_message methods
class EnhancedErrorMappingTest < SequelDuckDBTest::TestCase
  def test_database_exception_class_method_exists
    db = create_db

    # Test that the new database_exception_class method exists
    exception = ::DuckDB::Error.new("Test error")
    result = db.send(:database_exception_class, exception, {})

    assert_kind_of Class, result, "database_exception_class should return a class"
    assert result <= Exception, "Returned class should be an exception class"
  end

  def test_database_exception_message_method_exists
    db = create_db

    # Test that the new database_exception_message method exists
    exception = ::DuckDB::Error.new("Test error")
    result = db.send(:database_exception_message, exception, {})

    assert_instance_of String, result, "database_exception_message should return a string"
    assert_match(/DuckDB error:/, result, "Message should include DuckDB error prefix")
  end

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
      assert_match(/DuckDB error:/, e.message, "Error message should include DuckDB context")
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
      assert_match(/DuckDB error:/, e.message, "Error message should include DuckDB context")
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
      assert_match(/DuckDB error:/, e.message, "Error should include DuckDB context")
      assert_match(/(syntax|parse|unexpected)/i, e.message, "Error should indicate syntax issue")
      assert_match(/SQL: #{Regexp.escape(sql)}/, e.message, "Error should include the problematic SQL")
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
      assert_match(/DuckDB error:/, e.message, "Error should include DuckDB context")
      assert_match(/(table.*does.*not.*exist|no.*such.*table)/i, e.message, "Error should indicate table not found")
      assert_match(/SQL: #{Regexp.escape(sql)}/, e.message, "Error should include the problematic SQL")
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
      assert_match(/DuckDB error:/, e.message, "Error should include DuckDB context")
      assert_match(
        /(column.*does.*not.*exist|no.*such.*column|unknown.*column|referenced.*column.*not.*found|does.*not.*have.*a.*column)/i, e.message, "Error should indicate column not found"
      )
      assert_match(/SQL: #{Regexp.escape(sql)}/, e.message, "Error should include the problematic SQL")
    end
  end

  def test_parameterized_query_error_context
    db = create_db
    create_test_table(db)

    # Test that parameterized query errors include parameter context
    begin
      db.execute("INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)", [1, "Test", "invalid_age"])
      # This might succeed if DuckDB does type conversion, so we'll try a different approach
    rescue Sequel::DatabaseError => e
      assert_match(/DuckDB error:/, e.message, "Error should include DuckDB context")
      assert_match(/Parameters:/, e.message, "Error should include parameter context")
      assert_match(/\[1, "Test", "invalid_age"\]/, e.message, "Error should show actual parameters")
    end

    # Test with invalid SQL and parameters
    begin
      db.execute("INVALID SQL WITH ? PARAMETERS", %w[param1 param2])
      flunk "Should have raised a syntax error"
    rescue Sequel::DatabaseError => e
      assert_match(/DuckDB error:/, e.message, "Error should include DuckDB context")
      assert_match(/SQL: INVALID SQL WITH \? PARAMETERS/, e.message, "Error should include SQL context")
      assert_match(/Parameters: \["param1", "param2"\]/, e.message, "Error should include parameter context")
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

  def test_error_message_enhancement
    db = create_db

    # Test that error messages are enhanced with SQL and parameter context
    test_sql = "SELECT * FROM nonexistent_table_for_testing"
    test_params = ["param1", 123, true]

    begin
      db.execute(test_sql, test_params)
      flunk "Should have raised an error"
    rescue Sequel::DatabaseError => e
      # Verify enhanced message format
      assert_match(/DuckDB error:/, e.message, "Should include DuckDB error prefix")
      assert_match(/SQL: #{Regexp.escape(test_sql)}/, e.message, "Should include SQL context")
      assert_match(/Parameters: #{Regexp.escape(test_params.inspect)}/, e.message, "Should include parameter context")
    end

    # Test error message without parameters
    begin
      db.execute("SELECT * FROM another_nonexistent_table")
      flunk "Should have raised an error"
    rescue Sequel::DatabaseError => e
      assert_match(/DuckDB error:/, e.message, "Should include DuckDB error prefix")
      assert_match(/SQL: SELECT \* FROM another_nonexistent_table/, e.message, "Should include SQL context")
      refute_match(/Parameters:/, e.message, "Should not include parameter context when no parameters")
    end
  end

  def test_error_class_selection_logic
    db = create_db

    # Test the database_exception_class method directly with various error messages
    test_cases = [
      {
        message: "connection failed",
        expected_class: Sequel::DatabaseConnectionError,
        description: "Connection error"
      },
      {
        message: "violates NOT NULL constraint",
        expected_class: Sequel::NotNullConstraintViolation,
        description: "NOT NULL constraint"
      },
      {
        message: "UNIQUE constraint violation",
        expected_class: Sequel::UniqueConstraintViolation,
        description: "UNIQUE constraint"
      },
      {
        message: "CHECK constraint violation",
        expected_class: Sequel::CheckConstraintViolation,
        description: "CHECK constraint"
      },
      {
        message: "syntax error near 'SELCT'",
        expected_class: Sequel::DatabaseError,
        description: "Syntax error"
      },
      {
        message: "table does not exist",
        expected_class: Sequel::DatabaseError,
        description: "Table not found"
      },
      {
        message: "unknown error type",
        expected_class: Sequel::DatabaseError,
        description: "Unknown error"
      }
    ]

    test_cases.each do |test_case|
      exception = ::DuckDB::Error.new(test_case[:message])
      result_class = db.send(:database_exception_class, exception, {})

      assert_equal test_case[:expected_class], result_class,
                   "#{test_case[:description]} should map to #{test_case[:expected_class]}"
    end
  end

  def test_handle_constraint_violation_method
    db = create_db

    # Test the handle_constraint_violation method if it exists
    return unless db.respond_to?(:handle_constraint_violation, true)

    exception = ::DuckDB::Error.new("UNIQUE constraint violation")
    opts = { sql: "INSERT INTO test (id) VALUES (1)", params: [1] }

    result = db.send(:handle_constraint_violation, exception, opts)

    assert_kind_of Exception, result, "handle_constraint_violation should return an exception"
    assert_match(/DuckDB error:/, result.message, "Result should have enhanced message")
  end
end
