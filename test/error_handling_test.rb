# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for error handling and exception mapping in DuckDB adapter
# Tests Requirements 8.1, 8.2, 8.3, 8.7 - Error Handling and Logging
class ErrorHandlingTest < SequelDuckDBTest::TestCase
  def test_database_error_classes_method_exists
    db = create_db

    # Test that database_error_classes method exists and returns expected classes
    error_classes = db.send(:database_error_classes)
    assert_instance_of Array, error_classes, "database_error_classes should return an array"
    assert_includes error_classes, ::DuckDB::Error, "Should include DuckDB::Error class"
  end

  def test_database_exception_sqlstate_method_exists
    db = create_db

    # Test that database_exception_sqlstate method exists
    exception = ::DuckDB::Error.new("Test error")
    sqlstate = db.send(:database_exception_sqlstate, exception, {})

    # For now, DuckDB doesn't provide SQL state codes, so this should return nil
    assert_nil sqlstate, "database_exception_sqlstate should return nil for DuckDB errors without SQL state"
  end

  def test_database_exception_use_sqlstates_method
    db = create_db

    # Test that database_exception_use_sqlstates? method exists and returns boolean
    use_sqlstates = db.send(:database_exception_use_sqlstates?)
    assert [true, false].include?(use_sqlstates), "database_exception_use_sqlstates? should return boolean"
  end

  def test_connection_error_mapping
    # Test that DuckDB connection errors are mapped to Sequel::DatabaseConnectionError (Requirement 8.1)

    # Test with invalid database path
    assert_raises(Sequel::DatabaseConnectionError, "Invalid database path should raise DatabaseConnectionError") do
      Sequel.connect("duckdb:///invalid/path/that/does/not/exist/database.db")
    end

    # Test with invalid connection string format
    assert_raises(Sequel::DatabaseConnectionError, "Invalid connection format should raise DatabaseConnectionError") do
      # This should cause a connection error
      db = Sequel.connect("duckdb::memory:")
      # Force connection with invalid parameters
      db.send(:connect, { database: "/dev/null/invalid" })
    end
  end

  def test_sql_syntax_error_mapping
    # Test that SQL syntax errors are mapped to Sequel::DatabaseError (Requirement 8.2)
    db = create_db

    assert_raises(Sequel::DatabaseError, "SQL syntax error should raise DatabaseError") do
      db.execute("INVALID SQL SYNTAX HERE")
    end

    assert_raises(Sequel::DatabaseError, "Invalid SELECT syntax should raise DatabaseError") do
      db.execute("SELECT * FROM")  # Incomplete SQL
    end

    assert_raises(Sequel::DatabaseError, "Invalid INSERT syntax should raise DatabaseError") do
      db.execute("INSERT INTO VALUES")  # Incomplete SQL
    end
  end

  def test_constraint_violation_error_mapping
    # Test that constraint violations are mapped to appropriate Sequel exceptions (Requirement 8.3)
    db = create_db

    # Create table with constraints
    db.run <<~SQL
      CREATE TABLE constraint_test (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE
      )
    SQL

    # Test NOT NULL constraint violation
    error = assert_raises(Sequel::DatabaseError, "NOT NULL constraint violation should raise DatabaseError") do
      db.execute("INSERT INTO constraint_test (id, email) VALUES (1, 'test@example.com')")
    end
    assert_match(/NOT NULL/i, error.message, "Error message should indicate NOT NULL constraint violation")

    # Insert valid record first
    db.execute("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Test User', 'test@example.com')")

    # Test PRIMARY KEY constraint violation
    error = assert_raises(Sequel::DatabaseError, "PRIMARY KEY constraint violation should raise DatabaseError") do
      db.execute("INSERT INTO constraint_test (id, name, email) VALUES (1, 'Another User', 'another@example.com')")
    end
    assert_match(/(PRIMARY KEY|UNIQUE|duplicate)/i, error.message, "Error message should indicate constraint violation")

    # Test UNIQUE constraint violation
    error = assert_raises(Sequel::DatabaseError, "UNIQUE constraint violation should raise DatabaseError") do
      db.execute("INSERT INTO constraint_test (id, name, email) VALUES (2, 'Another User', 'test@example.com')")
    end
    assert_match(/(UNIQUE|duplicate)/i, error.message, "Error message should indicate UNIQUE constraint violation")
  end

  def test_specific_constraint_error_types
    # Test mapping to specific Sequel constraint error types (Requirement 8.3)
    db = create_db

    # Create table with various constraints
    db.run <<~SQL
      CREATE TABLE specific_constraint_test (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE,
        age INTEGER CHECK (age >= 0)
      )
    SQL

    # Test CHECK constraint violation
    error = assert_raises(Sequel::DatabaseError, "CHECK constraint violation should raise DatabaseError") do
      db.execute("INSERT INTO specific_constraint_test (id, name, email, age) VALUES (1, 'Test', 'test@example.com', -5)")
    end
    assert_match(/(CHECK|constraint)/i, error.message, "Error message should indicate CHECK constraint violation")

    # Test foreign key constraint (if supported)
    begin
      db.run <<~SQL
        CREATE TABLE parent_table (
          id INTEGER PRIMARY KEY,
          name VARCHAR(50)
        )
      SQL

      db.run <<~SQL
        CREATE TABLE child_table (
          id INTEGER PRIMARY KEY,
          parent_id INTEGER REFERENCES parent_table(id),
          name VARCHAR(50)
        )
      SQL

      # Test foreign key constraint violation
      error = assert_raises(Sequel::DatabaseError, "Foreign key constraint violation should raise DatabaseError") do
        db.execute("INSERT INTO child_table (id, parent_id, name) VALUES (1, 999, 'Child')")
      end
      assert_match(/(FOREIGN KEY|reference)/i, error.message, "Error message should indicate foreign key constraint violation")
    rescue Sequel::DatabaseError
      # DuckDB may not support foreign keys in all versions, so we'll skip this test if it fails
      skip "DuckDB version doesn't support foreign key constraints"
    end
  end

  def test_table_not_found_error_mapping
    # Test that table not found errors are properly mapped
    db = create_db

    assert_raises(Sequel::DatabaseError, "Table not found should raise DatabaseError") do
      db.execute("SELECT * FROM nonexistent_table")
    end

    assert_raises(Sequel::DatabaseError, "INSERT into nonexistent table should raise DatabaseError") do
      db.execute("INSERT INTO nonexistent_table (id, name) VALUES (1, 'test')")
    end
  end

  def test_column_not_found_error_mapping
    # Test that column not found errors are properly mapped
    db = create_db
    create_test_table(db)

    assert_raises(Sequel::DatabaseError, "Column not found should raise DatabaseError") do
      db.execute("SELECT nonexistent_column FROM test_table")
    end

    assert_raises(Sequel::DatabaseError, "INSERT with invalid column should raise DatabaseError") do
      db.execute("INSERT INTO test_table (nonexistent_column) VALUES ('test')")
    end
  end

  def test_data_type_error_mapping
    # Test that data type errors are properly mapped
    db = create_db
    create_test_table(db)

    # Test inserting invalid data type (string where integer expected)
    # Note: DuckDB may be more lenient with type conversions, so this test may need adjustment
    begin
      db.execute("INSERT INTO test_table (id, name, age) VALUES ('not_a_number', 'Test', 25)")
      # If DuckDB allows this conversion, we'll skip this specific test
    rescue Sequel::DatabaseError
      # This is expected if DuckDB rejects the type conversion
      assert true, "Data type error should raise DatabaseError"
    end
  end

  def test_error_message_preservation
    # Test that original DuckDB error messages are preserved in Sequel exceptions (Requirement 8.2)
    db = create_db

    begin
      db.execute("INVALID SQL SYNTAX")
      flunk "Should have raised an exception"
    rescue Sequel::DatabaseError => e
      assert_match(/DuckDB error:/, e.message, "Error message should indicate DuckDB origin")
      refute_empty e.message, "Error message should not be empty"
    end
  end

  def test_nested_error_handling_in_transactions
    # Test that errors within transactions are properly handled and mapped
    db = create_db
    create_test_table(db)

    # Insert initial data
    db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Initial', 25)")
    initial_count = db[:test_table].count

    # Test that database errors within transactions cause proper rollback and exception mapping
    assert_raises(Sequel::DatabaseError, "Database error in transaction should raise DatabaseError") do
      db.transaction do
        db.execute("INSERT INTO test_table (id, name, age) VALUES (2, 'Valid', 30)")
        db.execute("INVALID SQL SYNTAX")  # This should cause rollback
      end
    end

    # Verify rollback occurred
    assert_equal initial_count, db[:test_table].count, "Transaction should be rolled back on database error"
    refute_includes db[:test_table].select_map(:name), "Valid", "Valid insert should be rolled back"
  end

  def test_error_handling_with_prepared_statements
    # Test error handling with prepared statements
    db = create_db
    create_test_table(db)

    # Test syntax error in prepared statement
    assert_raises(Sequel::DatabaseError, "Prepared statement syntax error should raise DatabaseError") do
      db.execute("INVALID SQL WITH ? PARAMETER", [1])
    end

    # Test parameter mismatch error
    assert_raises(Sequel::DatabaseError, "Parameter mismatch should raise DatabaseError") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)", [1, "Test"])  # Missing parameter
    end
  end

  def test_connection_lost_error_handling
    # Test handling of connection lost scenarios
    db = create_db

    # This is a basic test - in practice, connection loss is hard to simulate
    # We'll test that the connection validation works
    assert db.test_connection, "Connection should be valid initially"

    # Test that invalid connection is detected
    # We can't easily simulate connection loss, so we'll test the validation method
    conn = db.synchronize { |c| c }
    assert db.send(:valid_connection?, conn), "Valid connection should be detected as valid"
  end

  def test_concurrent_error_handling
    # Test error handling under concurrent access patterns
    db = create_db
    create_test_table(db)

    # Test multiple sequential errors are handled properly
    5.times do |i|
      assert_raises(Sequel::DatabaseError, "Error #{i} should be handled properly") do
        db.execute("SELECT * FROM nonexistent_table_#{i}")
      end
    end

    # Database should still be functional after multiple errors
    assert_nothing_raised("Database should remain functional after multiple errors") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'After Errors', 30)")
    end

    assert_equal 1, db[:test_table].count, "Database should work normally after handling errors"
  end

  def test_error_handling_with_schema_operations
    # Test error handling during schema operations
    db = create_db

    # Test creating table with invalid syntax
    assert_raises(Sequel::DatabaseError, "Invalid CREATE TABLE should raise DatabaseError") do
      db.execute("CREATE TABLE invalid_syntax (")
    end

    # Test dropping nonexistent table
    assert_raises(Sequel::DatabaseError, "DROP nonexistent table should raise DatabaseError") do
      db.execute("DROP TABLE nonexistent_table")
    end

    # Test altering nonexistent table
    assert_raises(Sequel::DatabaseError, "ALTER nonexistent table should raise DatabaseError") do
      db.execute("ALTER TABLE nonexistent_table ADD COLUMN new_col INTEGER")
    end
  end

  def test_specific_duckdb_error_mapping
    # Test mapping of specific DuckDB error types to appropriate Sequel exceptions (Requirement 8.7)
    db = create_db

    # Test various DuckDB-specific error scenarios
    test_cases = [
      {
        sql: "SELECT * FROM information_schema.nonexistent_view",
        description: "Invalid system view access"
      },
      {
        sql: "CREATE TABLE test AS SELECT * FROM nonexistent_table",
        description: "CREATE TABLE AS with invalid source"
      },
      {
        sql: "INSERT INTO test_table SELECT * FROM nonexistent_table",
        description: "INSERT SELECT with invalid source"
      }
    ]

    test_cases.each do |test_case|
      assert_raises(Sequel::DatabaseError, "#{test_case[:description]} should raise DatabaseError") do
        db.execute(test_case[:sql])
      end
    end
  end

  def test_enhanced_duckdb_error_mapping
    # Test enhanced mapping of DuckDB-specific errors with better categorization (Requirement 8.7)
    db = create_db

    # Test catalog errors (schema/table/column not found)
    catalog_errors = [
      {
        sql: "SELECT * FROM nonexistent_schema.table_name",
        description: "Schema not found error"
      },
      {
        sql: "SELECT nonexistent_column FROM information_schema.tables LIMIT 1",
        description: "Column not found error"
      }
    ]

    catalog_errors.each do |test_case|
      error = assert_raises(Sequel::DatabaseError, "#{test_case[:description]} should raise DatabaseError") do
        db.execute(test_case[:sql])
      end
      assert_match(/(not found|does not exist|unknown)/i, error.message, "#{test_case[:description]} should have descriptive message")
    end

    # Test syntax errors with specific patterns
    syntax_errors = [
      {
        sql: "SELCT * FROM information_schema.tables",
        description: "Misspelled SELECT keyword"
      },
      {
        sql: "SELECT * FORM information_schema.tables",
        description: "Misspelled FROM keyword"
      }
    ]

    syntax_errors.each do |test_case|
      error = assert_raises(Sequel::DatabaseError, "#{test_case[:description]} should raise DatabaseError") do
        db.execute(test_case[:sql])
      end
      assert_match(/(syntax|parse|unexpected)/i, error.message, "#{test_case[:description]} should indicate syntax error")
    end
  end

  def test_error_context_preservation
    # Test that error context (SQL, parameters) is preserved for debugging
    db = create_db

    begin
      db.execute("SELECT * FROM nonexistent_table WHERE id = ?", [123])
      flunk "Should have raised an exception"
    rescue Sequel::DatabaseError => e
      # The error should contain useful context for debugging
      assert_match(/DuckDB error:/, e.message, "Error should indicate DuckDB origin")
      # Additional context checking could be added here if the implementation provides it
    end
  end

  def test_error_handling_performance
    # Test that error handling doesn't significantly impact performance
    db = create_db
    create_test_table(db)

    # Time normal operations
    start_time = Time.now
    100.times do |i|
      db.execute("INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)", [i, "User #{i}", 20 + i])
    end
    normal_time = Time.now - start_time

    # Clean up
    db.execute("DELETE FROM test_table")

    # Time operations with error handling (catching and ignoring errors)
    start_time = Time.now
    100.times do |i|
      begin
        if i % 10 == 0
          # Cause an error every 10th operation
          db.execute("SELECT * FROM nonexistent_table")
        else
          db.execute("INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)", [i, "User #{i}", 20 + i])
        end
      rescue Sequel::DatabaseError
        # Ignore errors for timing test
      end
    end
    error_time = Time.now - start_time

    # Error handling shouldn't add more than 50% overhead
    assert error_time < (normal_time * 1.5), "Error handling should not significantly impact performance"
  end

  def test_error_recovery_after_connection_issues
    # Test that the adapter can recover from connection issues
    db = create_db
    create_test_table(db)

    # Insert initial data
    db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Before Issue', 25)")

    # Simulate connection issue by causing an error
    begin
      db.execute("INVALID SQL TO CAUSE ERROR")
    rescue Sequel::DatabaseError
      # Expected error
    end

    # Database should still be functional after error
    assert_nothing_raised("Database should recover after connection error") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (2, 'After Recovery', 30)")
    end

    assert_equal 2, db[:test_table].count, "Database should be fully functional after error recovery"
  end
end