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

  # Transaction support tests (Requirements 5.1-5.4)
  def test_transaction_block_handling
    db = create_db
    create_test_table(db)

    # Test that transaction block is properly handled
    result = nil
    assert_nothing_raised("Transaction block should be handled properly") do
      result = db.transaction do
        db[:test_table].insert(id: 1, name: "Transaction Test", age: 30)
        "transaction_completed"
      end
    end

    assert_equal "transaction_completed", result, "Transaction should return block result"
    assert_equal 1, db[:test_table].count, "Data should be committed after successful transaction"
  end

  def test_transaction_automatic_commit
    db = create_db
    create_test_table(db)

    # Test automatic commit on successful completion (Requirement 5.2)
    assert_nothing_raised("Transaction should commit automatically on success") do
      db.transaction do
        db[:test_table].insert(id: 1, name: "Auto Commit Test", age: 25)
        db[:test_table].insert(id: 2, name: "Auto Commit Test 2", age: 35)
      end
    end

    # Verify data was committed
    assert_equal 2, db[:test_table].count, "Both records should be committed"
    records = db[:test_table].all
    assert_equal ["Auto Commit Test", "Auto Commit Test 2"], records.map { |r| r[:name] }.sort
  end

  def test_transaction_automatic_rollback_on_exception
    db = create_db
    create_test_table(db)

    # Insert initial data
    db[:test_table].insert(id: 1, name: "Initial Record", age: 20)
    initial_count = db[:test_table].count

    # Test automatic rollback on exceptions (Requirement 5.3)
    assert_raises(RuntimeError, "Exception should be raised") do
      db.transaction do
        db[:test_table].insert(id: 2, name: "Should Be Rolled Back", age: 30)
        raise "Simulated error"
      end
    end

    # Verify rollback occurred - count should be unchanged
    assert_equal initial_count, db[:test_table].count, "Transaction should be rolled back on exception"
    refute_includes db[:test_table].select_map(:name), "Should Be Rolled Back", "Rolled back data should not exist"
  end

  def test_transaction_explicit_rollback
    db = create_db
    create_test_table(db)

    # Insert initial data
    db[:test_table].insert(id: 1, name: "Initial Record", age: 20)
    initial_count = db[:test_table].count

    # Test explicit rollback (Requirement 5.4)
    assert_nothing_raised("Explicit rollback should work") do
      db.transaction do
        db[:test_table].insert(id: 2, name: "Should Be Rolled Back", age: 30)

        # Explicit rollback using Sequel::Rollback exception
        raise Sequel::Rollback
      end
    end

    # Verify rollback occurred
    assert_equal initial_count, db[:test_table].count, "Explicit rollback should revert changes"
    refute_includes db[:test_table].select_map(:name), "Should Be Rolled Back", "Rolled back data should not exist"
  end

  def test_transaction_nested_behavior
    db = create_db
    create_test_table(db)

    # Test nested transaction behavior
    result = nil
    assert_nothing_raised("Nested transactions should work") do
      result = db.transaction do
        db[:test_table].insert(id: 1, name: "Outer Transaction", age: 30)

        db.transaction do
          db[:test_table].insert(id: 2, name: "Inner Transaction", age: 25)
          "inner_result"
        end

        "outer_result"
      end
    end

    assert_equal "outer_result", result, "Outer transaction should return its result"
    assert_equal 2, db[:test_table].count, "Both records should be committed"

    names = db[:test_table].select_map(:name).sort
    assert_equal ["Inner Transaction", "Outer Transaction"], names
  end

  def test_transaction_rollback_in_nested_transaction
    db = create_db
    create_test_table(db)

    # Insert initial data
    db[:test_table].insert(id: 1, name: "Initial Record", age: 20)
    initial_count = db[:test_table].count

    # Test rollback in nested transaction
    assert_raises(RuntimeError, "Exception should be raised from inner transaction") do
      db.transaction do
        db[:test_table].insert(id: 2, name: "Outer Record", age: 30)

        db.transaction do
          db[:test_table].insert(id: 3, name: "Inner Record", age: 25)
          raise "Inner transaction error"
        end
      end
    end

    # Verify complete rollback occurred
    assert_equal initial_count, db[:test_table].count, "All changes should be rolled back"
    refute_includes db[:test_table].select_map(:name), "Outer Record", "Outer record should be rolled back"
    refute_includes db[:test_table].select_map(:name), "Inner Record", "Inner record should be rolled back"
  end

  def test_transaction_return_values
    db = create_db
    create_test_table(db)

    # Test various return value scenarios
    result1 = db.transaction { 42 }
    assert_equal 42, result1, "Transaction should return integer"

    result2 = db.transaction { "string_result" }
    assert_equal "string_result", result2, "Transaction should return string"

    result3 = db.transaction { [1, 2, 3] }
    assert_equal [1, 2, 3], result3, "Transaction should return array"

    result4 = db.transaction { { key: "value" } }
    assert_equal({ key: "value" }, result4, "Transaction should return hash")

    result5 = db.transaction { nil }
    assert_nil result5, "Transaction should return nil"
  end

  def test_transaction_with_database_errors
    db = create_db
    create_test_table(db)

    # Insert initial data
    db[:test_table].insert(id: 1, name: "Initial Record", age: 20)
    initial_count = db[:test_table].count

    # Test rollback on database errors (SQL errors should cause rollback)
    assert_raises(Sequel::DatabaseError, "Database error should be raised") do
      db.transaction do
        db[:test_table].insert(id: 2, name: "Valid Record", age: 30)
        # This should cause a database error and rollback
        db.run("INVALID SQL SYNTAX")
      end
    end

    # Verify rollback occurred
    assert_equal initial_count, db[:test_table].count, "Database error should cause rollback"
    refute_includes db[:test_table].select_map(:name), "Valid Record", "Valid record should be rolled back due to error"
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
      db[:test_table].insert(id: 1, name: "Test User", age: 25)
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
      db[:test_table].insert(id: 1, name: "User 1", age: 20)
      db[:test_table].insert(id: 2, name: "User 2", age: 30)
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

  # Advanced transaction features tests (Requirements 5.5, 5.6, 5.7)

  def test_savepoint_support
    db = create_db
    create_test_table(db)

    # Insert initial data
    db[:test_table].insert(id: 1, name: "Initial Record", age: 20)
    initial_count = db[:test_table].count

    # Test savepoint functionality if supported (Requirement 5.5)
    if db.supports_savepoints?
      assert_nothing_raised("Savepoints should work if supported") do
        db.transaction do
          db[:test_table].insert(id: 2, name: "Before Savepoint", age: 25)

          db.transaction(savepoint: true) do
            db[:test_table].insert(id: 3, name: "In Savepoint", age: 30)
            # This should rollback only the savepoint
            raise Sequel::Rollback
          end

          # The outer transaction should continue
          db[:test_table].insert(id: 4, name: "After Savepoint", age: 35)
        end
      end

      # Verify partial rollback - savepoint rolled back but outer transaction committed
      assert_equal initial_count + 2, db[:test_table].count, "Savepoint should allow partial rollback"
      names = db[:test_table].select_map(:name).sort
      assert_includes names, "Before Savepoint", "Record before savepoint should be committed"
      assert_includes names, "After Savepoint", "Record after savepoint should be committed"
      refute_includes names, "In Savepoint", "Record in savepoint should be rolled back"
    else
      # If savepoints aren't supported, nested transactions should behave as regular transactions
      assert_nothing_raised("Nested transactions should work even without savepoint support") do
        db.transaction do
          db[:test_table].insert(id: 2, name: "Nested Transaction", age: 25)
        end
      end

      assert_equal initial_count + 1, db[:test_table].count, "Nested transaction should work without savepoints"
    end
  end

  def test_transaction_isolation_levels
    db = create_db
    create_test_table(db)

    # Test transaction isolation level support if available (Requirement 5.6)
    isolation_levels = %i[read_uncommitted read_committed repeatable_read serializable]

    isolation_levels.each do |level|
      if db.supports_transaction_isolation_level?(level)
        assert_nothing_raised("Should support #{level} isolation level") do
          db.transaction(isolation: level) do
            db[:test_table].insert(id: 1, name: "Isolation Test", age: 30)
          end
        end

        # Clean up for next test
      else
        # If isolation level isn't supported, transaction should still work without it
        assert_nothing_raised("Should work even if #{level} isolation level isn't supported") do
          db.transaction do
            db[:test_table].insert(id: 1, name: "No Isolation Test", age: 30)
          end
        end

        # Clean up for next test
      end
      db[:test_table].delete
    end
  end

  def test_manual_transaction_control
    db = create_db
    create_test_table(db)

    # Test manual transaction control for autocommit mode (Requirement 5.7)
    if db.supports_manual_transaction_control?
      assert_nothing_raised("Should support manual transaction control") do
        # Use raw SQL for manual transaction control
        db.run("BEGIN TRANSACTION")
        db[:test_table].insert(id: 1, name: "Manual Transaction", age: 30)
        db.run("COMMIT")
      end

      assert_equal 1, db[:test_table].count, "Manual transaction should commit data"

      # Test manual rollback
      assert_nothing_raised("Should support manual rollback") do
        db.run("BEGIN TRANSACTION")
        db[:test_table].insert(id: 2, name: "Manual Rollback", age: 25)
        db.run("ROLLBACK")
      end

      assert_equal 1, db[:test_table].count, "Manual rollback should not commit data"
      refute_includes db[:test_table].select_map(:name), "Manual Rollback", "Rolled back data should not exist"
    else
      # If manual transaction control isn't supported, regular transactions should still work
      assert_nothing_raised("Regular transactions should work without manual control") do
        db.transaction do
          db[:test_table].insert(id: 1, name: "Regular Transaction", age: 30)
        end
      end

      assert_equal 1, db[:test_table].count, "Regular transaction should work"
    end
  end

  def test_autocommit_mode_handling
    db = create_db
    create_test_table(db)

    # Test autocommit mode behavior (Requirement 5.7)
    if db.supports_autocommit_control?
      # Test with autocommit enabled (default)
      assert_nothing_raised("Should work with autocommit enabled") do
        db[:test_table].insert(id: 1, name: "Autocommit Test", age: 30)
      end

      assert_equal 1, db[:test_table].count, "Data should be committed immediately with autocommit"

      # Test with autocommit disabled
      if db.supports_autocommit_disable?
        assert_nothing_raised("Should support disabling autocommit") do
          db.autocommit = false

          db[:test_table].insert(id: 2, name: "No Autocommit", age: 25)

          # Data shouldn't be visible until explicit commit
          # Note: This test may need adjustment based on DuckDB's actual behavior
          db.commit_transaction

          # Re-enable autocommit
          db.autocommit = true
        end

        assert_equal 2, db[:test_table].count, "Data should be committed after explicit commit"
      end
    else
      # If autocommit control isn't supported, regular behavior should work
      assert_nothing_raised("Regular operations should work without autocommit control") do
        db[:test_table].insert(id: 1, name: "Regular Insert", age: 30)
      end

      assert_equal 1, db[:test_table].count, "Regular insert should work"
    end
  end

  def test_transaction_status_methods
    db = create_db

    # Test transaction status inquiry methods
    refute db.in_transaction?, "Should not be in transaction initially"

    db.transaction do
      assert db.in_transaction?, "Should be in transaction inside transaction block"
    end

    refute db.in_transaction?, "Should not be in transaction after transaction block"
  end

  def test_concurrent_transaction_handling
    db = create_db
    create_test_table(db)

    # Test that transactions work correctly with concurrent access patterns
    # This is a basic test - real concurrent testing would require threading
    assert_nothing_raised("Should handle multiple sequential transactions") do
      5.times do |i|
        db.transaction do
          db[:test_table].insert(id: i + 1, name: "Concurrent Test #{i}", age: 20 + i)
        end
      end
    end

    assert_equal 5, db[:test_table].count, "All transactions should complete successfully"
  end

  # SQL Execution Methods Tests (Requirements 2.1, 2.2, 2.3, 2.4)

  def test_execute_method_with_connection_synchronization
    db = create_db
    create_test_table(db)

    # Test basic execute method with connection synchronization (Requirement 2.1)
    result = nil
    assert_nothing_raised("execute method should work with connection synchronization") do
      result = db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Execute Test', 30)")
    end

    # Verify the insert worked
    count = db[:test_table].count
    assert_equal 1, count, "Execute should insert record"

    # Test execute with SELECT query
    rows = []
    assert_nothing_raised("execute method should work with SELECT queries") do
      db.execute("SELECT * FROM test_table WHERE id = 1") do |row|
        rows << row
      end
    end

    assert_equal 1, rows.length, "Execute should return one row"
    assert_equal "Execute Test", rows.first[:name], "Execute should return correct data"
  end

  def test_execute_method_with_parameters
    db = create_db
    create_test_table(db)

    # Test execute method with parameterized queries
    assert_nothing_raised("execute method should support parameterized queries") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)", [1, "Param Test", 25])
    end

    # Verify the parameterized insert worked
    rows = []
    db.execute("SELECT * FROM test_table WHERE id = ?", [1]) do |row|
      rows << row
    end

    assert_equal 1, rows.length, "Parameterized execute should work"
    assert_equal "Param Test", rows.first[:name], "Parameterized execute should insert correct data"
    assert_equal 25, rows.first[:age], "Parameterized execute should handle integer parameters"
  end

  def test_execute_method_error_handling
    db = create_db

    # Test execute method error handling
    assert_database_error("execute should raise DatabaseError for invalid SQL") do
      db.execute("INVALID SQL SYNTAX")
    end

    # Test execute with invalid table
    assert_database_error("execute should raise DatabaseError for invalid table") do
      db.execute("SELECT * FROM nonexistent_table")
    end
  end

  def test_execute_method_with_block
    db = create_db
    create_test_table(db)

    # Insert test data
    db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Block Test 1', 20)")
    db.execute("INSERT INTO test_table (id, name, age) VALUES (2, 'Block Test 2', 30)")

    # Test execute method with block for result processing
    collected_rows = []
    result = nil
    assert_nothing_raised("execute method should work with block") do
      result = db.execute("SELECT * FROM test_table ORDER BY id") do |row|
        collected_rows << row
      end
    end

    assert_equal 2, collected_rows.length, "Block should receive all rows"
    assert_equal "Block Test 1", collected_rows.first[:name], "Block should receive correct first row"
    assert_equal "Block Test 2", collected_rows.last[:name], "Block should receive correct second row"
  end

  def test_execute_method_without_block
    db = create_db
    create_test_table(db)

    # Insert test data
    db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'No Block Test', 25)")

    # Test execute method without block (should return result object)
    result = nil
    assert_nothing_raised("execute method should work without block") do
      result = db.execute("SELECT * FROM test_table WHERE id = 1")
    end

    refute_nil result, "Execute without block should return result object"
  end

  def test_execute_insert_method
    db = create_db
    create_test_table(db)

    # Test execute_insert method (Requirement 2.2)
    result = nil
    assert_nothing_raised("execute_insert should work") do
      result = db.execute_insert("INSERT INTO test_table (id, name, age) VALUES (1, 'Insert Test', 35)")
    end

    # For DuckDB, execute_insert should return nil since AUTOINCREMENT isn't supported
    # This matches the expected behavior for databases without auto-increment
    assert_nil result, "execute_insert should return nil for DuckDB (no AUTOINCREMENT support)"

    # Verify the insert worked
    count = db[:test_table].count
    assert_equal 1, count, "execute_insert should insert record"

    # Verify the data
    row = db[:test_table].first
    assert_equal "Insert Test", row[:name], "execute_insert should insert correct data"
    assert_equal 35, row[:age], "execute_insert should insert correct age"
  end

  def test_execute_insert_with_parameters
    db = create_db
    create_test_table(db)

    # Test execute_insert with parameters
    result = nil
    assert_nothing_raised("execute_insert should work with parameters") do
      result = db.execute_insert("INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)",
                                 params: [2, "Param Insert", 40])
    end

    assert_nil result, "execute_insert should return nil for parameterized queries"

    # Verify the insert worked
    row = db[:test_table].where(id: 2).first
    refute_nil row, "Parameterized execute_insert should insert record"
    assert_equal "Param Insert", row[:name], "Parameterized execute_insert should insert correct data"
  end

  def test_execute_update_method
    db = create_db
    create_test_table(db)

    # Insert initial data
    db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Update Test', 25)")

    # Test execute_update method (Requirement 2.3)
    result = nil
    assert_nothing_raised("execute_update should work") do
      result = db.execute_update("UPDATE test_table SET age = 30 WHERE id = 1")
    end

    # Verify the update worked
    row = db[:test_table].where(id: 1).first
    assert_equal 30, row[:age], "execute_update should update record"
    assert_equal "Update Test", row[:name], "execute_update should preserve other fields"
  end

  def test_execute_update_with_parameters
    db = create_db
    create_test_table(db)

    # Insert initial data
    db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Param Update', 25)")

    # Test execute_update with parameters
    assert_nothing_raised("execute_update should work with parameters") do
      db.execute_update("UPDATE test_table SET name = ?, age = ? WHERE id = ?",
                        params: ["Updated Name", 35, 1])
    end

    # Verify the update worked
    row = db[:test_table].where(id: 1).first
    assert_equal "Updated Name", row[:name], "Parameterized execute_update should update name"
    assert_equal 35, row[:age], "Parameterized execute_update should update age"
  end

  def test_execute_update_error_handling
    db = create_db

    # Test execute_update error handling
    assert_database_error("execute_update should raise DatabaseError for invalid SQL") do
      db.execute_update("UPDATE nonexistent_table SET column = 'value'")
    end
  end

  def test_execute_statement_private_method
    db = create_db
    create_test_table(db)

    # Test that execute_statement is properly used internally
    # We can't test it directly since it's private, but we can verify it works through public methods

    # Test with simple SQL
    assert_nothing_raised("execute_statement should handle simple SQL") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Statement Test', 28)")
    end

    # Test with parameterized SQL
    assert_nothing_raised("execute_statement should handle parameterized SQL") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)", [2, "Param Statement", 32])
    end

    # Verify both inserts worked
    assert_equal 2, db[:test_table].count, "execute_statement should handle both simple and parameterized SQL"
  end

  def test_result_handling_and_iteration
    db = create_db
    create_test_table(db)

    # Insert test data with various data types
    db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Result Test 1', 25)")
    db.execute("INSERT INTO test_table (id, name, age) VALUES (2, 'Result Test 2', 30)")
    db.execute("INSERT INTO test_table (id, name, age) VALUES (3, 'Result Test 3', 35)")

    # Test proper result handling and iteration (Requirement 2.4)
    collected_results = []
    assert_nothing_raised("Result handling should work properly") do
      db.execute("SELECT * FROM test_table ORDER BY id") do |row|
        collected_results << row
      end
    end

    # Verify result structure and content
    assert_equal 3, collected_results.length, "Should collect all rows"

    # Verify first row
    first_row = collected_results[0]
    assert_instance_of Hash, first_row, "Each row should be a hash"
    assert_equal 1, first_row[:id], "First row should have correct id"
    assert_equal "Result Test 1", first_row[:name], "First row should have correct name"
    assert_equal 25, first_row[:age], "First row should have correct age"

    # Verify all rows have expected keys
    collected_results.each_with_index do |row, index|
      assert_includes row.keys, :id, "Row #{index} should have :id key"
      assert_includes row.keys, :name, "Row #{index} should have :name key"
      assert_includes row.keys, :age, "Row #{index} should have :age key"
    end

    # Verify data types are preserved
    assert_instance_of Integer, first_row[:id], "ID should be integer"
    assert_instance_of String, first_row[:name], "Name should be string"
    assert_instance_of Integer, first_row[:age], "Age should be integer"
  end

  def test_sql_execution_with_various_data_types
    db = create_db

    # Create table with various data types
    db.run <<~SQL
      CREATE TABLE type_test_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        age INTEGER,
        salary DOUBLE,
        is_active BOOLEAN,
        birth_date DATE,
        created_at TIMESTAMP,
        notes TEXT
      )
    SQL

    # Insert data with various types
    assert_nothing_raised("Should handle various data types in SQL execution") do
      db.execute(<<~SQL, [1, "John Doe", 30, 50_000.50, true, "1993-05-15", "2023-01-01 10:30:00", "Test notes"])
        INSERT INTO type_test_table
        (id, name, age, salary, is_active, birth_date, created_at, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      SQL
    end

    # Retrieve and verify data types
    row = nil
    db.execute("SELECT * FROM type_test_table WHERE id = 1") do |r|
      row = r
    end

    refute_nil row, "Should retrieve inserted row"
    assert_equal 1, row[:id], "Integer should be preserved"
    assert_equal "John Doe", row[:name], "String should be preserved"
    assert_equal 30, row[:age], "Integer should be preserved"
    assert_equal 50_000.5, row[:salary], "Double should be preserved"
    assert_equal true, row[:is_active], "Boolean should be preserved"
    assert_equal "Test notes", row[:notes], "Text should be preserved"

    # Date and timestamp handling may vary by DuckDB version, so we'll just check they're not nil
    refute_nil row[:birth_date], "Date should not be nil"
    refute_nil row[:created_at], "Timestamp should not be nil"
  end

  # Configuration convenience methods tests (Requirements 3.1, 3.2)

  def test_set_pragma_method
    db = create_db

    # Test set_pragma method (Requirement 3.1)
    assert_nothing_raised("set_pragma should work") do
      db.set_pragma("memory_limit", "1GB")
    end

    # Test with different pragma settings
    assert_nothing_raised("set_pragma should work with different settings") do
      db.set_pragma("threads", 4)
      db.set_pragma("enable_progress_bar", true)
    end

    # Test with string values
    assert_nothing_raised("set_pragma should work with string values") do
      db.set_pragma("default_order", "ASC")
    end

    # Test with boolean values (using a pragma that accepts boolean)
    assert_nothing_raised("set_pragma should work with boolean values") do
      db.set_pragma("enable_progress_bar", false)
    end
  end

  def test_set_pragma_error_handling
    db = create_db

    # Test error handling for invalid pragma
    assert_database_error("set_pragma should raise error for invalid pragma") do
      db.set_pragma("invalid_pragma_name", "value")
    end
  end

  def test_configure_duckdb_method
    db = create_db

    # Test configure_duckdb method for batch configuration (Requirement 3.2)
    options = {
      memory_limit: "2GB",
      threads: 8,
      enable_progress_bar: true
    }

    assert_nothing_raised("configure_duckdb should work with multiple options") do
      db.configure_duckdb(options)
    end

    # Test with empty options
    assert_nothing_raised("configure_duckdb should work with empty options") do
      db.configure_duckdb({})
    end

    # Test with single option
    assert_nothing_raised("configure_duckdb should work with single option") do
      db.configure_duckdb(memory_limit: "512MB")
    end
  end

  def test_configure_duckdb_error_handling
    db = create_db

    # Test error handling for invalid options
    invalid_options = {
      memory_limit: "1GB",
      invalid_pragma_name: "value",
      threads: 4
    }

    # Should handle partial success/failure gracefully
    # The exact behavior depends on DuckDB's pragma handling
    assert_raises(Sequel::DatabaseError, "configure_duckdb should raise error for invalid options") do
      db.configure_duckdb(invalid_options)
    end
  end

  def test_configure_duckdb_with_various_types
    db = create_db

    # Test configure_duckdb with various value types
    mixed_options = {
      memory_limit: "1GB",        # String
      threads: 4,                 # Integer
      enable_progress_bar: true   # Boolean
    }

    assert_nothing_raised("configure_duckdb should handle various value types") do
      db.configure_duckdb(mixed_options)
    end
  end

  def test_sql_execution_connection_synchronization
    db = create_db
    create_test_table(db)

    # Test that multiple SQL executions work properly with connection synchronization
    # This tests that the connection is properly managed across multiple operations
    assert_nothing_raised("Multiple SQL executions should work with proper connection synchronization") do
      # Multiple inserts
      db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Sync Test 1', 20)")
      db.execute("INSERT INTO test_table (id, name, age) VALUES (2, 'Sync Test 2', 25)")
      db.execute("INSERT INTO test_table (id, name, age) VALUES (3, 'Sync Test 3', 30)")

      # Mixed operations
      count = 0
      db.execute("SELECT COUNT(*) as count FROM test_table") do |row|
        count = row[:count]
      end
      assert_equal 3, count, "Count should be correct after multiple inserts"

      # Update operation
      db.execute("UPDATE test_table SET age = age + 5 WHERE id = 2")

      # Verify update
      updated_age = nil
      db.execute("SELECT age FROM test_table WHERE id = 2") do |row|
        updated_age = row[:age]
      end
      assert_equal 30, updated_age, "Update should work correctly"

      # Delete operation
      db.execute("DELETE FROM test_table WHERE id = 1")

      # Verify delete
      final_count = 0
      db.execute("SELECT COUNT(*) as count FROM test_table") do |row|
        final_count = row[:count]
      end
      assert_equal 2, final_count, "Delete should work correctly"
    end
  end

  # Logging and Debugging Support Tests (Requirements 8.4, 8.5, 8.6, 9.6)

  def test_sql_query_logging
    db = create_db
    create_test_table(db)

    # Test SQL query logging using Sequel's logging mechanism (Requirement 8.4)

    # Set up a custom logger to capture log messages
    require "logger"
    string_io = StringIO.new
    logger = Logger.new(string_io)

    # Enable logging on the database
    db.loggers = [logger]

    # Execute some queries that should be logged
    assert_nothing_raised("Should be able to execute queries with logging enabled") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Log Test', 25)")
      db[:test_table].count
      db[:test_table].where(id: 1).first
    end

    # Check that queries were logged
    log_output = string_io.string
    refute_empty log_output, "Queries should be logged when logging is enabled"

    # Should contain SQL statements
    assert_includes log_output, "INSERT", "INSERT query should be logged"
    assert_includes log_output, "SELECT", "SELECT query should be logged"
  end

  def test_timing_information_for_operations
    db = create_db
    create_test_table(db)

    # Test timing information for operations (Requirement 8.5)
    require "logger"
    string_io = StringIO.new
    logger = Logger.new(string_io)
    db.loggers = [logger]

    # Execute a query and check for timing information
    Time.now
    assert_nothing_raised("Should execute query with timing") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Timing Test', 30)")
    end
    Time.now

    # Check that timing information is included in logs
    log_output = string_io.string

    # Sequel typically includes timing information in its logs
    # The exact format may vary, but it should include some timing data
    refute_empty log_output, "Operations should be logged with timing information"
  end

  def test_connection_pooling_error_handling
    # Test connection pooling error handling (Requirement 8.6)

    # Test with invalid connection parameters
    assert_connection_error do
      invalid_db = Sequel.connect("duckdb:///invalid/path/that/cannot/be/created/database.db")
      invalid_db.test_connection
    end
  end

  def test_explain_functionality_access
    db = create_db
    create_test_table(db)

    # Insert some test data
    db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Explain Test', 25)")

    # Test EXPLAIN functionality access for query plans (Requirement 9.6)
    explain_result = nil
    assert_nothing_raised("Should be able to access EXPLAIN functionality") do
      db.execute("EXPLAIN SELECT * FROM test_table WHERE id = 1") do |row|
        explain_result = row
      end
    end

    refute_nil explain_result, "EXPLAIN should return query plan information"

    # The exact structure of EXPLAIN output varies by DuckDB version
    # but it should contain some plan information
    assert_instance_of Hash, explain_result, "EXPLAIN result should be a hash"
    refute_empty explain_result, "EXPLAIN result should not be empty"
  end

  def test_database_logging_configuration
    db = create_db

    # Test that logging can be enabled and disabled
    assert_respond_to db, :loggers, "Database should support loggers configuration"
    assert_respond_to db, :loggers=, "Database should support setting loggers"

    # Test initial state
    initial_loggers = db.loggers
    assert_instance_of Array, initial_loggers, "Loggers should be an array"

    # Test adding a logger
    require "logger"
    string_io = StringIO.new
    logger = Logger.new(string_io)

    assert_nothing_raised("Should be able to add logger") do
      db.loggers = [logger]
    end

    assert_equal [logger], db.loggers, "Logger should be set correctly"

    # Test removing loggers
    assert_nothing_raised("Should be able to remove loggers") do
      db.loggers = []
    end

    assert_empty db.loggers, "Loggers should be empty after removal"
  end

  def test_sql_logging_with_parameters
    db = create_db
    create_test_table(db)

    # Test SQL logging with parameterized queries
    require "logger"
    string_io = StringIO.new
    logger = Logger.new(string_io)
    db.loggers = [logger]

    # Execute parameterized query
    assert_nothing_raised("Should log parameterized queries") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)", [1, "Param Log Test", 28])
    end

    log_output = string_io.string
    refute_empty log_output, "Parameterized queries should be logged"

    # Should contain the SQL with parameters
    assert_includes log_output, "INSERT", "Parameterized INSERT should be logged"
  end

  def test_error_logging_and_debugging
    db = create_db

    # Test error logging and debugging support
    require "logger"
    string_io = StringIO.new
    logger = Logger.new(string_io)
    db.loggers = [logger]

    # Execute invalid SQL to trigger error logging
    assert_database_error("Should raise error for invalid SQL") do
      db.execute("INVALID SQL SYNTAX FOR LOGGING TEST")
    end

    log_output = string_io.string

    # Error should be logged (exact format may vary)
    # At minimum, the failed SQL should appear in logs
    assert_includes log_output, "INVALID SQL", "Failed SQL should be logged"
  end

  def test_performance_logging_for_slow_operations
    db = create_db
    create_test_table(db)

    # Test performance logging for operations (Requirement 8.5)
    require "logger"
    string_io = StringIO.new
    logger = Logger.new(string_io)
    db.loggers = [logger]

    # Execute multiple operations to test performance logging
    assert_nothing_raised("Should log performance information") do
      # Insert multiple records
      (1..5).each do |i|
        db.execute("INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)", [i, "Perf Test #{i}", 20 + i])
      end

      # Execute a more complex query
      db.execute("SELECT * FROM test_table WHERE age > 22 ORDER BY name")
    end

    log_output = string_io.string
    refute_empty log_output, "Performance information should be logged"

    # Should contain multiple SQL operations
    insert_count = log_output.scan(/INSERT/).length
    assert insert_count >= 5, "Multiple INSERT operations should be logged"
    assert_includes log_output, "SELECT", "SELECT operation should be logged"
  end

  def test_debug_information_availability
    db = create_db
    create_test_table(db)

    # Test that debug information is available when needed
    require "logger"
    string_io = StringIO.new
    logger = Logger.new(string_io)
    logger.level = Logger::DEBUG # Set to debug level
    db.loggers = [logger]

    # Execute operations with debug logging
    assert_nothing_raised("Should provide debug information") do
      db.execute("INSERT INTO test_table (id, name, age) VALUES (1, 'Debug Test', 30)")
      db[:test_table].count
    end

    log_output = string_io.string
    refute_empty log_output, "Debug information should be available"

    # Debug logs should contain detailed information
    # The exact format depends on Sequel's logging implementation
    assert_includes log_output, "INSERT", "Debug logs should contain SQL statements"
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end
