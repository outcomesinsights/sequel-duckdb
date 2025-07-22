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
    isolation_levels = [:read_uncommitted, :read_committed, :repeatable_read, :serializable]

    isolation_levels.each do |level|
      if db.supports_transaction_isolation_level?(level)
        assert_nothing_raised("Should support #{level} isolation level") do
          db.transaction(isolation: level) do
            db[:test_table].insert(id: 1, name: "Isolation Test", age: 30)
          end
        end

        # Clean up for next test
        db[:test_table].delete
      else
        # If isolation level isn't supported, transaction should still work without it
        assert_nothing_raised("Should work even if #{level} isolation level isn't supported") do
          db.transaction do
            db[:test_table].insert(id: 1, name: "No Isolation Test", age: 30)
          end
        end

        # Clean up for next test
        db[:test_table].delete
      end
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

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end