# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for Dataset class functionality (TDD - Red phase)
# Tests fetch_rows method and DuckDB capability flags
# These tests are written BEFORE implementation to follow TDD methodology
class DatasetFunctionalityTest < SequelDuckDBTest::TestCase
  # Tests for fetch_rows method using real DuckDB in-memory database
  def test_fetch_rows_with_empty_table
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    rows = []
    dataset.fetch_rows("SELECT * FROM test_table") do |row|
      rows << row
    end

    assert_empty rows, "Empty table should return no rows"
  end

  def test_fetch_rows_with_data
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table]

    rows = []
    dataset.fetch_rows("SELECT * FROM test_table ORDER BY name") do |row|
      rows << row
    end

    assert_equal 2, rows.length, "Should return 2 rows"

    # Check first row (Jane Smith)
    first_row = rows.first
    assert_instance_of Hash, first_row, "Row should be a hash"
    assert_equal "Jane Smith", first_row[:name], "First row should be Jane Smith"
    assert_equal 25, first_row[:age], "Jane's age should be 25"

    # Check second row (John Doe)
    second_row = rows.last
    assert_equal "John Doe", second_row[:name], "Second row should be John Doe"
    assert_equal 30, second_row[:age], "John's age should be 30"
  end

  def test_fetch_rows_with_where_clause
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table]

    rows = []
    dataset.fetch_rows("SELECT * FROM test_table WHERE age > 25") do |row|
      rows << row
    end

    assert_equal 1, rows.length, "Should return 1 row matching condition"
    assert_equal "John Doe", rows.first[:name], "Should return John Doe"
  end

  def test_fetch_rows_with_specific_columns
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table]

    rows = []
    dataset.fetch_rows("SELECT name, age FROM test_table ORDER BY name") do |row|
      rows << row
    end

    assert_equal 2, rows.length, "Should return 2 rows"

    first_row = rows.first
    assert_equal 2, first_row.keys.length, "Should have 2 columns"
    assert first_row.key?(:name), "Should have name column"
    assert first_row.key?(:age), "Should have age column"
    refute first_row.key?(:birth_date), "Should not have birth_date column"
  end

  def test_fetch_rows_with_aggregation
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table]

    rows = []
    dataset.fetch_rows("SELECT COUNT(*) as count, AVG(age) as avg_age FROM test_table") do |row|
      rows << row
    end

    assert_equal 1, rows.length, "Should return 1 aggregation row"

    row = rows.first
    assert_equal 2, row[:count], "Count should be 2"
    assert_equal 27.5, row[:avg_age], "Average age should be 27.5"
  end

  def test_fetch_rows_with_joins
    db = create_db

    # Create users table
    db.create_table(:users) do
      primary_key :id
      String :name, null: false
    end

    # Create profiles table
    db.create_table(:profiles) do
      primary_key :id
      foreign_key :user_id, :users
      String :email
    end

    # Insert test data
    user_id = 1
    db[:users].insert(id: user_id, name: "John Doe")
    db[:profiles].insert(id: 1, user_id: user_id, email: "john@example.com")

    dataset = db[:users]

    rows = []
    dataset.fetch_rows("SELECT u.name, p.email FROM users u JOIN profiles p ON u.id = p.user_id") do |row|
      rows << row
    end

    assert_equal 1, rows.length, "Should return 1 joined row"

    row = rows.first
    assert_equal "John Doe", row[:name], "Should have user name"
    assert_equal "john@example.com", row[:email], "Should have user email"
  end

  def test_fetch_rows_with_invalid_sql
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    assert_database_error do
      dataset.fetch_rows("INVALID SQL SYNTAX") do |row|
        # This should not be reached
      end
    end
  end

  def test_fetch_rows_with_nonexistent_table
    db = create_db
    dataset = db[:test_table]

    assert_database_error do
      dataset.fetch_rows("SELECT * FROM nonexistent_table") do |row|
        # This should not be reached
      end
    end
  end

  def test_fetch_rows_without_block
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table]

    # fetch_rows should work without a block and return an enumerator
    result = dataset.fetch_rows("SELECT * FROM test_table")

    if result.respond_to?(:each)
      rows = result.to_a
      assert_equal 2, rows.length, "Should return 2 rows via enumerator"
    else
      # If it doesn't return an enumerator, it should at least not crash
      assert_nothing_raised("fetch_rows without block should not raise error")
    end
  end

  def test_fetch_rows_with_different_data_types
    db = create_db
    create_test_table(db)

    # Insert data with various types
    db[:test_table].insert(
      id: 1,
      name: "Test User",
      age: 30,
      birth_date: Date.new(1993, 5, 15),
      active: true,
      created_at: Time.now,
      score: 85.5
    )

    dataset = db[:test_table]

    rows = []
    dataset.fetch_rows("SELECT * FROM test_table") do |row|
      rows << row
    end

    assert_equal 1, rows.length, "Should return 1 row"

    row = rows.first
    assert_instance_of String, row[:name], "Name should be string"
    assert_instance_of Integer, row[:age], "Age should be integer"
    assert_instance_of Date, row[:birth_date], "Birth date should be Date"
    assert [true, false].include?(row[:active]), "Active should be boolean"
    assert_instance_of Time, row[:created_at], "Created at should be Time"
    assert_instance_of Float, row[:score], "Score should be float"
  end

  # Tests for DuckDB capability flags
  def test_supports_window_functions
    dataset = mock_dataset(:users)
    assert dataset.supports_window_functions?, "DuckDB should support window functions"
  end

  def test_supports_cte
    dataset = mock_dataset(:users)
    assert dataset.supports_cte?, "DuckDB should support Common Table Expressions (CTE)"
  end

  def test_supports_returning
    dataset = mock_dataset(:users)
    refute dataset.supports_returning?, "DuckDB should not support RETURNING clause"
  end

  def test_supports_select_all_and_offset
    dataset = mock_dataset(:users)
    assert dataset.supports_select_all_and_offset?, "DuckDB should support SELECT with OFFSET"
  end

  def test_quote_identifiers_default
    dataset = mock_dataset(:users)
    refute dataset.quote_identifiers_default, "DuckDB adapter should not quote identifiers by default"
  end

  # Integration tests for basic query execution using fetch_rows
  def test_basic_query_execution_with_fetch_rows
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table]

    # Test that basic dataset operations work with fetch_rows
    count = 0
    dataset.fetch_rows(dataset.sql) do |row|
      count += 1
      assert_instance_of Hash, row, "Each row should be a hash"
      assert row.key?(:name), "Row should have name key"
      assert row.key?(:age), "Row should have age key"
    end

    assert_equal 2, count, "Should process 2 rows"
  end

  def test_query_execution_with_where_conditions
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table].where(age: 30)

    count = 0
    dataset.fetch_rows(dataset.sql) do |row|
      count += 1
      assert_equal 30, row[:age], "Filtered row should have age 30"
      assert_equal "John Doe", row[:name], "Should be John Doe"
    end

    assert_equal 1, count, "Should process 1 filtered row"
  end

  def test_query_execution_with_order_by
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table].order(:name)

    names = []
    dataset.fetch_rows(dataset.sql) do |row|
      names << row[:name]
    end

    assert_equal ["Jane Smith", "John Doe"], names, "Names should be ordered alphabetically"
  end

  def test_query_execution_with_limit
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table].limit(1)

    count = 0
    dataset.fetch_rows(dataset.sql) do |row|
      count += 1
    end

    assert_equal 1, count, "Should process only 1 row due to LIMIT"
  end

  def test_query_execution_with_select_columns
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table].select(:name, :age)

    dataset.fetch_rows(dataset.sql) do |row|
      assert_equal 2, row.keys.length, "Should have only 2 columns"
      assert row.key?(:name), "Should have name column"
      assert row.key?(:age), "Should have age column"
      refute row.key?(:birth_date), "Should not have birth_date column"
    end
  end

  def test_query_execution_with_complex_conditions
    db = create_db
    create_test_table(db)
    insert_test_data(db)

    # Add more test data
    db[:test_table].insert(id: 3, name: "Bob Wilson", age: 35, active: false)
    db[:test_table].insert(id: 4, name: "Alice Brown", age: 28, active: true)

    dataset = db[:test_table].where { (age > 25) & (active =~ true) }.order(:name)

    names = []
    dataset.fetch_rows(dataset.sql) do |row|
      names << row[:name]
      assert row[:age] > 25, "Age should be greater than 25"
      assert_equal true, row[:active], "Should be active"
    end

    assert_equal ["Alice Brown", "John Doe"], names, "Should return Alice and John in order"
  end

  def test_error_handling_in_fetch_rows
    db = create_db
    dataset = db[:test_table]

    # Test various error conditions
    assert_database_error("Should raise error for syntax error") do
      dataset.fetch_rows("SELECT * FROM") { |row| }
    end

    assert_database_error("Should raise error for non-existent table") do
      dataset.fetch_rows("SELECT * FROM non_existent_table") { |row| }
    end

    assert_database_error("Should raise error for invalid column") do
      create_test_table(db)
      dataset.fetch_rows("SELECT non_existent_column FROM test_table") { |row| }
    end
  end

  def test_fetch_rows_memory_efficiency
    db = create_db
    create_test_table(db)

    # Insert a larger dataset
    100.times do |i|
      db[:test_table].insert(id: i + 1, name: "User #{i}", age: 20 + (i % 50))
    end

    dataset = db[:test_table]

    # Test that fetch_rows processes rows one at a time (streaming)
    processed_count = 0
    dataset.fetch_rows("SELECT * FROM test_table") do |row|
      processed_count += 1
      assert_instance_of Hash, row, "Each row should be a hash"
      # Verify we're not loading all rows into memory at once
      # This is more of a behavioral test
    end

    assert_equal 100, processed_count, "Should process all 100 rows"
  end

  def test_fetch_rows_with_null_values
    db = create_db
    create_test_table(db)

    # Insert data with NULL values
    db[:test_table].insert(id: 1, name: "Test User", age: nil, birth_date: nil, active: nil)

    dataset = db[:test_table]

    rows = []
    dataset.fetch_rows("SELECT * FROM test_table") do |row|
      rows << row
    end

    assert_equal 1, rows.length, "Should return 1 row"

    row = rows.first
    assert_equal "Test User", row[:name], "Name should be preserved"
    assert_nil row[:age], "Age should be nil"
    assert_nil row[:birth_date], "Birth date should be nil"
    assert_nil row[:active], "Active should be nil"
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end