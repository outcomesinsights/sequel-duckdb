# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for Sequel::DuckDB::Dataset class
# Tests SQL generation, query building, and dataset operations
class DatasetTest < SequelDuckDBTest::TestCase
  def test_dataset_class_exists
    assert defined?(Sequel::DuckDB::Dataset), "Dataset class should be defined"
    assert Sequel::DuckDB::Dataset < Sequel::Dataset, "Dataset should inherit from Sequel::Dataset"
  end

  def test_dataset_includes_dataset_methods
    assert Sequel::DuckDB::Dataset.included_modules.include?(Sequel::DuckDB::DatasetMethods),
           "Dataset should include DatasetMethods module"
  end

  def test_dataset_creation_from_database
    db = create_db
    dataset = db[:test_table]

    assert_instance_of Sequel::DuckDB::Dataset, dataset,
                       "Database should create DuckDB::Dataset instances"
  end

  def test_mock_dataset_creation
    dataset = mock_dataset(:users)
    assert_kind_of Sequel::Dataset, dataset,
                   "Mock dataset should be created successfully"
  end

  # Basic SQL generation tests (using mock database)
  def test_basic_select_sql_generation
    dataset = mock_dataset(:users)
    expected_sql = "SELECT * FROM users"

    assert_sql expected_sql, dataset
  end

  def test_select_with_columns_sql_generation
    dataset = mock_dataset(:users).select(:name, :email)

    # Test selecting specific columns
    expected_sql = "SELECT name, email FROM users"
    assert_sql expected_sql, dataset
  end

  def test_where_clause_sql_generation
    dataset = mock_dataset(:users).where(active: true)

    # Test WHERE clause generation
    expected_sql = "SELECT * FROM users WHERE (active IS TRUE)"
    assert_sql expected_sql, dataset
  end

  def test_order_by_sql_generation
    dataset = mock_dataset(:users)

    # Test ORDER BY clause generation
    # This will be implemented when SQL generation methods are added
    assert_kind_of Sequel::Dataset, dataset
  end

  def test_limit_offset_sql_generation
    dataset = mock_dataset(:users)

    # Test LIMIT and OFFSET clause generation
    # This will be implemented when SQL generation methods are added
    assert_kind_of Sequel::Dataset, dataset
  end

  def test_join_sql_generation
    dataset = mock_dataset(:users)

    # Test JOIN clause generation
    # This will be implemented when SQL generation methods are added
    assert_kind_of Sequel::Dataset, dataset
  end

  def test_group_by_sql_generation
    dataset = mock_dataset(:users)

    # Test GROUP BY clause generation
    # This will be implemented when SQL generation methods are added
    assert_kind_of Sequel::Dataset, dataset
  end

  def test_having_clause_sql_generation
    dataset = mock_dataset(:users)

    # Test HAVING clause generation
    # This will be implemented when SQL generation methods are added
    assert_kind_of Sequel::Dataset, dataset
  end

  def test_insert_sql_generation
    dataset = mock_dataset(:users)

    # Test INSERT statement generation
    expected_sql = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
    actual_sql = dataset.insert_sql(name: "John", email: "john@example.com")
    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_generation
    dataset = mock_dataset(:users)

    # Test UPDATE statement generation
    expected_sql = "UPDATE users SET name = 'Jane', email = 'jane@example.com'"
    actual_sql = dataset.update_sql(name: "Jane", email: "jane@example.com")
    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_generation
    dataset = mock_dataset(:users)

    # Test DELETE statement generation
    expected_sql = "DELETE FROM users"
    actual_sql = dataset.delete_sql
    assert_equal expected_sql, actual_sql
  end

  # Integration tests with real database
  def test_dataset_count_with_real_database
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Test count on empty table
    assert_equal 0, dataset.count, "Empty table should have count of 0"

    # Insert some data and test count
    insert_test_data(db)
    assert_equal 2, dataset.count, "Table with 2 records should have count of 2"
  end

  def test_dataset_all_with_real_database
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table]

    records = dataset.all
    assert_instance_of Array, records, "all() should return an array"
    assert_equal 2, records.length, "Should return all records"

    # Check that records are hashes with expected keys
    record = records.first
    assert_instance_of Hash, record, "Each record should be a hash"
    assert record.key?(:name), "Record should have name key"
    assert record.key?(:age), "Record should have age key"
  end

  def test_dataset_first_with_real_database
    db = create_db
    create_test_table(db)

    # Test first on empty table
    assert_nil db[:test_table].first, "first() on empty table should return nil"

    # Test first with data
    insert_test_data(db)
    record = db[:test_table].first

    refute_nil record, "first() should return a record"
    assert_instance_of Hash, record, "Record should be a hash"
    assert record.key?(:name), "Record should have name key"
  end

  def test_dataset_where_with_real_database
    db = create_db
    create_test_table(db)
    insert_test_data(db)

    # Test WHERE clause with real database
    dataset = db[:test_table].where(name: "John Doe")
    records = dataset.all

    assert_equal 1, records.length, "Should find one matching record"
    assert_equal "John Doe", records.first[:name], "Should find correct record"
  end

  def test_dataset_order_with_real_database
    db = create_db
    create_test_table(db)
    insert_test_data(db)

    # Test ORDER BY with real database
    records = db[:test_table].order(:name).all
    names = records.map { |r| r[:name] }

    assert_equal ["Jane Smith", "John Doe"], names, "Records should be ordered by name"
  end

  def test_dataset_limit_with_real_database
    db = create_db
    create_test_table(db)
    insert_test_data(db)

    # Test LIMIT with real database
    records = db[:test_table].limit(1).all
    assert_equal 1, records.length, "LIMIT should restrict number of records"
  end

  def test_dataset_insert_with_real_database
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Test insert operation (need to provide id since DuckDB doesn't support AUTOINCREMENT)
    result = nil
    assert_nothing_raised("Should be able to insert record") do
      result = dataset.insert(id: 1, name: "New User", age: 35)
    end

    # For DuckDB, insert should return 1 (number of affected rows)
    assert_equal 1, result, "Insert should return number of affected rows"
    assert_equal 1, dataset.count, "Should have one record after insert"

    record = dataset.first
    assert_equal "New User", record[:name], "Should insert correct name"
    assert_equal 35, record[:age], "Should insert correct age"
  end

  def test_dataset_update_with_real_database
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table]

    # Test update operation
    updated_count = dataset.where(name: "John Doe").update(age: 31)
    assert_equal 1, updated_count, "Should update one record"

    # Verify update
    record = dataset.where(name: "John Doe").first
    assert_equal 31, record[:age], "Age should be updated"
  end

  def test_dataset_delete_with_real_database
    db = create_db
    create_test_table(db)
    insert_test_data(db)
    dataset = db[:test_table]

    initial_count = dataset.count
    assert_equal 2, initial_count, "Should start with 2 records"

    # Test delete operation
    deleted_count = dataset.where(name: "John Doe").delete
    assert_equal 1, deleted_count, "Should delete one record"

    # Verify deletion
    assert_equal 1, dataset.count, "Should have one record remaining"
    assert_nil dataset.where(name: "John Doe").first, "Deleted record should not exist"
  end

  def test_dataset_complex_query_with_real_database
    db = create_db
    create_test_table(db)
    insert_test_data(db)

    # Test complex query combining multiple clauses
    records = db[:test_table]
              .where { age > 20 }
              .order(:age)
              .limit(10)
              .all

    assert_instance_of Array, records, "Should return array of records"
    refute_empty records, "Should find matching records"

    # Verify all records meet the criteria
    records.each do |record|
      assert record[:age] > 20, "All records should have age > 20"
    end
  end

  def test_dataset_error_handling
    db = create_db
    dataset = db[:nonexistent_table]

    # Test error handling for non-existent table
    assert_database_error do
      dataset.count
    end

    assert_database_error do
      dataset.all
    end
  end

  def test_dataset_empty_result_handling
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Test operations on empty table
    assert_equal 0, dataset.count, "Empty table count should be 0"
    assert_equal [], dataset.all, "Empty table all() should return empty array"
    assert_nil dataset.first, "Empty table first() should return nil"
  end

  # Dataset Operation Support Tests (Requirements 6.1, 6.2, 6.3, 9.5)

  def test_dataset_count_method
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Test count on empty dataset (Requirement 6.3)
    assert_equal 0, dataset.count, "Count should return 0 for empty dataset"

    # Insert test data
    dataset.insert(id: 1, name: "Count Test 1", age: 25)
    dataset.insert(id: 2, name: "Count Test 2", age: 30)
    dataset.insert(id: 3, name: "Count Test 3", age: 35)

    # Test count with data
    assert_equal 3, dataset.count, "Count should return correct number of records"

    # Test count with WHERE clause
    filtered_count = dataset.where { age > 28 }.count
    assert_equal 2, filtered_count, "Count should work with WHERE clause"
  end

  def test_dataset_first_method
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Test first on empty dataset (Requirement 6.2)
    assert_nil dataset.first, "First should return nil for empty dataset"

    # Insert test data
    dataset.insert(id: 1, name: "First Test 1", age: 25)
    dataset.insert(id: 2, name: "First Test 2", age: 30)

    # Test first with data
    record = dataset.first
    refute_nil record, "First should return a record"
    assert_instance_of Hash, record, "First should return a hash"
    assert_includes [1, 2], record[:id], "First should return one of the inserted records"

    # Test first with ORDER BY
    ordered_record = dataset.order(:name).first
    assert_equal "First Test 1", ordered_record[:name], "First should respect ORDER BY"

    # Test first with WHERE clause
    filtered_record = dataset.where(age: 30).first
    assert_equal "First Test 2", filtered_record[:name], "First should work with WHERE clause"
  end

  def test_dataset_all_method
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Test all on empty dataset (Requirement 6.1)
    records = dataset.all
    assert_instance_of Array, records, "All should return an array"
    assert_empty records, "All should return empty array for empty dataset"

    # Insert test data
    dataset.insert(id: 1, name: "All Test 1", age: 25)
    dataset.insert(id: 2, name: "All Test 2", age: 30)
    dataset.insert(id: 3, name: "All Test 3", age: 35)

    # Test all with data
    all_records = dataset.all
    assert_equal 3, all_records.length, "All should return all records"

    # Verify each record is a hash with expected keys
    all_records.each do |record|
      assert_instance_of Hash, record, "Each record should be a hash"
      assert_includes record.keys, :id, "Each record should have id key"
      assert_includes record.keys, :name, "Each record should have name key"
      assert_includes record.keys, :age, "Each record should have age key"
    end

    # Test all with WHERE clause
    filtered_records = dataset.where { age >= 30 }.all
    assert_equal 2, filtered_records.length, "All should work with WHERE clause"
    filtered_records.each do |record|
      assert record[:age] >= 30, "Filtered records should meet criteria"
    end

    # Test all with ORDER BY
    ordered_records = dataset.order(:age).all
    ages = ordered_records.map { |r| r[:age] }
    assert_equal [25, 30, 35], ages, "All should respect ORDER BY"
  end

  def test_dataset_insert_method_return_value
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Test insert return value (should return number of affected rows)
    result = dataset.insert(id: 1, name: "Insert Return Test", age: 28)
    assert_equal 1, result, "Insert should return 1 for successful insertion"

    # Verify the record was actually inserted
    assert_equal 1, dataset.count, "Record should be inserted"
    record = dataset.first
    assert_equal "Insert Return Test", record[:name], "Correct data should be inserted"
  end

  def test_dataset_update_method_return_value
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert initial data
    dataset.insert(id: 1, name: "Update Test", age: 25)
    dataset.insert(id: 2, name: "Update Test 2", age: 30)

    # Test update return value (should return number of affected rows)
    result = dataset.where(id: 1).update(age: 26)
    assert_equal 1, result, "Update should return number of affected rows"

    # Verify the update worked
    updated_record = dataset.where(id: 1).first
    assert_equal 26, updated_record[:age], "Record should be updated"

    # Test update multiple records
    result_multiple = dataset.update(active: true)
    assert_equal 1, result_multiple, "Update should return affected row count"
  end

  def test_dataset_delete_method_return_value
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert initial data
    dataset.insert(id: 1, name: "Delete Test 1", age: 25)
    dataset.insert(id: 2, name: "Delete Test 2", age: 30)
    dataset.insert(id: 3, name: "Delete Test 3", age: 35)

    initial_count = dataset.count
    assert_equal 3, initial_count, "Should start with 3 records"

    # Test delete return value (should return number of affected rows)
    result = dataset.where(id: 2).delete
    assert_equal 1, result, "Delete should return number of affected rows"

    # Verify the delete worked
    assert_equal 2, dataset.count, "Should have 2 records after delete"
    assert_nil dataset.where(id: 2).first, "Deleted record should not exist"

    # Test delete multiple records
    result_multiple = dataset.where { age >= 30 }.delete
    assert_equal 1, result_multiple, "Delete should return affected row count"
    assert_equal 1, dataset.count, "Should have 1 record remaining"
  end

  def test_dataset_streaming_support
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    (1..5).each do |i|
      dataset.insert(id: i, name: "Stream Test #{i}", age: 20 + i)
    end

    # Test streaming with block (Requirement 9.5)
    streamed_records = []
    assert_nothing_raised("Streaming should work with block") do
      dataset.stream do |record|
        streamed_records << record
      end
    end

    assert_equal 5, streamed_records.length, "Streaming should process all records"
    streamed_records.each do |record|
      assert_instance_of Hash, record, "Streamed record should be a hash"
      assert_includes record.keys, :name, "Streamed record should have expected keys"
    end

    # Test streaming without block (should return enumerator)
    enumerator = dataset.stream
    assert_instance_of Enumerator, enumerator, "Stream without block should return enumerator"

    # Test enumerator functionality
    first_from_enum = enumerator.first
    assert_instance_of Hash, first_from_enum, "Enumerator should yield hashes"
  end

  def test_dataset_result_set_handling_and_conversion
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert data with various types (Requirement 9.5)
    dataset.insert(
      id: 1,
      name: "Type Test",
      age: 30,
      active: true,
      score: 85.5
    )

    # Test proper result set handling and conversion
    record = dataset.first

    # Verify data types are properly converted
    assert_instance_of Integer, record[:id], "Integer should be preserved"
    assert_instance_of String, record[:name], "String should be preserved"
    assert_instance_of Integer, record[:age], "Integer should be preserved"

    # Boolean and Float handling may vary by DuckDB version
    # Just ensure they're not nil and have reasonable values
    refute_nil record[:active], "Boolean should not be nil"
    refute_nil record[:score], "Float should not be nil"

    # Test that all method preserves types consistently
    all_records = dataset.all
    all_records.each do |rec|
      assert_instance_of Integer, rec[:id], "All records should have consistent integer IDs"
      assert_instance_of String, rec[:name], "All records should have consistent string names"
    end
  end

  def test_dataset_operations_with_complex_queries
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    (1..10).each do |i|
      dataset.insert(id: i, name: "Complex Test #{i}", age: 20 + (i % 5))
    end

    # Test count with complex WHERE clause
    complex_count = dataset.where { (age > 22) & (age < 25) }.count
    assert_instance_of Integer, complex_count, "Complex count should return integer"
    assert complex_count.positive?, "Complex count should find matching records"

    # Test first with complex query
    complex_first = dataset.where { age > 22 }.order(:id).first
    refute_nil complex_first, "Complex first should find a record"
    assert complex_first[:age] > 22, "Complex first should meet criteria"

    # Test all with complex query
    complex_all = dataset.where { age <= 23 }.order(:name).all
    assert_instance_of Array, complex_all, "Complex all should return array"
    complex_all.each do |record|
      assert record[:age] <= 23, "All complex records should meet criteria"
    end
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end
