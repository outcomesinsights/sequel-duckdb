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

    # Test insert operation
    assert_nothing_raised("Should be able to insert record") do
      dataset.insert(name: "New User", age: 35)
    end

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

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end