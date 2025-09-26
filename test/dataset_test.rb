# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for Sequel::DuckDB::Dataset class
# Tests SQL generation, query building, and dataset operations
class DatasetTest < SequelDuckDBTest::TestCase
  def test_dataset_class_exists
    assert defined?(Sequel::DuckDB::Dataset), "Dataset class should be defined"
    assert_operator Sequel::DuckDB::Dataset, :<, Sequel::Dataset, "Dataset should inherit from Sequel::Dataset"
  end

  def test_dataset_includes_dataset_methods
    assert_includes Sequel::DuckDB::Dataset.included_modules, Sequel::DuckDB::DatasetMethods,
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
    expected_sql = "SELECT * FROM \"users\""

    assert_sql expected_sql, dataset
  end

  def test_select_with_columns_sql_generation
    dataset = mock_dataset(:users).select(:name, :email)

    # Test selecting specific columns
    expected_sql = "SELECT \"name\", \"email\" FROM \"users\""

    assert_sql expected_sql, dataset
  end

  def test_where_clause_sql_generation
    dataset = mock_dataset(:users).where(active: true)

    # Test WHERE clause generation
    expected_sql = "SELECT * FROM \"users\" WHERE (\"active\" IS TRUE)"

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
    expected_sql = "INSERT INTO \"users\" (\"name\", \"email\") VALUES ('John', 'john@example.com')"
    actual_sql = dataset.insert_sql(name: "John", email: "john@example.com")

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_generation
    dataset = mock_dataset(:users)

    # Test UPDATE statement generation
    expected_sql = "UPDATE \"users\" SET \"name\" = 'Jane', \"email\" = 'jane@example.com'"
    actual_sql = dataset.update_sql(name: "Jane", email: "jane@example.com")

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_generation
    dataset = mock_dataset(:users)

    # Test DELETE statement generation
    expected_sql = "DELETE FROM \"users\""
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
      assert_operator record[:age], :>, 20, "All records should have age > 20"
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
    assert_empty dataset.all, "Empty table all() should return empty array"
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
      assert_operator record[:age], :>=, 30, "Filtered records should meet criteria"
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

    assert_equal 2, result_multiple, "Update should return affected row count for all records"
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
    assert_predicate complex_count, :positive?, "Complex count should find matching records"

    # Test first with complex query
    complex_first = dataset.where { age > 22 }.order(:id).first

    refute_nil complex_first, "Complex first should find a record"
    assert_operator complex_first[:age], :>, 22, "Complex first should meet criteria"

    # Test all with complex query
    complex_all = dataset.where { age <= 23 }.order(:name).all

    assert_instance_of Array, complex_all, "Complex all should return array"
    complex_all.each do |record|
      assert_operator record[:age], :<=, 23, "All complex records should meet criteria"
    end
  end

  # Integration tests for LiteralString expressions with real database (Requirements 1.2, 5.1, 6.1)

  def test_literal_string_in_select_clause_with_real_database
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data with created_at timestamps
    dataset.insert(id: 1, name: "Test User 1", age: 25, created_at: Time.new(2023, 5, 15, 10, 30, 0))
    dataset.insert(id: 2, name: "Test User 2", age: 30, created_at: Time.new(2024, 8, 20, 14, 45, 0))

    # Test LiteralString in SELECT clause - extract year from created_at
    result = dataset.select(Sequel.lit("YEAR(created_at) AS year_created")).all

    assert_equal 2, result.length, "Should return all records with year extraction"

    # Verify the literal expression was executed correctly
    years = result.map { |r| r[:year_created] }

    assert_includes years, 2023, "Should extract year 2023 from first record"
    assert_includes years, 2024, "Should extract year 2024 from second record"
  end

  def test_literal_string_in_where_clause_with_real_database
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    dataset.insert(id: 1, name: "Alice", age: 17)
    dataset.insert(id: 2, name: "Bob", age: 25)
    dataset.insert(id: 3, name: "Charlie", age: 30)

    # Test LiteralString in WHERE clause - age comparison
    adults = dataset.where(Sequel.lit("age >= 18")).all

    assert_equal 2, adults.length, "Should find 2 adults (age >= 18)"

    adult_names = adults.map { |r| r[:name] }.sort

    assert_equal %w[Bob Charlie], adult_names, "Should find Bob and Charlie as adults"

    # Test more complex LiteralString expression in WHERE
    young_adults = dataset.where(Sequel.lit("age BETWEEN 18 AND 29")).all

    assert_equal 1, young_adults.length, "Should find 1 young adult (age 18-29)"
    assert_equal "Bob", young_adults.first[:name], "Should find Bob as young adult"
  end

  def test_literal_string_with_string_functions_real_database
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    dataset.insert(id: 1, name: "John Doe", age: 25)
    dataset.insert(id: 2, name: "Jane Smith", age: 30)
    dataset.insert(id: 3, name: "Bob", age: 35)

    # Test LiteralString with LENGTH function in WHERE clause
    long_names = dataset.where(Sequel.lit("LENGTH(name) > 5")).all

    assert_equal 2, long_names.length, "Should find 2 records with names longer than 5 characters"

    long_name_list = long_names.map { |r| r[:name] }.sort

    assert_equal ["Jane Smith", "John Doe"], long_name_list, "Should find John Doe and Jane Smith"

    # Test LiteralString with UPPER function in SELECT
    upper_names = dataset.select(:id, Sequel.lit("UPPER(name) AS upper_name")).order(:id).all

    assert_equal 3, upper_names.length, "Should return all records with uppercase names"
    assert_equal "JOHN DOE", upper_names.first[:upper_name], "Should convert name to uppercase"
    assert_equal "JANE SMITH", upper_names[1][:upper_name], "Should convert name to uppercase"
    assert_equal "BOB", upper_names.last[:upper_name], "Should convert name to uppercase"
  end

  def test_literal_string_in_order_by_with_real_database
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    dataset.insert(id: 1, name: "Charlie", age: 30)
    dataset.insert(id: 2, name: "Alice", age: 25)
    dataset.insert(id: 3, name: "Bob", age: 35)

    # Test LiteralString in ORDER BY clause
    ordered_by_name_length = dataset.order(Sequel.lit("LENGTH(name) DESC")).all

    assert_equal 3, ordered_by_name_length.length, "Should return all records ordered by name length"

    # Verify ordering by name length (descending)
    names = ordered_by_name_length.map { |r| r[:name] }
    expected_order = %w[Charlie Alice Bob] # 7, 5, 3 characters respectively

    assert_equal expected_order, names, "Should be ordered by name length descending"
  end

  def test_literal_string_in_update_with_real_database
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    dataset.insert(id: 1, name: "Test User", age: 25, created_at: Time.now)

    # Test LiteralString in UPDATE SET clause - update created_at to current timestamp
    updated_count = dataset.where(id: 1).update(created_at: Sequel.lit("CURRENT_TIMESTAMP"))

    assert_equal 1, updated_count, "Should update one record"

    # Verify the update worked (created_at should be updated to a recent timestamp)
    updated_record = dataset.where(id: 1).first

    refute_nil updated_record[:created_at], "created_at should not be nil after update"
    assert_instance_of Time, updated_record[:created_at], "created_at should be a Time object"
  end

  def test_literal_string_complex_expressions_real_database
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    dataset.insert(id: 1, name: "John", age: 25)
    dataset.insert(id: 2, name: "Jane", age: 30)

    # Test complex LiteralString expression combining multiple functions
    result = dataset.select(
      :id,
      :name,
      Sequel.lit("name || ' (age: ' || age || ')' AS full_info")
    ).order(:id).all

    assert_equal 2, result.length, "Should return all records with concatenated info"
    assert_equal "John (age: 25)", result.first[:full_info], "Should concatenate name and age correctly"
    assert_equal "Jane (age: 30)", result.last[:full_info], "Should concatenate name and age correctly"
  end

  def test_literal_string_with_aggregates_real_database
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    dataset.insert(id: 1, name: "Group A User 1", age: 25, score: 85.5)
    dataset.insert(id: 2, name: "Group A User 2", age: 30, score: 92.0)
    dataset.insert(id: 3, name: "Group B User 1", age: 35, score: 78.5)

    # Test LiteralString with aggregate functions
    # Group by first word of name and calculate average score
    result = dataset.select(
      Sequel.lit("SUBSTRING(name, 1, 7) AS group_name"),
      Sequel.lit("AVG(score) AS avg_score"),
      Sequel.lit("COUNT(*) AS user_count")
    ).group(Sequel.lit("SUBSTRING(name, 1, 7)")).order(Sequel.lit("group_name")).all

    assert_equal 2, result.length, "Should return 2 groups"

    group_a = result.find { |r| r[:group_name] == "Group A" }
    group_b = result.find { |r| r[:group_name] == "Group B" }

    refute_nil group_a, "Should find Group A"
    refute_nil group_b, "Should find Group B"

    assert_equal 2, group_a[:user_count], "Group A should have 2 users"
    assert_equal 1, group_b[:user_count], "Group B should have 1 user"

    # Check average scores (allowing for floating point precision)
    assert_in_delta 88.75, group_a[:avg_score], 0.01, "Group A average score should be ~88.75"
    assert_in_delta 78.5, group_b[:avg_score], 0.01, "Group B average score should be 78.5"
  end

  def test_literal_string_no_regression_with_regular_strings
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    dataset.insert(id: 1, name: "Test User", age: 25)

    # Test that regular strings are still properly quoted and handled
    # This should work exactly as before (no regression)
    result = dataset.where(name: "Test User").all

    assert_equal 1, result.length, "Should find record with regular string comparison"
    assert_equal "Test User", result.first[:name], "Should match exact string"

    # Test with string containing special characters that need quoting
    dataset.insert(id: 2, name: "User's Name", age: 30)
    result_with_quotes = dataset.where(name: "User's Name").all

    assert_equal 1, result_with_quotes.length, "Should handle strings with quotes correctly"
    assert_equal "User's Name", result_with_quotes.first[:name], "Should match string with apostrophe"

    # Test regular string in INSERT (should be quoted properly)
    dataset.insert(id: 3, name: "Another 'quoted' string", age: 35)
    quoted_result = dataset.where(name: "Another 'quoted' string").first

    refute_nil quoted_result, "Should insert and find string with quotes"
    assert_equal "Another 'quoted' string", quoted_result[:name], "Should preserve quoted string exactly"
  end

  def test_literal_string_vs_sequel_function_comparison
    db = create_db
    create_test_table(db)
    dataset = db[:test_table]

    # Insert test data
    dataset.insert(id: 1, name: "Test", age: 25)
    dataset.insert(id: 2, name: "Another", age: 30)

    # Test that Sequel.function still works (should be unchanged)
    function_result = dataset.select(Sequel.function(:count, :*).as(:total_count)).first

    assert_equal 2, function_result[:total_count], "Sequel.function should still work correctly"

    # Test that LiteralString produces the same result for equivalent expressions
    literal_result = dataset.select(Sequel.lit("COUNT(*) AS total_count")).first

    assert_equal 2, literal_result[:total_count], "Sequel.lit should produce same result as Sequel.function"

    # Test both approaches with LENGTH function
    function_length = dataset.select(:name, Sequel.function(:length, :name).as(:name_length)).order(:id).all
    literal_length = dataset.select(:name, Sequel.lit("LENGTH(name) AS name_length")).order(:id).all

    assert_equal function_length.length, literal_length.length, "Both approaches should return same number of records"
    function_length.each_with_index do |func_record, index|
      lit_record = literal_length[index]

      assert_equal func_record[:name_length], lit_record[:name_length],
                   "Function and literal approaches should produce same length for #{func_record[:name]}"
    end
  end

  # LIKE clause integration tests (Requirements 1.1, 1.2, 1.3, 1.4)
  def test_like_functionality_integration
    db = create_db
    db.create_table(:like_test_users) do
      Integer :id, primary_key: true
      String :name, null: false
      String :email
    end

    # Insert test data
    db[:like_test_users].insert(id: 1, name: "John Doe", email: "john@example.com")
    db[:like_test_users].insert(id: 2, name: "Jane Smith", email: "jane@test.org")
    db[:like_test_users].insert(id: 3, name: "Johnny Walker", email: "johnny@example.com")
    db[:like_test_users].insert(id: 4, name: "Bob Johnson", email: "bob@company.net")

    # Test basic LIKE functionality
    results = db[:like_test_users].where(Sequel.like(:name, "John%")).all

    assert_equal 2, results.length
    names = results.map { |r| r[:name] }.sort

    assert_equal ["John Doe", "Johnny Walker"], names

    # Test LIKE with suffix patterns
    results = db[:like_test_users].where(Sequel.like(:email, "%@example.com")).all

    assert_equal 2, results.length

    # Test NOT LIKE functionality
    results = db[:like_test_users].exclude(Sequel.like(:name, "%John%")).all

    assert_equal 1, results.length
    assert_equal "Jane Smith", results.first[:name]

    db.disconnect
  end

  def test_ilike_functionality_integration
    db = create_db
    db.create_table(:ilike_test_users) do
      Integer :id, primary_key: true
      String :name, null: false
    end

    # Insert test data
    db[:ilike_test_users].insert(id: 1, name: "John Doe")
    db[:ilike_test_users].insert(id: 2, name: "Jane Smith")
    db[:ilike_test_users].insert(id: 3, name: "Johnny Walker")
    db[:ilike_test_users].insert(id: 4, name: "Bob Johnson")

    # Test case-insensitive ILIKE functionality
    results = db[:ilike_test_users].where(Sequel.ilike(:name, "%JOHN%")).all

    assert_equal 3, results.length # John, Johnny, Johnson
    names = results.map { |r| r[:name] }.sort

    assert_equal ["Bob Johnson", "John Doe", "Johnny Walker"], names

    # Test NOT ILIKE functionality
    results = db[:ilike_test_users].exclude(Sequel.ilike(:name, "%JOHN%")).all

    assert_equal 1, results.length
    assert_equal "Jane Smith", results.first[:name]

    db.disconnect
  end

  def test_like_with_wildcards_integration
    db = create_db
    db.create_table(:wildcard_test_users) do
      Integer :id, primary_key: true
      String :name, null: false
    end

    # Insert test data
    db[:wildcard_test_users].insert(id: 1, name: "John")
    db[:wildcard_test_users].insert(id: 2, name: "Joan")
    db[:wildcard_test_users].insert(id: 3, name: "Jane")

    # Test single character wildcard
    results = db[:wildcard_test_users].where(Sequel.like(:name, "Jo_n")).all

    assert_equal 2, results.length # John and Joan
    names = results.map { |r| r[:name] }.sort

    assert_equal %w[Joan John], names

    db.disconnect
  end

  def test_regex_functionality_integration
    db = create_db
    db.create_table(:regex_test_users) do
      Integer :id, primary_key: true
      String :name, null: false
    end

    # Insert test data
    db[:regex_test_users].insert(id: 1, name: "John Doe")
    db[:regex_test_users].insert(id: 2, name: "Jane Smith")
    db[:regex_test_users].insert(id: 3, name: "Johnny Walker")

    # Test regex functionality
    results = db[:regex_test_users].where(name: /^John/).all

    assert_equal 2, results.length # John Doe and Johnny Walker
    names = results.map { |r| r[:name] }.sort

    assert_equal ["John Doe", "Johnny Walker"], names

    db.disconnect
  end

  def test_recursive_cte_integration
    db = create_db

    # Create a test table for hierarchical data
    db.create_table(:categories) do
      Integer :id, primary_key: true
      String :name, null: false
      Integer :parent_id
    end

    # Insert test data
    db[:categories].insert(id: 1, name: "Electronics", parent_id: nil)
    db[:categories].insert(id: 2, name: "Computers", parent_id: 1)
    db[:categories].insert(id: 3, name: "Laptops", parent_id: 2)
    db[:categories].insert(id: 4, name: "Gaming Laptops", parent_id: 3)
    db[:categories].insert(id: 5, name: "Mobile", parent_id: 1)
    db[:categories].insert(id: 6, name: "Smartphones", parent_id: 5)

    # Test recursive CTE with hierarchical data
    base_case = db[:categories].select(:id, :name, :parent_id, Sequel.as(0, :level)).where(parent_id: nil)
    recursive_case = db[:categories].select(
      Sequel.qualify(:c, :id),
      Sequel.qualify(:c, :name),
      Sequel.qualify(:c, :parent_id),
      Sequel.lit("cat_tree.level + 1")
    ).from(Sequel.as(:categories, :c))
                                    .join(:cat_tree, id: :parent_id)

    results = db[:dummy].with_recursive(:cat_tree, base_case, recursive_case)
                        .from(:cat_tree)
                        .order(:level, :id)
                        .all

    # Verify we get all categories in hierarchical order
    assert_equal 6, results.length, "Should return all 6 categories"

    # Electronics should be at level 0
    electronics = results.find { |r| r[:name] == "Electronics" }

    assert_equal 0, electronics[:level], "Electronics should be at level 0"

    # Computers and Mobile should be at level 1
    computers = results.find { |r| r[:name] == "Computers" }
    mobile = results.find { |r| r[:name] == "Mobile" }

    assert_equal 1, computers[:level], "Computers should be at level 1"
    assert_equal 1, mobile[:level], "Mobile should be at level 1"

    # Gaming Laptops should be at level 3 (deepest)
    gaming = results.find { |r| r[:name] == "Gaming Laptops" }

    assert_equal 3, gaming[:level], "Gaming Laptops should be at level 3"

    # Test simple recursive CTE (number sequence)
    base_case = db.select(Sequel.as(1, :n))
    recursive_case = db[:t].select(Sequel.lit("n + 1")).where { n < 5 }

    number_results = db[:dummy].with_recursive(:t, base_case, recursive_case)
                               .from(:t)
                               .all

    # Should get numbers 1 through 5
    assert_equal 5, number_results.length, "Should get 5 numbers"
    assert_equal [1, 2, 3, 4, 5], number_results.map { |r| r[:n] }.sort, "Should get sequence 1-5"

    db.disconnect
  end

  # JOIN USING tests
  def test_supports_join_using
    dataset = mock_dataset(:users)

    assert_predicate dataset, :supports_join_using?, "DuckDB adapter should support JOIN USING"
  end

  def test_join_using_single_column
    dataset = mock_dataset(:users)
    join_clause = Sequel::SQL::JoinUsingClause.new([:user_id], :inner, :profiles)
    dataset = dataset.clone(join: [join_clause])
    expected_sql = "SELECT * FROM \"users\" INNER JOIN \"profiles\" USING (\"user_id\")"

    assert_sql expected_sql, dataset
  end

  def test_join_using_multiple_columns
    dataset = mock_dataset(:users)
    join_clause = Sequel::SQL::JoinUsingClause.new(%i[user_id company_id], :inner, :profiles)
    dataset = dataset.clone(join: [join_clause])
    expected_sql = "SELECT * FROM \"users\" INNER JOIN \"profiles\" USING (\"user_id\", \"company_id\")"

    assert_sql expected_sql, dataset
  end

  def test_join_using_left_join
    dataset = mock_dataset(:users)
    join_clause = Sequel::SQL::JoinUsingClause.new([:user_id], :left, :profiles)
    dataset = dataset.clone(join: [join_clause])
    expected_sql = "SELECT * FROM \"users\" LEFT JOIN \"profiles\" USING (\"user_id\")"

    assert_sql expected_sql, dataset
  end

  # Recursive CTE tests
  def test_with_recursive_method_generates_recursive_keyword
    base_case = mock_db.select(Sequel.as(1, :n))
    recursive_case = mock_db[:t].select(Sequel.lit("n + 1")).where { n < 10 }
    dataset = mock_db[:dummy].with_recursive(:t, base_case, recursive_case).from(:t)

    expected_sql = "WITH RECURSIVE \"t\" AS (SELECT 1 AS \"n\" UNION ALL SELECT n + 1 FROM \"t\" WHERE (\"n\" < 10)) SELECT * FROM \"t\""

    assert_sql expected_sql, dataset
  end

  def test_auto_detection_of_recursive_cte_patterns
    base_case = mock_db.select(Sequel.as(1, :n))
    recursive_case = mock_db[:t].select(Sequel.lit("n + 1")).where { n < 10 }
    combined = base_case.union(recursive_case, all: true)
    dataset = mock_db[:dummy].with(:t, combined).from(:t)

    expected_sql = "WITH \"t\" AS (SELECT * FROM (SELECT 1 AS \"n\" UNION ALL SELECT n + 1 FROM \"t\" WHERE (\"n\" < 10)) AS \"t1\") SELECT * FROM \"t\""

    assert_sql expected_sql, dataset
  end

  # Regex functionality tests
  def test_regex_sql_generation
    dataset = mock_dataset(:users).where(name: /^John/)
    expected_sql = "SELECT * FROM \"users\" WHERE (regexp_matches(\"name\", '^John'))"

    assert_sql expected_sql, dataset
  end

  def test_regex_case_insensitive_pattern
    dataset = mock_dataset(:users).where(name: /john/i)
    expected_sql = "SELECT * FROM \"users\" WHERE (regexp_matches(\"name\", 'john', 'i'))"

    assert_sql expected_sql, dataset
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end
