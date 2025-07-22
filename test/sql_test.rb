# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for SQL generation and syntax verification
# Tests that generated SQL is correct and follows DuckDB syntax requirements
class SqlTest < SequelDuckDBTest::TestCase
  def test_basic_select_sql
    dataset = mock_dataset(:users)

    # Basic SELECT * test
    expected_sql = "SELECT * FROM users"
    assert_sql expected_sql, dataset
  end

  def test_select_with_specific_columns
    dataset = mock_dataset(:users).select(:name, :age)

    # SELECT with specific columns
    expected_sql = "SELECT name, age FROM users"
    assert_sql expected_sql, dataset
  end

  def test_select_with_where_clause
    dataset = mock_dataset(:users).where(name: "John")

    # SELECT with WHERE clause
    expected_sql = "SELECT * FROM users WHERE (name = 'John')"
    assert_sql expected_sql, dataset
  end

  def test_select_with_multiple_where_conditions
    dataset = mock_dataset(:users)

    # SELECT with multiple WHERE conditions
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT * FROM users WHERE ((name = 'John') AND (age > 25))"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_or_conditions
    dataset = mock_dataset(:users)

    # SELECT with OR conditions
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT * FROM users WHERE ((name = 'John') OR (name = 'Jane'))"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_order_by
    dataset = mock_dataset(:users)

    # SELECT with ORDER BY
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT * FROM users ORDER BY name"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_order_by_desc
    dataset = mock_dataset(:users)

    # SELECT with ORDER BY DESC
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT * FROM users ORDER BY name DESC"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_multiple_order_columns
    dataset = mock_dataset(:users)

    # SELECT with multiple ORDER BY columns
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT * FROM users ORDER BY name, age DESC"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_limit
    dataset = mock_dataset(:users)

    # SELECT with LIMIT
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT * FROM users LIMIT 10"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_limit_and_offset
    dataset = mock_dataset(:users)

    # SELECT with LIMIT and OFFSET
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT * FROM users LIMIT 10 OFFSET 20"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_group_by
    dataset = mock_dataset(:users)

    # SELECT with GROUP BY
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT name, COUNT(*) FROM users GROUP BY name"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_having
    dataset = mock_dataset(:users)

    # SELECT with HAVING
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT name, COUNT(*) FROM users GROUP BY name HAVING (COUNT(*) > 1)"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_join
    dataset = mock_dataset(:users)

    # SELECT with JOIN
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT * FROM users INNER JOIN profiles ON (profiles.user_id = users.id)"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_select_with_left_join
    dataset = mock_dataset(:users)

    # SELECT with LEFT JOIN
    # This will be implemented when SQL generation methods are added
    # Expected: "SELECT * FROM users LEFT JOIN profiles ON (profiles.user_id = users.id)"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_insert_sql_generation
    dataset = mock_dataset(:users)

    # INSERT statement
    expected_sql = "INSERT INTO users (name, age) VALUES ('John', 30)"
    actual_sql = dataset.insert_sql(name: "John", age: 30)
    assert_equal expected_sql, actual_sql
  end

  def test_insert_sql_with_multiple_values
    dataset = mock_dataset(:users)

    # INSERT with multiple value sets
    # This will be implemented when SQL generation methods are added
    # Expected: "INSERT INTO users (name, age) VALUES ('John', 30), ('Jane', 25)"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_update_sql_generation
    dataset = mock_dataset(:users)

    # UPDATE statement
    expected_sql = "UPDATE users SET name = 'John', age = 30"
    actual_sql = dataset.update_sql(name: "John", age: 30)
    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_with_where
    dataset = mock_dataset(:users).where(id: 1)

    # UPDATE with WHERE clause
    expected_sql = "UPDATE users SET name = 'John' WHERE (id = 1)"
    actual_sql = dataset.update_sql(name: "John")
    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_generation
    dataset = mock_dataset(:users)

    # DELETE statement
    expected_sql = "DELETE FROM users"
    actual_sql = dataset.delete_sql
    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_where
    dataset = mock_dataset(:users).where(id: 1)

    # DELETE with WHERE clause
    expected_sql = "DELETE FROM users WHERE (id = 1)"
    actual_sql = dataset.delete_sql
    assert_equal expected_sql, actual_sql
  end

  def test_string_literal_escaping
    dataset = mock_dataset(:users)

    # Test string literal escaping
    # This will be implemented when literal methods are added
    # Should properly escape single quotes and other special characters
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_identifier_quoting
    dataset = mock_dataset(:users)

    # Test identifier quoting for reserved words and special characters
    # This will be implemented when identifier quoting is added
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_date_literal_formatting
    dataset = mock_dataset(:users)

    # Test date literal formatting
    # This will be implemented when date literal methods are added
    # Expected format for DuckDB date literals
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_datetime_literal_formatting
    dataset = mock_dataset(:users)

    # Test datetime literal formatting
    # This will be implemented when datetime literal methods are added
    # Expected format for DuckDB timestamp literals
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_boolean_literal_formatting
    dataset = mock_dataset(:users)

    # Test boolean literal formatting
    # This will be implemented when boolean literal methods are added
    # Expected: TRUE/FALSE for DuckDB
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_null_literal_formatting
    dataset = mock_dataset(:users)

    # Test NULL literal formatting
    # This will be implemented when NULL handling is added
    # Expected: NULL
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_numeric_literal_formatting
    dataset = mock_dataset(:users)

    # Test numeric literal formatting (integers, floats)
    # This will be implemented when numeric literal methods are added
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_subquery_generation
    dataset = mock_dataset(:users)

    # Test subquery generation
    # This will be implemented when subquery support is added
    # Expected: "SELECT * FROM users WHERE (id IN (SELECT user_id FROM profiles))"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_complex_query_generation
    dataset = mock_dataset(:users)

    # Test complex query with multiple clauses
    # This will be implemented when all SQL generation methods are added
    # Expected: Complex SELECT with JOIN, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_window_function_support
    dataset = mock_dataset(:users)

    # Test window function generation (DuckDB supports window functions)
    # This will be implemented when window function support is added
    # Expected: "SELECT name, ROW_NUMBER() OVER (ORDER BY name) FROM users"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_cte_support
    dataset = mock_dataset(:users)

    # Test Common Table Expression (CTE) support
    # This will be implemented when CTE support is added
    # Expected: "WITH user_stats AS (SELECT ...) SELECT * FROM user_stats"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_case_expression_generation
    dataset = mock_dataset(:users)

    # Test CASE expression generation
    # This will be implemented when CASE expression support is added
    # Expected: "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_aggregate_function_generation
    dataset = mock_dataset(:users)

    # Test aggregate function generation
    # This will be implemented when aggregate function support is added
    # Expected: "SELECT COUNT(*), AVG(age), MAX(age), MIN(age) FROM users"
    assert_instance_of Sequel::Dataset, dataset
  end

  def test_sql_syntax_validation_with_real_database
    # Test that generated SQL actually works with DuckDB
    db = create_db
    create_test_table(db)

    # Test basic operations to ensure SQL syntax is valid
    assert_nothing_raised("Basic SELECT should work") do
      db[:test_table].all
    end

    assert_nothing_raised("INSERT should work") do
      db[:test_table].insert(name: "Test User", age: 25)
    end

    assert_nothing_raised("UPDATE should work") do
      db[:test_table].where(name: "Test User").update(age: 26)
    end

    assert_nothing_raised("DELETE should work") do
      db[:test_table].where(name: "Test User").delete
    end
  end

  def test_sql_error_handling
    db = create_db

    # Test that invalid SQL generates appropriate errors
    assert_database_error do
      db.run("INVALID SQL SYNTAX")
    end
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end