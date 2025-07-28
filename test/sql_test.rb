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
    dataset = mock_dataset(:users).where(name: "John").where { age > 25 }

    # SELECT with multiple WHERE conditions
    expected_sql = "SELECT * FROM users WHERE ((name = 'John') AND (age > 25))"
    assert_sql expected_sql, dataset
  end

  def test_select_with_or_conditions
    dataset = mock_dataset(:users).where(Sequel.|({ name: "John" }, { name: "Jane" }))

    # SELECT with OR conditions
    expected_sql = "SELECT * FROM users WHERE ((name = 'John') OR (name = 'Jane'))"
    assert_sql expected_sql, dataset
  end

  def test_select_with_order_by
    dataset = mock_dataset(:users).order(:name)

    # SELECT with ORDER BY
    expected_sql = "SELECT * FROM users ORDER BY name"
    assert_sql expected_sql, dataset
  end

  def test_select_with_order_by_desc
    dataset = mock_dataset(:users).order(Sequel.desc(:name))

    # SELECT with ORDER BY DESC
    expected_sql = "SELECT * FROM users ORDER BY name DESC"
    assert_sql expected_sql, dataset
  end

  def test_select_with_multiple_order_columns
    dataset = mock_dataset(:users).order(:name, Sequel.desc(:age))

    # SELECT with multiple ORDER BY columns
    expected_sql = "SELECT * FROM users ORDER BY name, age DESC"
    assert_sql expected_sql, dataset
  end

  def test_select_with_limit
    dataset = mock_dataset(:users).limit(10)

    # SELECT with LIMIT
    expected_sql = "SELECT * FROM users LIMIT 10"
    assert_sql expected_sql, dataset
  end

  def test_select_with_limit_and_offset
    dataset = mock_dataset(:users).limit(10, 20)

    # SELECT with LIMIT and OFFSET
    expected_sql = "SELECT * FROM users LIMIT 10 OFFSET 20"
    assert_sql expected_sql, dataset
  end

  def test_select_with_group_by
    dataset = mock_dataset(:users).select(:name, Sequel.function(:count, :*)).group(:name)

    # SELECT with GROUP BY
    expected_sql = "SELECT name, count(*) FROM users GROUP BY name"
    assert_sql expected_sql, dataset
  end

  def test_select_with_having
    dataset = mock_dataset(:users).select(:name, Sequel.function(:count, :*)).group(:name).having { count(:*) > 1 }

    # SELECT with HAVING
    expected_sql = "SELECT name, count(*) FROM users GROUP BY name HAVING (count(*) > 1)"
    assert_sql expected_sql, dataset
  end

  def test_select_with_join
    dataset = mock_dataset(:users).join(:profiles, user_id: :id)

    # SELECT with JOIN
    expected_sql = "SELECT * FROM users INNER JOIN profiles ON (profiles.user_id = users.id)"
    assert_sql expected_sql, dataset
  end

  def test_select_with_left_join
    dataset = mock_dataset(:users).left_join(:profiles, user_id: :id)

    # SELECT with LEFT JOIN
    expected_sql = "SELECT * FROM users LEFT JOIN profiles ON (profiles.user_id = users.id)"
    assert_sql expected_sql, dataset
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

    # INSERT with multiple value sets - test the SQL generation for multi_insert
    columns = %i[name age]
    values = [["John", 30], ["Jane", 25]]

    # For multi_insert, we test that it generates proper INSERT statements
    sql_statements = dataset.multi_insert_sql(columns, values)

    # Verify that we get SQL statements for inserting the data
    assert_kind_of Array, sql_statements
    assert sql_statements.length > 0, "Should generate at least one SQL statement"

    # Each statement should be an INSERT statement
    sql_statements.each do |sql|
      assert_match(/^INSERT INTO users/, sql, "Each statement should be an INSERT")
    end
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
    dataset = mock_dataset(:users).where(name: "John's Name")

    # Test string literal escaping - should properly escape single quotes
    expected_sql = "SELECT * FROM users WHERE (name = 'John''s Name')"
    assert_sql expected_sql, dataset
  end

  def test_identifier_quoting
    dataset = mock_dataset(:users).select(:name, :order)

    # Test identifier quoting for reserved words and special characters
    # DuckDB should quote reserved words like 'order'
    expected_sql = "SELECT name, \"order\" FROM users"
    assert_sql expected_sql, dataset
  end

  def test_date_literal_formatting
    dataset = mock_dataset(:users).where(birth_date: Date.new(2023, 5, 15))

    # Test date literal formatting - DuckDB uses string format for dates
    expected_sql = "SELECT * FROM users WHERE (birth_date = '2023-05-15')"
    assert_sql expected_sql, dataset
  end

  def test_datetime_literal_formatting
    datetime = Time.new(2023, 5, 15, 14, 30, 0)
    dataset = mock_dataset(:users).where(created_at: datetime)

    # Test datetime literal formatting - DuckDB uses string format for timestamps
    expected_sql = "SELECT * FROM users WHERE (created_at = '2023-05-15 14:30:00')"
    assert_sql expected_sql, dataset
  end

  def test_boolean_literal_formatting
    dataset = mock_dataset(:users).where(active: true)

    # Test boolean literal formatting - DuckDB uses IS TRUE for boolean comparisons
    expected_sql = "SELECT * FROM users WHERE (active IS TRUE)"
    assert_sql expected_sql, dataset
  end

  def test_null_literal_formatting
    dataset = mock_dataset(:users).where(deleted_at: nil)

    # Test NULL literal formatting
    expected_sql = "SELECT * FROM users WHERE (deleted_at IS NULL)"
    assert_sql expected_sql, dataset
  end

  def test_numeric_literal_formatting
    dataset = mock_dataset(:users).where(age: 25, score: 85.5)

    # Test numeric literal formatting (integers, floats)
    expected_sql = "SELECT * FROM users WHERE ((age = 25) AND (score = 85.5))"
    assert_sql expected_sql, dataset
  end

  def test_subquery_generation
    subquery = mock_dataset(:profiles).select(:user_id)
    dataset = mock_dataset(:users).where(id: subquery)

    # Test subquery generation
    expected_sql = "SELECT * FROM users WHERE (id IN (SELECT user_id FROM profiles))"
    assert_sql expected_sql, dataset
  end

  def test_complex_query_generation
    dataset = mock_dataset(:users)
              .select(:name, Sequel.function(:count, :*).as(:order_count))
              .join(:orders, user_id: :id)
              .where { created_at > Date.today - 30 }
              .group(:name)
              .having { count(:*) > 5 }
              .order(Sequel.desc(:order_count))
              .limit(10)

    # Test complex query with multiple clauses
    expected_sql = "SELECT name, count(*) AS order_count FROM users INNER JOIN orders ON (orders.user_id = users.id) WHERE (created_at > '#{Date.today - 30}') GROUP BY name HAVING (count(*) > 5) ORDER BY order_count DESC LIMIT 10"
    assert_sql expected_sql, dataset
  end

  def test_window_function_support
    dataset = mock_dataset(:users).select(:name, Sequel.function(:row_number).over(order: :name))

    # Test window function generation (DuckDB supports window functions)
    expected_sql = "SELECT name, row_number() OVER (ORDER BY name) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_cte_support
    cte = mock_dataset(:users).select(:id, :name).where { age > 18 }
    dataset = mock_dataset.with(:adults, cte).from(:adults)

    # Test Common Table Expression (CTE) support
    expected_sql = "WITH adults AS (SELECT id, name FROM users WHERE (age > 18)) SELECT * FROM adults"
    assert_sql expected_sql, dataset
  end

  def test_case_expression_generation
    case_expr = Sequel.case({ { age: (18..65) } => "adult" }, "other")
    dataset = mock_dataset(:users).select(:name, case_expr.as(:category))

    # Test CASE expression generation - DuckDB wraps CASE in parentheses
    expected_sql = "SELECT name, (CASE WHEN ((age >= 18) AND (age <= 65)) THEN 'adult' ELSE 'other' END) AS category FROM users"
    assert_sql expected_sql, dataset
  end

  def test_aggregate_function_generation
    dataset = mock_dataset(:users).select(
      Sequel.function(:count, :*).as(:total_count),
      Sequel.function(:avg, :age).as(:avg_age),
      Sequel.function(:max, :age).as(:max_age),
      Sequel.function(:min, :age).as(:min_age)
    )

    # Test aggregate function generation
    expected_sql = "SELECT count(*) AS total_count, avg(age) AS avg_age, max(age) AS max_age, min(age) AS min_age FROM users"
    assert_sql expected_sql, dataset
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
      db[:test_table].insert(id: 1, name: "Test User", age: 25)
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

  # Tests for LiteralString handling (Requirements 1.1, 1.2, 4.1, 6.1)
  def test_literal_string_in_select_clause
    dataset = mock_dataset(:test).select(Sequel.lit("YEAR(created_at)"))

    # LiteralString should not be quoted in SELECT clause
    expected_sql = "SELECT YEAR(created_at) FROM test"
    assert_sql expected_sql, dataset
  end

  def test_literal_string_in_where_clause
    dataset = mock_dataset(:test).where(Sequel.lit("age > 18"))

    # LiteralString should not be quoted in WHERE clause
    expected_sql = "SELECT * FROM test WHERE (age > 18)"
    assert_sql expected_sql, dataset
  end

  def test_literal_string_with_function_call
    dataset = mock_dataset(:users).select(Sequel.lit("LENGTH(name)"))

    # Function calls in LiteralString should not be quoted
    expected_sql = "SELECT LENGTH(name) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_literal_string_with_complex_expression
    dataset = mock_dataset(:users).select(Sequel.lit("name || ' ' || email AS full_info"))

    # Complex expressions in LiteralString should not be quoted
    expected_sql = "SELECT name || ' ' || email AS full_info FROM users"
    assert_sql expected_sql, dataset
  end

  def test_literal_string_in_update_clause
    dataset = mock_dataset(:users).where(id: 1)

    # LiteralString in UPDATE SET clause should not be quoted
    expected_sql = "UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE (id = 1)"
    actual_sql = dataset.update_sql(updated_at: Sequel.lit("CURRENT_TIMESTAMP"))
    assert_equal expected_sql, actual_sql
  end

  def test_literal_string_in_order_by_clause
    dataset = mock_dataset(:users).order(Sequel.lit("LENGTH(name) DESC"))

    # LiteralString in ORDER BY should not be quoted
    expected_sql = "SELECT * FROM users ORDER BY LENGTH(name) DESC"
    assert_sql expected_sql, dataset
  end

  def test_literal_string_in_group_by_clause
    dataset = mock_dataset(:users).group(Sequel.lit("YEAR(created_at)"))

    # LiteralString in GROUP BY should not be quoted
    expected_sql = "SELECT * FROM users GROUP BY YEAR(created_at)"
    assert_sql expected_sql, dataset
  end

  def test_literal_string_in_having_clause
    dataset = mock_dataset(:users).group(:name).having(Sequel.lit("COUNT(*) > 1"))

    # LiteralString in HAVING should not be quoted
    expected_sql = "SELECT * FROM users GROUP BY name HAVING (COUNT(*) > 1)"
    assert_sql expected_sql, dataset
  end

  # Regression tests to ensure regular strings are still quoted properly (Requirement 6.1)
  def test_regular_string_still_quoted_in_where
    dataset = mock_dataset(:users).where(name: "John's Name")

    # Regular strings should still be properly quoted and escaped
    expected_sql = "SELECT * FROM users WHERE (name = 'John''s Name')"
    assert_sql expected_sql, dataset
  end

  def test_regular_string_still_quoted_in_select
    dataset = mock_dataset(:users).select(Sequel.as("John's Name", :display_name))

    # Regular strings should still be properly quoted and escaped
    expected_sql = "SELECT 'John''s Name' AS display_name FROM users"
    assert_sql expected_sql, dataset
  end

  def test_regular_string_still_quoted_in_insert
    dataset = mock_dataset(:users)

    # Regular strings in INSERT should still be quoted
    expected_sql = "INSERT INTO users (name, description) VALUES ('John', 'A user''s profile')"
    actual_sql = dataset.insert_sql(name: "John", description: "A user's profile")
    assert_equal expected_sql, actual_sql
  end

  def test_regular_string_still_quoted_in_update
    dataset = mock_dataset(:users).where(id: 1)

    # Regular strings in UPDATE should still be quoted
    expected_sql = "UPDATE users SET name = 'John''s Name' WHERE (id = 1)"
    actual_sql = dataset.update_sql(name: "John's Name")
    assert_equal expected_sql, actual_sql
  end

  # Test that SQL::Function continues working (should be unchanged)
  def test_sql_function_still_works
    dataset = mock_dataset(:users).select(Sequel.function(:count, :*))

    # SQL::Function should continue to work as before
    expected_sql = "SELECT count(*) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_sql_function_with_arguments
    dataset = mock_dataset(:users).select(Sequel.function(:sum, :amount))

    # SQL::Function with arguments should work
    expected_sql = "SELECT sum(amount) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_sql_function_nested
    dataset = mock_dataset(:users).select(Sequel.function(:count, Sequel.function(:distinct, :name)))

    # Nested SQL::Function calls should work
    expected_sql = "SELECT count(distinct(name)) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_mixed_literal_string_and_function
    dataset = mock_dataset(:users).select(
      Sequel.function(:count, :*).as(:total_count),
      Sequel.lit("YEAR(created_at)").as(:year)
    )

    # Mix of SQL::Function and LiteralString should work
    expected_sql = "SELECT count(*) AS total_count, YEAR(created_at) AS year FROM users"
    assert_sql expected_sql, dataset
  end

  def test_literal_string_with_parameters_not_supported
    # NOTE: LiteralString doesn't support parameterized queries by design
    # This test documents the expected behavior
    dataset = mock_dataset(:users).where(Sequel.lit("age > 18"))

    # Parameters in LiteralString are not processed - they're literal
    expected_sql = "SELECT * FROM users WHERE (age > 18)"
    assert_sql expected_sql, dataset
  end

  # LIKE clause SQL generation tests (Requirements 1.1, 1.2, 1.3, 1.4)
  def test_like_clause_sql_generation
    dataset = mock_dataset(:users).where(Sequel.like(:name, "%John%"))
    expected_sql = "SELECT * FROM users WHERE (name LIKE '%John%')"
    assert_sql expected_sql, dataset
  end

  def test_not_like_clause_sql_generation
    dataset = mock_dataset(:users).exclude(Sequel.like(:name, "%John%"))
    expected_sql = "SELECT * FROM users WHERE (name NOT LIKE '%John%')"
    assert_sql expected_sql, dataset
  end

  def test_ilike_clause_sql_generation
    dataset = mock_dataset(:users).where(Sequel.ilike(:name, "%john%"))
    expected_sql = "SELECT * FROM users WHERE (UPPER(name) LIKE UPPER('%john%'))"
    assert_sql expected_sql, dataset
  end

  def test_not_ilike_clause_sql_generation
    dataset = mock_dataset(:users).exclude(Sequel.ilike(:name, "%john%"))
    expected_sql = "SELECT * FROM users WHERE (UPPER(name) NOT LIKE UPPER('%john%'))"
    assert_sql expected_sql, dataset
  end

  def test_like_with_various_patterns
    # Test with prefix pattern
    dataset = mock_dataset(:users).where(Sequel.like(:name, "John%"))
    expected_sql = "SELECT * FROM users WHERE (name LIKE 'John%')"
    assert_sql expected_sql, dataset

    # Test with suffix pattern
    dataset = mock_dataset(:users).where(Sequel.like(:name, "%Doe"))
    expected_sql = "SELECT * FROM users WHERE (name LIKE '%Doe')"
    assert_sql expected_sql, dataset

    # Test with exact match
    dataset = mock_dataset(:users).where(Sequel.like(:name, "John Doe"))
    expected_sql = "SELECT * FROM users WHERE (name LIKE 'John Doe')"
    assert_sql expected_sql, dataset
  end

  def test_like_with_special_characters
    # Test with underscore (single character wildcard)
    dataset = mock_dataset(:users).where(Sequel.like(:name, "J_hn"))
    expected_sql = "SELECT * FROM users WHERE (name LIKE 'J_hn')"
    assert_sql expected_sql, dataset

    # Test with mixed wildcards
    dataset = mock_dataset(:users).where(Sequel.like(:name, "%J_hn%"))
    expected_sql = "SELECT * FROM users WHERE (name LIKE '%J_hn%')"
    assert_sql expected_sql, dataset
  end

  def test_like_in_complex_conditions
    dataset = mock_dataset(:users).where(
      Sequel.like(:name, "%John%") & (Sequel[:age] > 25)
    )
    expected_sql = "SELECT * FROM users WHERE ((name LIKE '%John%') AND (age > 25))"
    assert_sql expected_sql, dataset
  end

  def test_like_with_or_conditions
    dataset = mock_dataset(:users).where(
      Sequel.like(:name, "%John%") | Sequel.like(:email, "%@example.com")
    )
    expected_sql = "SELECT * FROM users WHERE ((name LIKE '%John%') OR (email LIKE '%@example.com'))"
    assert_sql expected_sql, dataset
  end

  def test_regex_expression_sql_generation
    dataset = mock_dataset(:users).where(name: /^John/)
    expected_sql = "SELECT * FROM users WHERE (regexp_matches(name, '^John'))"
    assert_sql expected_sql, dataset
  end

  def test_regex_with_complex_pattern
    dataset = mock_dataset(:users).where(name: /^John.*Doe$/)
    expected_sql = "SELECT * FROM users WHERE (regexp_matches(name, '^John.*Doe$'))"
    assert_sql expected_sql, dataset
  end

  # DuckDB Array Syntax Tests (Requirements 2.1)
  def test_duckdb_array_literal_syntax
    # Test that DuckDB array literals use [1, 2, 3] syntax
    dataset = mock_dataset(:users).select(Sequel.lit("[1, 2, 3]").as(:numbers))
    expected_sql = "SELECT [1, 2, 3] AS numbers FROM users"
    assert_sql expected_sql, dataset
  end

  def test_duckdb_array_literal_with_strings
    # Test DuckDB array with string literals
    dataset = mock_dataset(:users).select(Sequel.lit("['apple', 'banana', 'cherry']").as(:fruits))
    expected_sql = "SELECT ['apple', 'banana', 'cherry'] AS fruits FROM users"
    assert_sql expected_sql, dataset
  end

  def test_duckdb_array_literal_in_where_clause
    # Test DuckDB array literal in WHERE clause
    dataset = mock_dataset(:users).where(Sequel.lit("id = ANY([1, 2, 3])"))
    expected_sql = "SELECT * FROM users WHERE (id = ANY([1, 2, 3]))"
    assert_sql expected_sql, dataset
  end

  def test_duckdb_array_literal_mixed_types
    # Test DuckDB array with mixed types (numbers and strings)
    dataset = mock_dataset(:users).select(Sequel.lit("[1, 'two', 3.0]").as(:mixed_array))
    expected_sql = "SELECT [1, 'two', 3.0] AS mixed_array FROM users"
    assert_sql expected_sql, dataset
  end

  def test_duckdb_nested_array_literals
    # Test nested DuckDB arrays
    dataset = mock_dataset(:users).select(Sequel.lit("[[1, 2], [3, 4]]").as(:nested_array))
    expected_sql = "SELECT [[1, 2], [3, 4]] AS nested_array FROM users"
    assert_sql expected_sql, dataset
  end

  def test_duckdb_array_functions
    # Test DuckDB array functions with array literals
    dataset = mock_dataset(:users).select(Sequel.lit("array_length([1, 2, 3])").as(:array_len))
    expected_sql = "SELECT array_length([1, 2, 3]) AS array_len FROM users"
    assert_sql expected_sql, dataset
  end

  def test_duckdb_array_element_access
    # Test DuckDB array element access syntax
    dataset = mock_dataset(:users).select(Sequel.lit("[1, 2, 3][1]").as(:first_element))
    expected_sql = "SELECT [1, 2, 3][1] AS first_element FROM users"
    assert_sql expected_sql, dataset
  end

  # JSON Functions Tests (Requirements 2.2)
  def test_json_extract_function
    # Test json_extract function for extracting values from JSON
    dataset = mock_dataset(:users).select(Sequel.lit("json_extract(data, '$.name')").as(:extracted_name))
    expected_sql = "SELECT json_extract(data, '$.name') AS extracted_name FROM users"
    assert_sql expected_sql, dataset
  end

  def test_json_extract_with_path
    # Test json_extract with complex JSON path
    dataset = mock_dataset(:users).select(Sequel.lit("json_extract(profile, '$.address.city')").as(:city))
    expected_sql = "SELECT json_extract(profile, '$.address.city') AS city FROM users"
    assert_sql expected_sql, dataset
  end

  def test_json_extract_array_element
    # Test json_extract with array element access
    dataset = mock_dataset(:users).select(Sequel.lit("json_extract(tags, '$[0]')").as(:first_tag))
    expected_sql = "SELECT json_extract(tags, '$[0]') AS first_tag FROM users"
    assert_sql expected_sql, dataset
  end

  def test_json_extract_in_where_clause
    # Test json_extract in WHERE clause
    dataset = mock_dataset(:users).where(Sequel.lit("json_extract(data, '$.active') = true"))
    expected_sql = "SELECT * FROM users WHERE (json_extract(data, '$.active') = true)"
    assert_sql expected_sql, dataset
  end

  def test_json_extract_with_cast
    # Test json_extract with type casting
    dataset = mock_dataset(:users).select(Sequel.lit("CAST(json_extract(data, '$.age') AS INTEGER)").as(:age))
    expected_sql = "SELECT CAST(json_extract(data, '$.age') AS INTEGER) AS age FROM users"
    assert_sql expected_sql, dataset
  end

  def test_json_extract_multiple_paths
    # Test multiple json_extract calls in same query
    dataset = mock_dataset(:users).select(
      Sequel.lit("json_extract(data, '$.name')").as(:name),
      Sequel.lit("json_extract(data, '$.email')").as(:email)
    )
    expected_sql = "SELECT json_extract(data, '$.name') AS name, json_extract(data, '$.email') AS email FROM users"
    assert_sql expected_sql, dataset
  end

  def test_json_extract_nested_objects
    # Test json_extract with deeply nested JSON objects
    dataset = mock_dataset(:users).select(Sequel.lit("json_extract(data, '$.profile.settings.theme')").as(:theme))
    expected_sql = "SELECT json_extract(data, '$.profile.settings.theme') AS theme FROM users"
    assert_sql expected_sql, dataset
  end

  def test_json_functions_with_arrays
    # Test JSON functions working with DuckDB arrays
    dataset = mock_dataset(:users).select(Sequel.lit("json_extract('[1, 2, 3]', '$[1]')").as(:second_element))
    expected_sql = "SELECT json_extract('[1, 2, 3]', '$[1]') AS second_element FROM users"
    assert_sql expected_sql, dataset
  end

  def test_json_extract_with_null_handling
    # Test json_extract with NULL handling
    dataset = mock_dataset(:users).where(Sequel.lit("json_extract(data, '$.deleted_at') IS NULL"))
    expected_sql = "SELECT * FROM users WHERE (json_extract(data, '$.deleted_at') IS NULL)"
    assert_sql expected_sql, dataset
  end

  def test_json_extract_with_complex_conditions
    # Test json_extract in complex WHERE conditions
    dataset = mock_dataset(:users).where(
      Sequel.lit("json_extract(data, '$.active') = true") &
      Sequel.lit("json_extract(data, '$.age') > 18")
    )
    expected_sql = "SELECT * FROM users WHERE (json_extract(data, '$.active') = true AND json_extract(data, '$.age') > 18)"
    assert_sql expected_sql, dataset
  end

  # Integration tests with actual DuckDB database
  def test_duckdb_array_syntax_with_real_database
    # Test that DuckDB array syntax actually works with real database
    db = create_db

    assert_nothing_raised("DuckDB array literal should work") do
      result = db.fetch("SELECT [1, 2, 3] AS numbers").first
      assert_equal [1, 2, 3], result[:numbers]
    end
  end

  def test_json_extract_with_real_database
    # Test that json_extract actually works with real database
    db = create_db

    assert_nothing_raised("json_extract should work") do
      result = db.fetch("SELECT json_extract('{\"name\": \"John\", \"age\": 30}', '$.name') AS name").first
      # DuckDB json_extract returns quoted strings, so we expect "John" not John
      assert_equal "\"John\"", result[:name]
    end
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end
