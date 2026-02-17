# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for core SQL generation methods (TDD - Red phase)
# Tests the four core SQL generation methods: select_sql, insert_sql, update_sql, delete_sql
# These tests are written BEFORE implementation to follow TDD methodology
class CoreSqlGenerationTest < SequelDuckDBTest::TestCase
  # Tests for select_sql method
  def test_select_sql_basic
    dataset = mock_dataset(:users)
    expected_sql = "SELECT * FROM \"users\""

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_columns
    dataset = mock_dataset(:users).select(:name, :age, :email)
    expected_sql = "SELECT \"name\", \"age\", \"email\" FROM \"users\""

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_where_single_condition
    dataset = mock_dataset(:users).where(name: "John")
    expected_sql = "SELECT * FROM \"users\" WHERE (\"name\" = 'John')"

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_where_multiple_conditions
    dataset = mock_dataset(:users).where(name: "John", age: 30)
    expected_sql = "SELECT * FROM \"users\" WHERE ((\"name\" = 'John') AND (\"age\" = 30))"

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_where_or_conditions
    dataset = mock_dataset(:users).where(Sequel.|({ name: "John" }, { name: "Jane" }))
    expected_sql = "SELECT * FROM \"users\" WHERE ((\"name\" = 'John') OR (\"name\" = 'Jane'))"

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_order_by
    dataset = mock_dataset(:users).order(:name)
    expected_sql = "SELECT * FROM \"users\" ORDER BY \"name\""

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_order_by_desc
    dataset = mock_dataset(:users).order(Sequel.desc(:name))
    expected_sql = "SELECT * FROM \"users\" ORDER BY \"name\" DESC"

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_limit
    dataset = mock_dataset(:users).limit(10)
    expected_sql = "SELECT * FROM \"users\" LIMIT 10"

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_limit_and_offset
    dataset = mock_dataset(:users).limit(10, 20)
    expected_sql = "SELECT * FROM \"users\" LIMIT 10 OFFSET 20"

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_group_by
    dataset = mock_dataset(:users).select(:name, Sequel.function(:count, :*)).group(:name)
    expected_sql = "SELECT \"name\", count(*) FROM \"users\" GROUP BY \"name\""

    assert_sql expected_sql, dataset
  end

  def test_select_sql_with_having
    dataset = mock_dataset(:users)
              .select(:name, Sequel.function(:count, :*))
              .group(:name)
              .having { count(:*) > 1 }
    expected_sql = "SELECT \"name\", count(*) FROM \"users\" GROUP BY \"name\" HAVING (count(*) > 1)"

    assert_sql expected_sql, dataset
  end

  def test_select_sql_complex_query
    dataset = mock_dataset(:users)
              .select(:name, :age)
              .where { age > 18 }
              .order(:name)
              .limit(5)
    expected_sql = "SELECT \"name\", \"age\" FROM \"users\" WHERE (\"age\" > 18) ORDER BY \"name\" LIMIT 5"

    assert_sql expected_sql, dataset
  end

  # Tests for insert_sql method
  def test_insert_sql_single_record
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"name\", \"age\") VALUES ('John', 30)"
    actual_sql = dataset.insert_sql(name: "John", age: 30)

    assert_equal expected_sql, actual_sql
  end

  def test_insert_sql_with_string_values
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"name\", \"email\") VALUES ('John Doe', 'john@example.com')"
    actual_sql = dataset.insert_sql(name: "John Doe", email: "john@example.com")

    assert_equal expected_sql, actual_sql
  end

  def test_insert_sql_with_integer_values
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"age\", \"score\") VALUES (25, 100)"
    actual_sql = dataset.insert_sql(age: 25, score: 100)

    assert_equal expected_sql, actual_sql
  end

  def test_insert_sql_with_boolean_values
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"name\", \"active\") VALUES ('John', TRUE)"
    actual_sql = dataset.insert_sql(name: "John", active: true)

    assert_equal expected_sql, actual_sql
  end

  def test_insert_sql_with_null_values
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"name\", \"email\") VALUES ('John', NULL)"
    actual_sql = dataset.insert_sql(name: "John", email: nil)

    assert_equal expected_sql, actual_sql
  end

  def test_insert_sql_with_date_values
    dataset = mock_dataset(:users)
    date = Date.new(2023, 5, 15)
    expected_sql = "INSERT INTO \"users\" (\"name\", \"birth_date\") VALUES ('John', '2023-05-15')"
    actual_sql = dataset.insert_sql(name: "John", birth_date: date)

    assert_equal expected_sql, actual_sql
  end

  def test_insert_sql_with_datetime_values
    dataset = mock_dataset(:users)
    datetime = Time.new(2023, 5, 15, 14, 30, 0)
    expected_sql = "INSERT INTO \"users\" (\"name\", \"created_at\") VALUES ('John', '2023-05-15 14:30:00')"
    actual_sql = dataset.insert_sql(name: "John", created_at: datetime)

    assert_equal expected_sql, actual_sql
  end

  def test_insert_sql_with_multiple_values
    # DuckDB supports multi-row inserts via the multi_insert_sql_strategy
    # The DuckDB adapter sets multi_insert_sql_strategy to :values
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"name\", \"age\") VALUES ('John', 30), ('Jane', 25)"

    actual_sql = dataset.multi_insert_sql(%i[name age], [["John", 30], ["Jane", 25]]).first

    assert_equal expected_sql, actual_sql
  end

  def test_insert_sql_empty_values
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" DEFAULT VALUES"
    actual_sql = dataset.insert_sql({})

    assert_equal expected_sql, actual_sql
  end

  # Tests for update_sql method
  def test_update_sql_basic
    dataset = mock_dataset(:users)
    expected_sql = "UPDATE \"users\" SET \"name\" = 'John', \"age\" = 30"
    actual_sql = dataset.update_sql(name: "John", age: 30)

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_with_where_clause
    dataset = mock_dataset(:users).where(id: 1)
    expected_sql = "UPDATE \"users\" SET \"name\" = 'John' WHERE (\"id\" = 1)"
    actual_sql = dataset.update_sql(name: "John")

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_with_complex_where
    dataset = mock_dataset(:users).where { (age > 18) & (active =~ true) }
    expected_sql = "UPDATE \"users\" SET \"name\" = 'Updated' WHERE ((\"age\" > 18) AND (\"active\" IS TRUE))"
    actual_sql = dataset.update_sql(name: "Updated")

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_with_string_values
    dataset = mock_dataset(:users)
    expected_sql = "UPDATE \"users\" SET \"name\" = 'John Doe', \"email\" = 'john@example.com'"
    actual_sql = dataset.update_sql(name: "John Doe", email: "john@example.com")

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_with_integer_values
    dataset = mock_dataset(:users)
    expected_sql = "UPDATE \"users\" SET \"age\" = 25, \"score\" = 95"
    actual_sql = dataset.update_sql(age: 25, score: 95)

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_with_boolean_values
    dataset = mock_dataset(:users)
    expected_sql = "UPDATE \"users\" SET \"active\" = TRUE, \"verified\" = FALSE"
    actual_sql = dataset.update_sql(active: true, verified: false)

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_with_null_values
    dataset = mock_dataset(:users)
    expected_sql = "UPDATE \"users\" SET \"email\" = NULL, \"phone\" = NULL"
    actual_sql = dataset.update_sql(email: nil, phone: nil)

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_with_date_values
    dataset = mock_dataset(:users)
    date = Date.new(2023, 5, 15)
    expected_sql = "UPDATE \"users\" SET \"birth_date\" = '2023-05-15'"
    actual_sql = dataset.update_sql(birth_date: date)

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_with_datetime_values
    dataset = mock_dataset(:users)
    datetime = Time.new(2023, 5, 15, 14, 30, 0)
    expected_sql = "UPDATE \"users\" SET \"updated_at\" = '2023-05-15 14:30:00'"
    actual_sql = dataset.update_sql(updated_at: datetime)

    assert_equal expected_sql, actual_sql
  end

  def test_update_sql_single_column
    dataset = mock_dataset(:users)
    expected_sql = "UPDATE \"users\" SET \"name\" = 'Updated Name'"
    actual_sql = dataset.update_sql(name: "Updated Name")

    assert_equal expected_sql, actual_sql
  end

  # Tests for delete_sql method
  def test_delete_sql_basic
    dataset = mock_dataset(:users)
    expected_sql = "DELETE FROM \"users\""
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_where_clause
    dataset = mock_dataset(:users).where(id: 1)
    expected_sql = "DELETE FROM \"users\" WHERE (\"id\" = 1)"
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_string_condition
    dataset = mock_dataset(:users).where(name: "John")
    expected_sql = "DELETE FROM \"users\" WHERE (\"name\" = 'John')"
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_multiple_conditions
    dataset = mock_dataset(:users).where(name: "John", age: 30)
    expected_sql = "DELETE FROM \"users\" WHERE ((\"name\" = 'John') AND (\"age\" = 30))"
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_or_conditions
    dataset = mock_dataset(:users).where(Sequel.|({ name: "John" }, { name: "Jane" }))
    expected_sql = "DELETE FROM \"users\" WHERE ((\"name\" = 'John') OR (\"name\" = 'Jane'))"
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_complex_where
    dataset = mock_dataset(:users).where { (age > 65) | (active =~ false) }
    expected_sql = "DELETE FROM \"users\" WHERE ((\"age\" > 65) OR (\"active\" IS FALSE))"
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_null_condition
    dataset = mock_dataset(:users).where(email: nil)
    expected_sql = "DELETE FROM \"users\" WHERE (\"email\" IS NULL)"
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_boolean_condition
    dataset = mock_dataset(:users).where(active: false)
    expected_sql = "DELETE FROM \"users\" WHERE (\"active\" IS FALSE)"
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_range_condition
    dataset = mock_dataset(:users).where(age: 18..65)
    expected_sql = "DELETE FROM \"users\" WHERE ((\"age\" >= 18) AND (\"age\" <= 65))"
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  def test_delete_sql_with_in_condition
    dataset = mock_dataset(:users).where(name: %w[John Jane Bob])
    expected_sql = "DELETE FROM \"users\" WHERE (\"name\" IN ('John', 'Jane', 'Bob'))"
    actual_sql = dataset.delete_sql

    assert_equal expected_sql, actual_sql
  end

  # Tests for SQL escaping and literal handling
  def test_string_literal_escaping
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"name\", \"comment\") VALUES ('John''s Name', 'He said \"Hello\"')"
    actual_sql = dataset.insert_sql(name: "John's Name", comment: 'He said "Hello"')

    assert_equal expected_sql, actual_sql
  end

  def test_identifier_quoting
    dataset = mock_dataset(:users).select(:name, :order, :group)
    dataset = dataset.clone(quote_identifiers: false) # Turn off quoting to test reserved word handling
    # Even though order and group are reserved words, Sequel's behavior is to
    # not quote them if quoting is turned off
    expected_sql = "SELECT name, order, group FROM users"

    assert_sql expected_sql, dataset
  end

  def test_table_name_quoting
    dataset = mock_dataset(:"user-table")
    expected_sql = 'SELECT * FROM "user-table"'

    assert_sql expected_sql, dataset
  end

  # Tests for edge cases and error conditions
  def test_empty_table_name
    # Sequel allows nil as a table name and generates "SELECT * FROM NULL"
    # This is technically valid SQL, though not particularly useful
    dataset = mock_dataset(nil)

    assert_equal "SELECT * FROM NULL", dataset.sql
  end

  def test_sql_injection_prevention
    dataset = mock_dataset(:users)
    malicious_input = "'; DROP TABLE users; --"
    expected_sql = "INSERT INTO \"users\" (\"name\") VALUES ('''; DROP TABLE users; --')"
    actual_sql = dataset.insert_sql(name: malicious_input)

    assert_equal expected_sql, actual_sql
  end

  def test_unicode_string_handling
    dataset = mock_dataset(:users)
    unicode_name = "José María"
    expected_sql = "INSERT INTO \"users\" (\"name\") VALUES ('José María')"
    actual_sql = dataset.insert_sql(name: unicode_name)

    assert_equal expected_sql, actual_sql
  end

  def test_very_long_string_handling
    dataset = mock_dataset(:users)
    long_string = "a" * 1000
    expected_sql = "INSERT INTO \"users\" (\"description\") VALUES ('#{long_string}')"
    actual_sql = dataset.insert_sql(description: long_string)

    assert_equal expected_sql, actual_sql
  end

  def test_special_characters_in_identifiers
    dataset = mock_dataset(:test_table).select(:"column-name", :"column.name", :"column name")
    expected_sql = 'SELECT "column-name", "column.name", "column name" FROM "test_table"'

    assert_sql expected_sql, dataset
  end

  # Tests for different data types
  def test_float_literal_formatting
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"score\", \"rating\") VALUES (85.5, 4.75)"
    actual_sql = dataset.insert_sql(score: 85.5, rating: 4.75)

    assert_equal expected_sql, actual_sql
  end

  def test_large_integer_handling
    dataset = mock_dataset(:users)
    large_int = 9_223_372_036_854_775_807 # Max 64-bit signed integer
    expected_sql = "INSERT INTO \"users\" (\"big_number\") VALUES (9223372036854775807)"
    actual_sql = dataset.insert_sql(big_number: large_int)

    assert_equal expected_sql, actual_sql
  end

  def test_negative_number_handling
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"balance\", \"temperature\") VALUES (-100, -15.5)"
    actual_sql = dataset.insert_sql(balance: -100, temperature: -15.5)

    assert_equal expected_sql, actual_sql
  end

  def test_zero_values
    dataset = mock_dataset(:users)
    expected_sql = "INSERT INTO \"users\" (\"count\", \"score\") VALUES (0, 0.0)"
    actual_sql = dataset.insert_sql(count: 0, score: 0.0)

    assert_equal expected_sql, actual_sql
  end

  # Tests for complex expressions
  def test_select_with_expressions
    dataset = mock_dataset(:users).select(Sequel.lit("name || ' ' || email AS full_info"))
    # Since 'name || ' ' || email' is a literal, 'name' and 'email' and 'full_info' are not quoted
    expected_sql = "SELECT name || ' ' || email AS full_info FROM \"users\""

    assert_sql expected_sql, dataset
  end

  def test_where_with_expressions
    dataset = mock_dataset(:users).where(Sequel.lit("LENGTH(name) > 5"))
    # Since 'LENGTH(name) > 5' is a literal, 'name' is not quoted
    expected_sql = "SELECT * FROM \"users\" WHERE (LENGTH(name) > 5)"

    assert_sql expected_sql, dataset
  end

  def test_update_with_expressions
    dataset = mock_dataset(:users)
    expected_sql = "UPDATE \"users\" SET \"updated_at\" = CURRENT_TIMESTAMP"
    actual_sql = dataset.update_sql(updated_at: Sequel.lit("CURRENT_TIMESTAMP"))

    assert_equal expected_sql, actual_sql
  end
end
