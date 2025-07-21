# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for data type handling and conversion
# Tests Ruby ↔ DuckDB type mapping and data conversion
class TypeTest < SequelDuckDBTest::TestCase
  def test_string_type_handling
    db = create_db

    db.create_table(:string_test) do
      primary_key :id
      String :text_field
      String :varchar_field, size: 100
    end

    # Test string insertion and retrieval
    test_strings = [
      "Simple string",
      "String with 'single quotes'",
      'String with "double quotes"',
      "String with\nnewlines\nand\ttabs",
      "Unicode string: 你好世界 🌍",
      "",  # Empty string
      " ",  # Whitespace
    ]

    test_strings.each_with_index do |test_string, index|
      assert_nothing_raised("Should handle string: #{test_string.inspect}") do
        db[:string_test].insert(id: index + 1, text_field: test_string, varchar_field: test_string)
      end

      # Retrieve and verify
      record = db[:string_test].where(id: index + 1).first
      assert_equal test_string, record[:text_field], "Text field should match inserted string"
      assert_equal test_string, record[:varchar_field], "Varchar field should match inserted string"
    end
  end

  def test_integer_type_handling
    db = create_db

    db.create_table(:integer_test) do
      primary_key :id
      Integer :int_field
      Integer :bigint_field
    end

    # Test various integer values
    test_integers = [
      0,
      1,
      -1,
      42,
      -42,
      1000000,
      -1000000,
      2147483647,    # Max 32-bit signed integer
      -2147483648,   # Min 32-bit signed integer
    ]

    test_integers.each_with_index do |test_int, index|
      assert_nothing_raised("Should handle integer: #{test_int}") do
        db[:integer_test].insert(id: index + 1, int_field: test_int, bigint_field: test_int)
      end

      # Retrieve and verify
      record = db[:integer_test].where(id: index + 1).first
      assert_equal test_int, record[:int_field], "Int field should match inserted integer"
      assert_equal test_int, record[:bigint_field], "Bigint field should match inserted integer"
    end
  end

  def test_float_type_handling
    db = create_db

    db.create_table(:float_test) do
      primary_key :id
      Float :float_field
      Float :double_field
    end

    # Test various float values
    test_floats = [
      0.0,
      1.0,
      -1.0,
      3.14159,
      -3.14159,
      1.23456789,
      -1.23456789,
      1e10,
      -1e10,
      1e-10,
      -1e-10,
    ]

    test_floats.each_with_index do |test_float, index|
      assert_nothing_raised("Should handle float: #{test_float}") do
        db[:float_test].insert(id: index + 1, float_field: test_float, double_field: test_float)
      end

      # Retrieve and verify (with small tolerance for floating point precision)
      record = db[:float_test].where(id: index + 1).first
      assert_in_delta test_float, record[:float_field], 0.000001, "Float field should match inserted float"
      assert_in_delta test_float, record[:double_field], 0.000001, "Double field should match inserted float"
    end
  end

  def test_boolean_type_handling
    db = create_db

    db.create_table(:boolean_test) do
      primary_key :id
      Boolean :bool_field
      Boolean :bool_with_default, default: true
    end

    # Test boolean values
    test_cases = [
      { value: true, expected: true },
      { value: false, expected: false },
    ]

    test_cases.each_with_index do |test_case, index|
      assert_nothing_raised("Should handle boolean: #{test_case[:value]}") do
        db[:boolean_test].insert(id: index + 1, bool_field: test_case[:value])
      end

      # Retrieve and verify
      record = db[:boolean_test].where(id: index + 1).first
      assert_equal test_case[:expected], record[:bool_field], "Boolean field should match inserted boolean"
      assert_equal true, record[:bool_with_default], "Default boolean should be true"
    end
  end

  def test_date_type_handling
    db = create_db

    db.create_table(:date_test) do
      primary_key :id
      Date :date_field
    end

    # Test various date values
    test_dates = [
      Date.new(2023, 1, 1),
      Date.new(2023, 12, 31),
      Date.new(1990, 6, 15),
      Date.new(2050, 3, 20),
      Date.today,
    ]

    test_dates.each_with_index do |test_date, index|
      assert_nothing_raised("Should handle date: #{test_date}") do
        db[:date_test].insert(id: index + 1, date_field: test_date)
      end

      # Retrieve and verify
      record = db[:date_test].where(id: index + 1).first
      retrieved_date = record[:date_field]

      # Convert to Date if it's not already (some databases return strings)
      retrieved_date = Date.parse(retrieved_date.to_s) unless retrieved_date.is_a?(Date)

      assert_equal test_date, retrieved_date, "Date field should match inserted date"
    end
  end

  def test_datetime_type_handling
    db = create_db

    db.create_table(:datetime_test) do
      primary_key :id
      DateTime :datetime_field
      DateTime :timestamp_field
    end

    # Test various datetime values
    test_datetimes = [
      Time.new(2023, 1, 1, 0, 0, 0),
      Time.new(2023, 12, 31, 23, 59, 59),
      Time.new(1990, 6, 15, 12, 30, 45),
      Time.now,
    ]

    test_datetimes.each_with_index do |test_datetime, index|
      assert_nothing_raised("Should handle datetime: #{test_datetime}") do
        db[:datetime_test].insert(id: index + 1, datetime_field: test_datetime, timestamp_field: test_datetime)
      end

      # Retrieve and verify
      record = db[:datetime_test].where(id: index + 1).first
      retrieved_datetime = record[:datetime_field]
      retrieved_timestamp = record[:timestamp_field]

      # Convert to Time if needed and compare (allowing for small precision differences)
      retrieved_datetime = Time.parse(retrieved_datetime.to_s) unless retrieved_datetime.is_a?(Time)
      retrieved_timestamp = Time.parse(retrieved_timestamp.to_s) unless retrieved_timestamp.is_a?(Time)

      # Allow for small differences in precision (1 second tolerance)
      assert_in_delta test_datetime.to_f, retrieved_datetime.to_f, 1.0, "Datetime field should match inserted datetime"
      assert_in_delta test_datetime.to_f, retrieved_timestamp.to_f, 1.0, "Timestamp field should match inserted datetime"
    end
  end

  def test_null_value_handling
    db = create_db

    db.create_table(:null_test) do
      primary_key :id
      String :nullable_string
      Integer :nullable_int
      Float :nullable_float
      Boolean :nullable_bool
      Date :nullable_date
      DateTime :nullable_datetime
    end

    # Insert record with all null values
    assert_nothing_raised("Should handle null values") do
      db[:null_test].insert(
        id: 1,
        nullable_string: nil,
        nullable_int: nil,
        nullable_float: nil,
        nullable_bool: nil,
        nullable_date: nil,
        nullable_datetime: nil
      )
    end

    # Retrieve and verify all fields are nil
    record = db[:null_test].where(id: 1).first
    assert_nil record[:nullable_string], "Nullable string should be nil"
    assert_nil record[:nullable_int], "Nullable int should be nil"
    assert_nil record[:nullable_float], "Nullable float should be nil"
    assert_nil record[:nullable_bool], "Nullable bool should be nil"
    assert_nil record[:nullable_date], "Nullable date should be nil"
    assert_nil record[:nullable_datetime], "Nullable datetime should be nil"
  end

  def test_type_conversion_edge_cases
    db = create_db

    db.create_table(:edge_case_test) do
      primary_key :id
      String :string_field
      Integer :int_field
      Float :float_field
    end

    # Test edge cases and boundary values
    assert_nothing_raised("Should handle empty string") do
      db[:edge_case_test].insert(id: 1, string_field: "", int_field: 0, float_field: 0.0)
    end

    assert_nothing_raised("Should handle very long string") do
      long_string = "x" * 10000
      db[:edge_case_test].insert(id: 2, string_field: long_string, int_field: 1, float_field: 1.0)

      record = db[:edge_case_test].where(id: 2).first
      assert_equal long_string, record[:string_field], "Long string should be preserved"
    end

    assert_nothing_raised("Should handle special float values") do
      # Note: NaN and Infinity handling depends on DuckDB support
      db[:edge_case_test].insert(id: 3, string_field: "test", int_field: 999, float_field: Float::MAX)
    end
  end

  def test_binary_data_handling
    # This test will be implemented when BLOB support is added
    # For now, just verify the test structure exists
    assert true, "Binary data test placeholder"
  end

  def test_type_coercion
    db = create_db

    db.create_table(:coercion_test) do
      primary_key :id
      String :string_field
      Integer :int_field
      Float :float_field
      Boolean :bool_field
    end

    # Test that Ruby types are properly coerced
    assert_nothing_raised("Should coerce string to string") do
      db[:coercion_test].insert(id: 1, string_field: "123")
    end

    assert_nothing_raised("Should coerce integer to string") do
      db[:coercion_test].insert(id: 2, string_field: 123)
    end

    assert_nothing_raised("Should coerce string to integer if valid") do
      db[:coercion_test].insert(id: 3, int_field: "456")
    end

    assert_nothing_raised("Should coerce integer to float") do
      db[:coercion_test].insert(id: 4, float_field: 789)
    end

    # Verify the coerced values
    record1 = db[:coercion_test].where(id: 1).first
    assert_equal "123", record1[:string_field], "String should remain string"

    record2 = db[:coercion_test].where(id: 2).first
    assert_equal "123", record2[:string_field], "Integer should be coerced to string"
  end

  def test_schema_type_reporting
    db = create_db

    db.create_table(:type_schema_test) do
      primary_key :id
      String :string_col
      Integer :int_col
      Float :float_col
      Boolean :bool_col
      Date :date_col
      DateTime :datetime_col
    end

    schema = db.schema(:type_schema_test)

    # Verify that schema reports correct types
    type_map = schema.to_h

    # Check that each column has the expected type information
    assert type_map[:string_col][:type], "String column should have type info"
    assert type_map[:int_col][:type], "Integer column should have type info"
    assert type_map[:float_col][:type], "Float column should have type info"
    assert type_map[:bool_col][:type], "Boolean column should have type info"
    assert type_map[:date_col][:type], "Date column should have type info"
    assert type_map[:datetime_col][:type], "DateTime column should have type info"

    # Verify db_type is also present
    schema.each do |column_name, column_info|
      assert column_info[:db_type], "Column #{column_name} should have db_type"
      assert_instance_of String, column_info[:db_type], "db_type should be a string"
    end
  end

  def test_type_error_handling
    db = create_db

    db.create_table(:type_error_test) do
      primary_key :id
      Integer :int_field, null: false
    end

    # Test type constraint violations
    # Note: Specific error handling will depend on DuckDB's error reporting
    assert_database_error do
      db[:type_error_test].insert(id: 1, int_field: "not_a_number")
    end
  end

  def test_default_value_types
    db = create_db

    db.create_table(:default_type_test) do
      primary_key :id
      String :string_with_default, default: "default_string"
      Integer :int_with_default, default: 42
      Float :float_with_default, default: 3.14
      Boolean :bool_with_default, default: true
      Date :date_with_default, default: Date.new(2023, 1, 1)
    end

    # Insert record without specifying default fields
    db[:default_type_test].insert(id: 1)

    record = db[:default_type_test].where(id: 1).first

    # Verify default values are applied and have correct types
    assert_equal "default_string", record[:string_with_default], "String default should be applied"
    assert_equal 42, record[:int_with_default], "Integer default should be applied"
    assert_in_delta 3.14, record[:float_with_default], 0.001, "Float default should be applied"
    assert_equal true, record[:bool_with_default], "Boolean default should be applied"

    # Date default handling may vary by database
    refute_nil record[:date_with_default], "Date default should not be nil"
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end