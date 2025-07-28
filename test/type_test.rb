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
      "", # Empty string
      " " # Whitespace
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
      1_000_000,
      -1_000_000,
      2_147_483_647, # Max 32-bit signed integer
      -2_147_483_648 # Min 32-bit signed integer
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
      -1e-10
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
      { value: false, expected: false }
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
      Date.today
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
      Time.now
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
      assert_in_delta test_datetime.to_f, retrieved_timestamp.to_f, 1.0,
                      "Timestamp field should match inserted datetime"
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
      long_string = "x" * 10_000
      db[:edge_case_test].insert(id: 2, string_field: long_string, int_field: 1, float_field: 1.0)

      record = db[:edge_case_test].where(id: 2).first
      assert_equal long_string, record[:string_field], "Long string should be preserved"
    end

    assert_nothing_raised("Should handle special float values") do
      # NOTE: NaN and Infinity handling depends on DuckDB support
      db[:edge_case_test].insert(id: 3, string_field: "test", int_field: 999, float_field: Float::MAX)
    end
  end

  def test_binary_data_handling
    db = create_db

    db.create_table(:binary_test) do
      primary_key :id
      column :blob_field, :blob
      column :binary_field, :binary
    end

    # Test various binary data scenarios
    test_binary_data = [
      "Simple binary data".b,
      "\x00\x01\x02\x03\x04\x05".b, # Binary with null bytes
      "Binary with\nnewlines\nand\ttabs".b,
      "Binary with 'quotes' and \"double quotes\"".b,
      "\xFF\xFE\xFD\xFC".b, # High byte values
      "".b, # Empty binary data
      ("A" * 1000).b # Large binary data
    ]

    test_binary_data.each_with_index do |binary_data, index|
      assert_nothing_raised("Should handle binary data: #{binary_data.inspect}") do
        db[:binary_test].insert(id: index + 1, blob_field: binary_data, binary_field: binary_data)
      end

      # Retrieve and verify
      record = db[:binary_test].where(id: index + 1).first
      retrieved_blob = record[:blob_field]
      retrieved_binary = record[:binary_field]

      # DuckDB returns BLOB data as hex string, so we need to convert it back
      if retrieved_blob.is_a?(String) && retrieved_blob.match?(/\A[0-9a-fA-F]*\z/) && !retrieved_blob.empty?
        # Convert hex string back to binary
        retrieved_blob = [retrieved_blob].pack("H*").b
      end

      if retrieved_binary.is_a?(String) && retrieved_binary.match?(/\A[0-9a-fA-F]*\z/) && !retrieved_binary.empty?
        # Convert hex string back to binary
        retrieved_binary = [retrieved_binary].pack("H*").b
      end

      assert_equal binary_data, retrieved_blob, "BLOB field should match inserted binary data"
      assert_equal binary_data, retrieved_binary, "Binary field should match inserted binary data"
    end
  end

  # ========================================
  # LITERAL CONVERSION TESTS (TDD - TESTS FIRST)
  # ========================================
  # These tests verify SQL literal generation for various data types
  # using Sequel's mock database functionality

  def test_literal_string_append_basic
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test basic string literal generation
    sql = dataset.literal("simple string")
    assert_equal "'simple string'", sql, "Basic string should be quoted"

    # Test empty string
    sql = dataset.literal("")
    assert_equal "''", sql, "Empty string should be quoted"

    # Test string with spaces
    sql = dataset.literal("string with spaces")
    assert_equal "'string with spaces'", sql, "String with spaces should be quoted"
  end

  def test_literal_string_append_escaping
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test single quote escaping
    sql = dataset.literal("string with 'single quotes'")
    assert_equal "'string with ''single quotes'''", sql, "Single quotes should be escaped by doubling"

    # Test multiple single quotes
    sql = dataset.literal("'multiple' 'quotes'")
    assert_equal "'''multiple'' ''quotes'''", sql, "Multiple single quotes should be escaped"

    # Test string that is only single quotes
    sql = dataset.literal("'")
    assert_equal "''''", sql, "Single quote should be escaped"

    # Test string with consecutive single quotes
    sql = dataset.literal("''")
    assert_equal "''''''", sql, "Consecutive single quotes should be escaped"
  end

  def test_literal_string_append_special_characters
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test newlines and tabs
    sql = dataset.literal("string\nwith\nnewlines")
    assert_equal "'string\nwith\nnewlines'", sql, "Newlines should be preserved in literals"

    sql = dataset.literal("string\twith\ttabs")
    assert_equal "'string\twith\ttabs'", sql, "Tabs should be preserved in literals"

    # Test carriage returns
    sql = dataset.literal("string\rwith\rcarriage\rreturns")
    assert_equal "'string\rwith\rcarriage\rreturns'", sql, "Carriage returns should be preserved"

    # Test mixed whitespace
    sql = dataset.literal("string\n\t\rwith\n\t\rmixed")
    assert_equal "'string\n\t\rwith\n\t\rmixed'", sql, "Mixed whitespace should be preserved"
  end

  def test_literal_string_append_unicode
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test Unicode characters
    sql = dataset.literal("Unicode: 你好世界")
    assert_equal "'Unicode: 你好世界'", sql, "Unicode characters should be preserved"

    # Test emojis
    sql = dataset.literal("Emoji: 🌍🚀💻")
    assert_equal "'Emoji: 🌍🚀💻'", sql, "Emojis should be preserved"

    # Test mixed Unicode and ASCII
    sql = dataset.literal("Mixed: Hello 世界 🌍")
    assert_equal "'Mixed: Hello 世界 🌍'", sql, "Mixed Unicode and ASCII should be preserved"
  end

  def test_literal_date_conversion
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test basic date literal
    date = Date.new(2023, 12, 25)
    sql = dataset.literal(date)
    assert_equal "'2023-12-25'", sql, "Date should be formatted as ISO string"

    # Test different date formats
    date = Date.new(1990, 1, 1)
    sql = dataset.literal(date)
    assert_equal "'1990-01-01'", sql, "Date should maintain ISO format"

    # Test leap year date
    date = Date.new(2024, 2, 29)
    sql = dataset.literal(date)
    assert_equal "'2024-02-29'", sql, "Leap year date should be handled correctly"

    # Test current date
    today = Date.today
    sql = dataset.literal(today)
    expected = "'#{today.strftime("%Y-%m-%d")}'"
    assert_equal expected, sql, "Current date should be formatted correctly"
  end

  def test_literal_datetime_conversion
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test basic datetime literal - use literal_datetime directly
    datetime = Time.new(2023, 12, 25, 14, 30, 45)
    sql = dataset.send(:literal_datetime, datetime)
    assert_equal "'2023-12-25 14:30:45'", sql, "DateTime should be formatted as ISO string"

    # Test datetime with seconds
    datetime = Time.new(2023, 1, 1, 0, 0, 0)
    sql = dataset.send(:literal_datetime, datetime)
    assert_equal "'2023-01-01 00:00:00'", sql, "DateTime with zero time should be formatted correctly"

    # Test datetime with different time
    datetime = Time.new(2023, 6, 15, 23, 59, 59)
    sql = dataset.send(:literal_datetime, datetime)
    assert_equal "'2023-06-15 23:59:59'", sql, "DateTime with max time should be formatted correctly"

    # Test DateTime object (not Time)
    datetime = DateTime.new(2023, 3, 20, 12, 15, 30)
    sql = dataset.send(:literal_datetime, datetime)
    assert_equal "'2023-03-20 12:15:30'", sql, "DateTime object should be formatted correctly"
  end

  def test_literal_time_conversion
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test basic time literal - use literal_time directly
    time = Time.new(2023, 1, 1, 14, 30, 45)
    sql = dataset.send(:literal_time, time)
    assert_equal "'14:30:45'", sql, "Time should extract only time component"

    # Test midnight
    time = Time.new(2023, 1, 1, 0, 0, 0)
    sql = dataset.send(:literal_time, time)
    assert_equal "'00:00:00'", sql, "Midnight should be formatted correctly"

    # Test end of day
    time = Time.new(2023, 1, 1, 23, 59, 59)
    sql = dataset.send(:literal_time, time)
    assert_equal "'23:59:59'", sql, "End of day should be formatted correctly"

    # Test noon
    time = Time.new(2023, 1, 1, 12, 0, 0)
    sql = dataset.send(:literal_time, time)
    assert_equal "'12:00:00'", sql, "Noon should be formatted correctly"
  end

  def test_literal_boolean_conversion
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test true literal
    sql = dataset.literal(true)
    assert_equal "TRUE", sql, "Boolean true should be converted to TRUE"

    # Test false literal
    sql = dataset.literal(false)
    assert_equal "FALSE", sql, "Boolean false should be converted to FALSE"
  end

  def test_literal_null_value_handling
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test nil literal
    sql = dataset.literal(nil)
    assert_equal "NULL", sql, "nil should be converted to NULL"
  end

  def test_literal_integer_conversion
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test positive integer
    sql = dataset.literal(42)
    assert_equal "42", sql, "Positive integer should be converted as-is"

    # Test negative integer
    sql = dataset.literal(-42)
    assert_equal "-42", sql, "Negative integer should be converted as-is"

    # Test zero
    sql = dataset.literal(0)
    assert_equal "0", sql, "Zero should be converted as-is"

    # Test large integer
    sql = dataset.literal(2_147_483_647)
    assert_equal "2147483647", sql, "Large integer should be converted as-is"
  end

  def test_literal_float_conversion
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test positive float
    sql = dataset.literal(3.14159)
    assert_equal "3.14159", sql, "Positive float should be converted as-is"

    # Test negative float
    sql = dataset.literal(-3.14159)
    assert_equal "-3.14159", sql, "Negative float should be converted as-is"

    # Test zero float
    sql = dataset.literal(0.0)
    assert_equal "0.0", sql, "Zero float should be converted as-is"

    # Test scientific notation
    sql = dataset.literal(1.23e10)
    assert_equal "12300000000.0", sql, "Scientific notation should be converted to decimal"

    sql = dataset.literal(1.23e-10)
    assert_equal "1.23e-10", sql, "Small scientific notation should be preserved"
  end

  def test_literal_conversion_edge_cases
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test very long string
    long_string = "x" * 1000
    sql = dataset.literal(long_string)
    expected = "'#{"x" * 1000}'"
    assert_equal expected, sql, "Very long string should be handled correctly"

    # Test string with only quotes
    input_string = "''" # Two single quotes
    sql = dataset.literal(input_string)
    # Input: '' (2 quotes)
    # Each quote becomes '' (doubled)
    # So '' becomes ''''
    # Wrapped in quotes: ''''''
    assert_equal "''''''", sql, "String with only quotes should be escaped correctly"

    # Test empty string edge case
    sql = dataset.literal("")
    assert_equal "''", sql, "Empty string should produce empty quotes"

    # Test string with null character (if supported)
    string_with_null = "before\x00after"
    sql = dataset.literal(string_with_null)
    assert_equal "'before\x00after'", sql, "String with null character should be preserved"
  end

  def test_literal_conversion_in_where_clauses
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    # Test that literal conversion works in WHERE clauses
    dataset = db[:users].where(name: "John's Data")
    sql = dataset.sql
    assert_match(/name = 'John''s Data'/, sql, "String literals in WHERE should be escaped")

    # Test with boolean
    dataset = db[:users].where(active: true)
    sql = dataset.sql
    assert_match(/active IS TRUE/, sql, "Boolean literals in WHERE should be converted")

    # Test with date
    date = Date.new(2023, 12, 25)
    dataset = db[:users].where(created_at: date)
    sql = dataset.sql
    assert_match(/created_at = '2023-12-25'/, sql, "Date literals in WHERE should be formatted")

    # Test with nil
    dataset = db[:users].where(deleted_at: nil)
    sql = dataset.sql
    assert_match(/deleted_at IS NULL/, sql, "Nil literals in WHERE should use IS NULL")
  end

  def test_literal_conversion_in_insert_statements
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    # Test literal conversion in INSERT
    dataset = db[:users]
    insert_sql = dataset.insert_sql(
      name: "John's Data",
      active: true,
      created_at: Date.new(2023, 12, 25),
      score: 3.14,
      count: 42,
      notes: nil
    )

    # Check the generated SQL
    assert_match(/name.*'John''s Data'/, insert_sql, "String should be escaped in INSERT")
    assert_match(/active.*TRUE/, insert_sql, "Boolean should be converted in INSERT")
    assert_match(/created_at.*'2023-12-25'/, insert_sql, "Date should be formatted in INSERT")
    assert_match(/score.*3\.14/, insert_sql, "Float should be converted in INSERT")
    assert_match(/count.*42/, insert_sql, "Integer should be converted in INSERT")
    assert_match(/notes.*NULL/, insert_sql, "Nil should be converted to NULL in INSERT")
  end

  # ========================================
  # BINARY DATA AND NUMERIC TYPE TESTS (TDD - TESTS FIRST)
  # ========================================

  def test_literal_binary_data_conversion
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test basic binary data literal
    binary_data = "binary data".b
    sql = dataset.literal(binary_data)
    # Binary data should be converted to hex format for DuckDB (without \x prefix)
    expected = "'#{binary_data.unpack1("H*")}'"
    assert_equal expected, sql, "Binary data should be converted to hex format"

    # Test binary data with null bytes
    binary_data = "\x00\x01\x02\x03".b
    sql = dataset.literal(binary_data)
    expected = "'00010203'"
    assert_equal expected, sql, "Binary data with null bytes should be hex encoded"

    # Test empty binary data
    binary_data = "".b
    sql = dataset.literal(binary_data)
    expected = "''"
    assert_equal expected, sql, "Empty binary data should produce empty hex"

    # Test binary data with high byte values
    binary_data = "\xFF\xFE\xFD".b
    sql = dataset.literal(binary_data)
    expected = "'fffefd'"
    assert_equal expected, sql, "High byte values should be hex encoded"
  end

  def test_literal_numeric_precision_handling
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test integer boundary values
    sql = dataset.literal(2_147_483_647) # Max 32-bit signed int
    assert_equal "2147483647", sql, "Max 32-bit integer should be handled"

    sql = dataset.literal(-2_147_483_648) # Min 32-bit signed int
    assert_equal "-2147483648", sql, "Min 32-bit integer should be handled"

    sql = dataset.literal(9_223_372_036_854_775_807) # Max 64-bit signed int
    assert_equal "9223372036854775807", sql, "Max 64-bit integer should be handled"

    # Test float precision
    sql = dataset.literal(1.7976931348623157e+308)  # Near Float::MAX
    assert sql.include?("1.797693134862315") || sql.include?("1.797693134862316"),
           "Large float should preserve precision"

    sql = dataset.literal(2.2250738585072014e-308)  # Near Float::MIN
    assert sql.include?("2.225073858507201e-308") || sql.include?("2.2250738585072014e-308"),
           "Small float should preserve precision"

    # Test decimal precision
    sql = dataset.literal(123.456789012345)
    assert_equal "123.456789012345", sql, "Decimal precision should be preserved"
  end

  def test_literal_numeric_special_values
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    dataset = db[:test_table]

    # Test positive and negative zero
    sql = dataset.literal(0.0)
    assert_equal "0.0", sql, "Positive zero should be handled"

    sql = dataset.literal(-0.0)
    assert_equal "-0.0", sql, "Negative zero should be handled"

    # Test infinity (if supported)
    begin
      sql = dataset.literal(Float::INFINITY)
      assert sql.include?("Infinity") || sql.include?("inf"), "Positive infinity should be handled"
    rescue StandardError
      # Skip if infinity is not supported
    end

    begin
      sql = dataset.literal(-Float::INFINITY)
      assert sql.include?("-Infinity") || sql.include?("-inf"), "Negative infinity should be handled"
    rescue StandardError
      # Skip if negative infinity is not supported
    end

    # Test NaN (if supported)
    begin
      sql = dataset.literal(Float::NAN)
      assert sql.include?("NaN") || sql.include?("nan"), "NaN should be handled"
    rescue StandardError
      # Skip if NaN is not supported
    end
  end

  def test_ruby_to_duckdb_type_conversion_edge_cases
    db = create_db

    db.create_table(:conversion_edge_test) do
      primary_key :id
      Integer :int_field
      Float :float_field
      String :string_field
      column :blob_field, :blob
    end

    # Test integer boundary handling
    large_int = 2_147_483_647 # Max 32-bit signed int (DuckDB INTEGER is INT32)
    assert_nothing_raised("Should handle large integer") do
      db[:conversion_edge_test].insert(id: 1, int_field: large_int)
    end

    record = db[:conversion_edge_test].where(id: 1).first
    assert_equal large_int, record[:int_field], "Large integer should be preserved"

    # Test float precision preservation
    precise_float = 123.456789012345
    assert_nothing_raised("Should handle precise float") do
      db[:conversion_edge_test].insert(id: 2, float_field: precise_float)
    end

    record = db[:conversion_edge_test].where(id: 2).first
    assert_in_delta precise_float, record[:float_field], 0.000000000001, "Float precision should be preserved"

    # Test string to numeric coercion
    assert_nothing_raised("Should coerce string to integer") do
      db[:conversion_edge_test].insert(id: 3, int_field: "12345")
    end

    record = db[:conversion_edge_test].where(id: 3).first
    assert_equal 12_345, record[:int_field], "String should be coerced to integer"

    # Test string to float coercion
    assert_nothing_raised("Should coerce string to float") do
      db[:conversion_edge_test].insert(id: 4, float_field: "123.45")
    end

    record = db[:conversion_edge_test].where(id: 4).first
    assert_in_delta 123.45, record[:float_field], 0.01, "String should be coerced to float"

    # Test binary data handling
    binary_data = "\x00\x01\x02\x03\xFF\xFE".b
    assert_nothing_raised("Should handle binary data") do
      db[:conversion_edge_test].insert(id: 5, blob_field: binary_data)
    end

    record = db[:conversion_edge_test].where(id: 5).first
    retrieved_binary = record[:blob_field]

    # DuckDB returns BLOB data as hex string, so we need to convert it back
    if retrieved_binary.is_a?(String) && retrieved_binary.match?(/\A[0-9a-fA-F]*\z/) && !retrieved_binary.empty?
      retrieved_binary = [retrieved_binary].pack("H*").b
    end

    assert_equal binary_data, retrieved_binary, "Binary data should be preserved"
  end

  def test_numeric_type_boundary_values
    db = create_db

    db.create_table(:boundary_test) do
      primary_key :id
      Integer :small_int
      column :big_int, :bigint # Use BIGINT for 64-bit integers
      Float :float_val
      Float :double_val
    end

    # Test integer boundaries
    test_integers = [
      0,
      1,
      -1,
      127,          # Max signed 8-bit
      -128,         # Min signed 8-bit
      32_767,        # Max signed 16-bit
      -32_768,       # Min signed 16-bit
      2_147_483_647, # Max signed 32-bit
      -2_147_483_648 # Min signed 32-bit
    ]

    test_integers.each_with_index do |test_int, index|
      assert_nothing_raised("Should handle boundary integer: #{test_int}") do
        db[:boundary_test].insert(id: index + 1, small_int: test_int, big_int: test_int)
      end

      record = db[:boundary_test].where(id: index + 1).first
      assert_equal test_int, record[:small_int], "Small int should handle boundary value"
      assert_equal test_int, record[:big_int], "Big int should handle boundary value"
    end

    # Test float boundaries
    test_floats = [
      0.0,
      1.0,
      -1.0,
      Float::MIN,
      -Float::MIN,
      1.0e-10,
      -1.0e-10,
      1.0e10,
      -1.0e10
    ]

    test_floats.each_with_index do |test_float, index|
      next_id = test_integers.length + index + 1
      assert_nothing_raised("Should handle boundary float: #{test_float}") do
        db[:boundary_test].insert(id: next_id, float_val: test_float, double_val: test_float)
      end

      record = db[:boundary_test].where(id: next_id).first
      assert_in_delta test_float, record[:float_val], 1e-15, "Float should handle boundary value"
      assert_in_delta test_float, record[:double_val], 1e-15, "Double should handle boundary value"
    end
  end

  def test_binary_data_literal_in_sql_context
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    # Test binary data in WHERE clauses
    binary_data = "test\x00data".b
    dataset = db[:files].where(content: binary_data)
    sql = dataset.sql
    expected_hex = binary_data.unpack1("H*")
    assert_match(/content = '#{expected_hex}'/, sql, "Binary data in WHERE should be hex encoded")

    # Test binary data in INSERT statements
    dataset = db[:files]
    insert_sql = dataset.insert_sql(name: "test.bin", content: binary_data)
    assert_match(/content.*'#{expected_hex}'/, insert_sql, "Binary data in INSERT should be hex encoded")
  end

  def test_numeric_precision_in_sql_context
    db = Sequel.mock(host: "duckdb")
    db.extend_datasets(Sequel::DuckDB::DatasetMethods)

    # Test high precision decimal in WHERE
    precise_decimal = 123.456789012345
    dataset = db[:measurements].where(value: precise_decimal)
    sql = dataset.sql
    assert_match(/value = 123\.456789012345/, sql, "Precise decimal should be preserved in WHERE")

    # Test large integer in INSERT
    large_int = 9_223_372_036_854_775_807
    dataset = db[:counters]
    insert_sql = dataset.insert_sql(count: large_int)
    assert_match(/count.*9223372036854775807/, insert_sql, "Large integer should be preserved in INSERT")
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
