# frozen_string_literal: true

require_relative "spec_helper"
require_relative "../lib/sequel/duckdb/helpers/pathifier"

# Test Sequel::DuckDB::Helpers::Pathifier for SQL generation
class PathifierTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
  end

  def teardown
    @db&.disconnect
    super
  end

  # Test single parquet file
  def test_single_parquet_file_generates_read_parquet_sql
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.parquet")
    sql_expr = pathifier.to_sql

    assert_equal "read_parquet(['/path/to/file.parquet'])", @db.literal(sql_expr)
  end

  # Test multiple parquet files
  def test_multiple_parquet_files_generates_read_parquet_sql
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new([
                                                         "/path/to/file1.parquet",
                                                         "/path/to/file2.parquet",
                                                         "/path/to/file3.parquet"
                                                       ])
    sql_expr = pathifier.to_sql

    assert_equal "read_parquet(['/path/to/file1.parquet','/path/to/file2.parquet','/path/to/file3.parquet'])", @db.literal(sql_expr)
  end

  # Test single CSV file
  def test_single_csv_file_generates_read_csv_sql
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.csv")
    sql_expr = pathifier.to_sql

    assert_equal "read_csv(['/path/to/file.csv'])", @db.literal(sql_expr)
  end

  # Test multiple CSV files
  def test_multiple_csv_files_generates_read_csv_sql
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new([
                                                         "/data/file1.csv",
                                                         "/data/file2.csv"
                                                       ])
    sql_expr = pathifier.to_sql

    assert_equal "read_csv(['/data/file1.csv','/data/file2.csv'])", @db.literal(sql_expr)
  end

  # Test single JSON file
  def test_single_json_file_generates_read_json_sql
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/data.json")
    sql_expr = pathifier.to_sql

    assert_equal "read_json(['/path/to/data.json'])", @db.literal(sql_expr)
  end

  # Test multiple JSON files
  def test_multiple_json_files_generates_read_json_sql
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new([
                                                         "/data/file1.json",
                                                         "/data/file2.json",
                                                         "/data/file3.json"
                                                       ])
    sql_expr = pathifier.to_sql

    assert_equal "read_json(['/data/file1.json','/data/file2.json','/data/file3.json'])", @db.literal(sql_expr)
  end

  # Test using option to override format
  def test_using_option_overrides_file_extension
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.txt", using: :parquet)
    sql_expr = pathifier.to_sql

    assert_equal "read_parquet(['/path/to/file.txt'])", @db.literal(sql_expr)
  end

  def test_using_option_csv_overrides_extension
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.data", using: :csv)
    sql_expr = pathifier.to_sql

    assert_equal "read_csv(['/path/to/file.data'])", @db.literal(sql_expr)
  end

  def test_using_option_json_overrides_extension
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.bin", using: :json)
    sql_expr = pathifier.to_sql

    assert_equal "read_json(['/path/to/file.bin'])", @db.literal(sql_expr)
  end

  # Test validation errors
  def test_raises_error_when_no_paths_provided
    error = assert_raises(Sequel::Error) do
      Sequel::DuckDB::Helpers::Pathifier.new([])
    end

    assert_equal "No paths provided", error.message
  end

  def test_raises_error_when_multiple_different_extensions
    error = assert_raises(Sequel::Error) do
      Sequel::DuckDB::Helpers::Pathifier.new([
                                               "/path/to/file.parquet",
                                               "/path/to/file.csv"
                                             ])
    end

    assert_includes error.message, "Multiple different file extensions provided"
    assert_includes error.message, ".parquet"
    assert_includes error.message, ".csv"
  end

  def test_raises_error_for_unsupported_format
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.xlsx")

    error = assert_raises(Sequel::Error) do
      pathifier.to_sql
    end

    assert_includes error.message, "Unsupported :using type"
    assert_includes error.message, "xlsx"
  end

  def test_raises_error_for_unsupported_using_option
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.txt", using: :excel)

    error = assert_raises(Sequel::Error) do
      pathifier.to_sql
    end

    assert_includes error.message, "Unsupported :using type"
    assert_includes error.message, "excel"
  end

  # Test that same extension files are allowed
  def test_allows_multiple_files_with_same_extension
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new([
                                                         "/data/file1.parquet",
                                                         "/data/file2.parquet",
                                                         "/data/file3.parquet"
                                                       ])

    assert_nothing_raised do
      pathifier.to_sql
    end
  end

  # Test extnames method
  def test_extnames_returns_unique_extensions
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new([
                                                         "/data/file1.csv",
                                                         "/data/file2.csv",
                                                         "/data/file3.csv"
                                                       ])

    assert_equal [".csv"], pathifier.extnames
  end

  def test_extnames_returns_multiple_unique_extensions
    # This will fail validation, but we can test extnames before validation
    pathifier = Sequel::DuckDB::Helpers::Pathifier.allocate
    pathifier.instance_variable_set(:@paths, [
                                      Pathname.new("/data/file1.csv"),
                                      Pathname.new("/data/file2.parquet")
                                    ])
    pathifier.instance_variable_set(:@options, {})

    assert_equal [".csv", ".parquet"], pathifier.extnames
  end

  # Test to_format method
  def test_to_format_returns_symbol_from_extension
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.parquet")

    assert_equal :parquet, pathifier.to_format
  end

  def test_to_format_returns_using_option_when_provided
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.txt", using: :csv)

    assert_equal :csv, pathifier.to_format
  end

  def test_to_format_converts_string_to_symbol
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file.txt", using: "json")

    assert_equal :json, pathifier.to_format
  end

  # Test with relative paths
  def test_works_with_relative_paths
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("data/file.parquet")
    sql_expr = pathifier.to_sql

    assert_equal "read_parquet(['data/file.parquet'])", @db.literal(sql_expr)
  end

  # Test with paths containing special characters
  def test_handles_paths_with_spaces
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/my file.parquet")
    sql_expr = pathifier.to_sql

    assert_equal "read_parquet(['/path/to/my file.parquet'])", @db.literal(sql_expr)
  end

  def test_handles_paths_with_special_characters
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/path/to/file-name_2024.parquet")
    sql_expr = pathifier.to_sql

    assert_equal "read_parquet(['/path/to/file-name_2024.parquet'])", @db.literal(sql_expr)
  end

  # Test with glob patterns
  def test_handles_glob_pattern
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/data/*.parquet")
    sql_expr = pathifier.to_sql

    assert_equal "read_parquet(['/data/*.parquet'])", @db.literal(sql_expr)
  end
end
