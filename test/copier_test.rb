# frozen_string_literal: true

require_relative "spec_helper"
require_relative "../lib/sequel/duckdb/helpers/copier"

# Test suite for Copier helper
class CopierTest < SequelDuckDBTest::TestCase
  def test_copy_sql_query_to_parquet_file
    copier = Sequel::DuckDB::Helpers::Copier.new(
      "SELECT * FROM users WHERE age > 18",
      "/tmp/output.parquet"
    )
    expected_sql = "COPY (SELECT * FROM users WHERE age > 18) TO '/tmp/output.parquet' (FORMAT PARQUET)"

    assert_equal expected_sql, copier.to_sql
  end

  def test_copy_dataset_to_parquet_file
    dataset = mock_dataset(:users).where { age > 18 }
    copier = Sequel::DuckDB::Helpers::Copier.new(
      dataset,
      "/tmp/users.parquet"
    )
    expected_sql = "COPY (SELECT * FROM \"users\" WHERE (\"age\" > 18)) TO '/tmp/users.parquet' (FORMAT PARQUET)"

    assert_equal expected_sql, copier.to_sql
  end

  def test_copy_to_csv_with_header
    copier = Sequel::DuckDB::Helpers::Copier.new(
      "SELECT * FROM products",
      "/tmp/products.csv",
      format: "CSV", header: true
    )
    expected_sql = "COPY (SELECT * FROM products) TO '/tmp/products.csv' (FORMAT CSV, HEADER)"

    assert_equal expected_sql, copier.to_sql
  end

  def test_copy_to_csv_without_header
    copier = Sequel::DuckDB::Helpers::Copier.new(
      "SELECT * FROM products",
      "/tmp/products.csv",
      format: "CSV", header: false
    )
    expected_sql = "COPY (SELECT * FROM products) TO '/tmp/products.csv' (FORMAT CSV)"

    assert_equal expected_sql, copier.to_sql
  end

  def test_copy_with_delimiter_option
    copier = Sequel::DuckDB::Helpers::Copier.new(
      "SELECT * FROM data",
      "/tmp/data.csv",
      format: "CSV", delimiter: "|"
    )
    expected_sql = "COPY (SELECT * FROM data) TO '/tmp/data.csv' (FORMAT CSV, DELIMITER |)"

    assert_equal expected_sql, copier.to_sql
  end

  def test_copy_to_json_file
    copier = Sequel::DuckDB::Helpers::Copier.new(
      "SELECT * FROM logs",
      "/tmp/logs.json",
      format: "JSON"
    )
    expected_sql = "COPY (SELECT * FROM logs) TO '/tmp/logs.json' (FORMAT JSON)"

    assert_equal expected_sql, copier.to_sql
  end

  def test_copy_with_compression_option
    copier = Sequel::DuckDB::Helpers::Copier.new(
      "SELECT * FROM users",
      "/tmp/users.parquet",
      format: "PARQUET", compression: "SNAPPY"
    )
    expected_sql = "COPY (SELECT * FROM users) TO '/tmp/users.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)"

    assert_equal expected_sql, copier.to_sql
  end

  def test_copy_with_multiple_options
    copier = Sequel::DuckDB::Helpers::Copier.new(
      "SELECT id, name, email FROM customers",
      "/tmp/customers.csv",
      format: "CSV", header: true, delimiter: ",", quote: "\""
    )

    # The output should contain all the options
    actual_sql = copier.to_sql

    assert_includes actual_sql, "COPY (SELECT id, name, email FROM customers) TO '/tmp/customers.csv'"
    assert_includes actual_sql, "FORMAT CSV"
    assert_includes actual_sql, "HEADER"
  end

  def test_source_with_string_query
    copier = Sequel::DuckDB::Helpers::Copier.new(
      "SELECT * FROM test",
      "/tmp/test.parquet"
    )

    assert_equal "SELECT * FROM test", copier.source
  end

  def test_source_with_dataset
    dataset = mock_dataset(:users).select(:id, :name).where(active: true)
    copier = Sequel::DuckDB::Helpers::Copier.new(
      dataset,
      "/tmp/users.parquet"
    )
    expected_sql = "SELECT \"id\", \"name\" FROM \"users\" WHERE (\"active\" IS TRUE)"

    assert_equal expected_sql, copier.source
  end

  def test_copy_with_no_options
    copier = Sequel::DuckDB::Helpers::Copier.new(
      "SELECT * FROM simple",
      "/tmp/simple.parquet"
    )
    expected_sql = "COPY (SELECT * FROM simple) TO '/tmp/simple.parquet' (FORMAT PARQUET)"

    assert_equal expected_sql, copier.to_sql
  end
end
