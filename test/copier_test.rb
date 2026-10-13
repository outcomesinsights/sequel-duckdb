# frozen_string_literal: true

require_relative "spec_helper"
require_relative "../lib/sequel/duckdb/helpers/copier"
require "tmpdir"
require "fileutils"

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
    expected_sql = "COPY (SELECT * FROM data) TO '/tmp/data.csv' (FORMAT CSV, DELIMITER '|')"

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

  # Current behaviour that the quoting rules must keep.

  def test_symbol_format_is_upcased_bare
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out.parquet", format: :parquet, compression: :zstd)

    assert_equal "COPY (SELECT 1) TO '/tmp/out.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)", copier.to_sql
  end

  def test_integer_option_is_bare_number
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out.parquet", row_group_size: 100_000)

    assert_equal "COPY (SELECT 1) TO '/tmp/out.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 100000)", copier.to_sql
  end

  def test_explicit_format_overrides_extension
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out.txt", format: :csv)

    assert_equal "COPY (SELECT 1) TO '/tmp/out.txt' (FORMAT CSV)", copier.to_sql
  end

  # Quoting and format inference.

  def test_doubles_single_quotes_in_the_destination
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/o'x/out.parquet")

    assert_equal "COPY (SELECT 1) TO '/tmp/o''x/out.parquet' (FORMAT PARQUET)", copier.to_sql
  end

  def test_partition_by_string_is_a_quoted_identifier
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out", format: :parquet, partition_by: "Mixed Col")

    assert_equal "COPY (SELECT 1) TO '/tmp/out' (FORMAT PARQUET, PARTITION_BY (\"Mixed Col\"))", copier.to_sql
  end

  def test_partition_by_array_lists_quoted_identifiers
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out", format: :parquet, partition_by: %i[a b])

    assert_equal "COPY (SELECT 1) TO '/tmp/out' (FORMAT PARQUET, PARTITION_BY (\"a\", \"b\"))", copier.to_sql
  end

  def test_partition_by_doubles_double_quotes
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out", format: :parquet, partition_by: "a\"b")

    assert_equal "COPY (SELECT 1) TO '/tmp/out' (FORMAT PARQUET, PARTITION_BY (\"a\"\"b\"))", copier.to_sql
  end

  def test_non_word_string_value_is_a_quoted_string
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out.csv", delimiter: "|", quote: "'")

    assert_equal "COPY (SELECT 1) TO '/tmp/out.csv' (FORMAT CSV, DELIMITER '|', QUOTE '''')", copier.to_sql
  end

  def test_float_option_is_bare_number
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out.parquet", file_size_mb: 1.5)

    assert_equal "COPY (SELECT 1) TO '/tmp/out.parquet' (FORMAT PARQUET, FILE_SIZE_MB 1.5)", copier.to_sql
  end

  def test_extensionless_path_with_explicit_format
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/outdir", format: :parquet)

    assert_equal "COPY (SELECT 1) TO '/tmp/outdir' (FORMAT PARQUET)", copier.to_sql
  end

  def test_extensionless_path_without_format_raises
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/outdir")

    error = assert_raises(Sequel::Error) { copier.to_sql }

    assert_includes error.message, "/tmp/outdir"
    assert_includes error.message, "format:"
  end

  # Real in-memory DuckDB: the generated SQL must run.

  def test_real_copy_to_a_path_with_an_apostrophe
    with_real_db do |db, dir|
      path = File.join(dir, "o'x", "out.parquet")
      FileUtils.mkdir_p(File.dirname(path))
      db.run(Sequel::DuckDB::Helpers::Copier.new(db[:people], path).to_sql)

      assert_equal 3, db.from(Sequel.function(:read_parquet, path)).count
    end
  end

  def test_real_partitioned_copy_to_a_directory
    with_real_db do |db, dir|
      out = File.join(dir, "parts")
      db.run(Sequel::DuckDB::Helpers::Copier.new(db[:people], out, format: :parquet, partition_by: "Mixed Col").to_sql)

      # DuckDB percent-encodes the space in hive directory names.
      assert_equal [ "Mixed%20Col=x", "Mixed%20Col=y" ], Dir.children(out).sort
      assert_equal 3, db.from(Sequel.function(:read_parquet, File.join(out, "*", "*.parquet"))).count
    end
  end

  def test_real_csv_with_pipe_delimiter
    with_real_db do |db, dir|
      path = File.join(dir, "out.csv")
      db.run(Sequel::DuckDB::Helpers::Copier.new(db[:people].select(:id, :name), path, header: true, delimiter: "|").to_sql)

      assert_equal "id|name", File.readlines(path, chomp: true).first
    end
  end

  private

  def with_real_db
    db = SequelDuckDBTest.create_test_db
    db.run('CREATE TABLE people (id INTEGER, name VARCHAR, "Mixed Col" VARCHAR)')
    db.run("INSERT INTO people VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')")
    Dir.mktmpdir { |dir| yield db, dir }
  ensure
    db&.disconnect
  end
end
