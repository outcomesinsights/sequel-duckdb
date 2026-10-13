# frozen_string_literal: true

require_relative "spec_helper"
require "tmpdir"

# Dataset#copy_to_sql returns the COPY text without running it;
# Database#copy_to runs that same text.
class CopyToSqlTest < SequelDuckDBTest::TestCase
  def test_copy_to_sql_returns_the_statement_and_runs_nothing
    db = Sequel.mock(host: :duckdb)

    assert_equal "COPY (SELECT * FROM \"t\") TO '/x/out.parquet' (FORMAT PARQUET)", db[:t].copy_to_sql("/x/out.parquet")
    assert_empty db.sqls
  end

  def test_copy_to_sql_partitioned_directory_with_explicit_format
    db = Sequel.mock(host: :duckdb)
    sql = db[:t].copy_to_sql("/x/parts", format: :parquet, partition_by: %i[a b], compression: :zstd)

    assert_equal "COPY (SELECT * FROM \"t\") TO '/x/parts' (FORMAT PARQUET, PARTITION_BY (\"a\", \"b\"), COMPRESSION ZSTD)", sql
    assert_empty db.sqls
  end

  def test_copy_to_runs_the_dataset_copy_to_sql
    db = Sequel.mock(host: :duckdb)
    db.copy_to(db[:t].where(id: 1), "/x/out.csv", header: true)

    assert_equal [ "COPY (SELECT * FROM \"t\" WHERE (\"id\" = 1)) TO '/x/out.csv' (FORMAT CSV, HEADER)" ], db.sqls
  end

  def test_copy_to_with_a_string_source_still_runs
    db = Sequel.mock(host: :duckdb)
    db.copy_to("SELECT 1", "/x/out.parquet")

    assert_equal [ "COPY (SELECT 1) TO '/x/out.parquet' (FORMAT PARQUET)" ], db.sqls
  end

  def test_copy_to_writes_a_partitioned_directory_on_real_duckdb
    db = SequelDuckDBTest.create_test_db
    db.run("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'x'), (2, 'y'), (3, 'x')) v(id, grp)")
    Dir.mktmpdir do |dir|
      out = File.join(dir, "parts")
      db.copy_to(db[:t], out, format: :parquet, partition_by: :grp)

      assert_equal 3, db.from(Sequel.function(:read_parquet, File.join(out, "*", "*.parquet"))).count
    end
  ensure
    db&.disconnect
  end
end
