# frozen_string_literal: true

require "minitest/autorun"
require "minitest/pride"
require "sequel"
require_relative "../lib/sequel/adapters/duckdb"

# Test that the DuckDB helpers can be loaded via Sequel's mock adapter,
# which loads shared/duckdb.rb before Sequel::DuckDB is fully defined.
# This exercises the load path: mock adapter -> shared/duckdb -> helpers.
#
# Regression test: compact module notation (module Sequel::DuckDB::Helpers)
# raises NameError when Sequel::DuckDB hasn't been defined yet.
class MockAdapterTest < Minitest::Test
  def test_mock_duckdb_connection_sets_database_type
    db = Sequel.mock(host: :duckdb)

    assert_equal :duckdb, db.database_type
  end

  def test_mock_duckdb_dataset_supports_ctes
    db = Sequel.mock(host: :duckdb)

    assert_equal true, db.dataset.send(:supports_cte?)
  end

  def test_mock_duckdb_uses_duckdb_interval_sql
    db = Sequel.mock(host: :duckdb)
    db.extension :date_arithmetic

    sql = db[:items].select(Sequel.date_add(:start_date, days: 2).as(:shifted)).sql

    assert_match(/INTERVAL 2 DAY|INTERVAL '2 day'|INTERVAL \(2\) DAY/i, sql)
  end

  def test_pathifier_accessible_via_mock_adapter
    Sequel.mock(host: :duckdb)
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/data/test.parquet")

    refute_nil pathifier
  end

  def test_copier_accessible_via_mock_adapter
    Sequel.mock(host: :duckdb)
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out.parquet")

    refute_nil copier
  end
end
