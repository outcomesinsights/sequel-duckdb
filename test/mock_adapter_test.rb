# frozen_string_literal: true

require "minitest/autorun"
require "minitest/pride"
require "sequel"

# Test that the DuckDB helpers can be loaded via Sequel's mock adapter,
# which loads shared/duckdb.rb before Sequel::DuckDB is fully defined.
# This exercises the load path: mock adapter -> shared/duckdb -> helpers.
#
# Regression test: compact module notation (module Sequel::DuckDB::Helpers)
# raises NameError when Sequel::DuckDB hasn't been defined yet.
class MockAdapterTest < Minitest::Test
  def test_mock_duckdb_connection_succeeds
    db = Sequel.mock(host: "duckdb")
    refute_nil db
  end

  def test_pathifier_accessible_via_mock_adapter
    Sequel.mock(host: "duckdb")
    pathifier = Sequel::DuckDB::Helpers::Pathifier.new("/data/test.parquet")
    refute_nil pathifier
  end

  def test_copier_accessible_via_mock_adapter
    Sequel.mock(host: "duckdb")
    copier = Sequel::DuckDB::Helpers::Copier.new("SELECT 1", "/tmp/out.parquet")
    refute_nil copier
  end
end
