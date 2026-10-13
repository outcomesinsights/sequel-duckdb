# frozen_string_literal: true

require_relative "spec_helper"

# DuckDB supports IS DISTINCT FROM natively. Sequel's is_distinct_from
# extension asks the dataset through supports_is_distinct_from?, and emits a
# CASE emulation (which DuckDB cannot run as a hash join) when it is false.
class IsDistinctFromTest < SequelDuckDBTest::TestCase
  def test_is_distinct_from_literalizes_natively
    db = mock_db
    db.extension :is_distinct_from
    sql = db[:t].where(Sequel.is_distinct_from(:a, :b)).sql

    assert_includes sql, %{("a" IS DISTINCT FROM "b")}
    refute_includes sql, "CASE"
  end

  def test_is_distinct_from_on_real_duckdb
    db = create_db
    db.extension :is_distinct_from
    db.create_table(:pairs) do
      Integer :id
      Integer :a
      Integer :b
    end
    db[:pairs].import(%i[id a b], [ [ 1, nil, nil ], [ 2, 1, nil ], [ 3, 1, 1 ], [ 4, 1, 2 ] ])

    ds = db[:pairs].select(:id, Sequel.is_distinct_from(:a, :b).as(:d))
    distinct = ds.to_hash(:id, :d)

    assert_includes ds.sql, "IS DISTINCT FROM"
    assert_equal({ 1 => false, 2 => true, 3 => false, 4 => true }, distinct)
  end
end
