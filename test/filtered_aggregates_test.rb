# frozen_string_literal: true

require_relative "spec_helper"

# DuckDB supports the aggregate FILTER clause natively. Sequel emulates FILTER
# with CASE when a dataset does not declare supports_filtered_aggregates?, and
# that emulation gives wrong results for order-sensitive aggregates.
class FilteredAggregatesTest < SequelDuckDBTest::TestCase
  def test_supports_filtered_aggregates
    assert_predicate mock_dataset, :supports_filtered_aggregates?
  end

  def test_filtered_aggregate_literalizes_native_filter
    expr = Sequel.function(:first, :x).order(Sequel.desc(:ts)).filter(Sequel.~(x: nil))
    sql = mock_db[:t].select_group(:g).select_append(expr.as(:v)).sql

    assert_includes sql, %{first("x" ORDER BY "ts" DESC) FILTER (WHERE ("x" IS NOT NULL))}
    refute_includes sql, "CASE WHEN"
  end

  def test_filtered_count_star_literalizes_native_filter
    sql = mock_db[:t].select(Sequel.function(:count).*.filter(g: 1).as(:n)).sql

    assert_includes sql, %{count(*) FILTER (WHERE ("g" = 1))}
    refute_includes sql, "CASE WHEN"
  end

  # The emulated form, first(CASE WHEN x IS NOT NULL THEN x ELSE NULL END
  # ORDER BY ts DESC), returns NULL for g=1 because the latest row's x is NULL.
  def test_filtered_order_sensitive_aggregate_on_real_duckdb
    db = filter_test_db
    expr = Sequel.function(:first, :x).order(Sequel.desc(:ts)).filter(Sequel.~(x: nil))
    rows = db[:t].select_group(:g).select_append(expr.as(:v)).order(:g).to_hash(:g, :v)

    assert_equal({ 1 => 10, 2 => 20 }, rows)
  end

  def test_filtered_count_star_on_real_duckdb
    db = filter_test_db
    expr = Sequel.function(:count).*.filter(Sequel.~(x: nil))
    rows = db[:t].select_group(:g).select_append(expr.as(:n)).order(:g).to_hash(:g, :n)

    assert_equal({ 1 => 1, 2 => 1 }, rows)
  end

  private

  def filter_test_db
    db = create_db
    db.create_table(:t) do
      Integer :g
      Integer :x
      Integer :ts
    end
    db[:t].import(%i[g x ts], [ [ 1, 10, 1 ], [ 1, nil, 2 ], [ 2, nil, 1 ], [ 2, 20, 2 ] ])
    db
  end
end
