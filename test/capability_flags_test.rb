# frozen_string_literal: true

require_relative "spec_helper"

# Sequel refuses a feature, or leaves out its SQL, when the adapter does not declare the
# capability flag for it. Each test here runs the feature against a real DuckDB, so a
# declared flag is a checked claim that DuckDB runs Sequel's SQL for it correctly.
class CapabilityFlagsTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.create_table(:t) do
      Integer :id, primary_key: true
      String :g
      Integer :v
    end
    @db[:t].multi_insert([ { id: 1, g: "abc", v: 1 }, { id: 2, g: "xAbc", v: 2 }, { id: 3, g: "zzz", v: 5 } ])
    @db.create_table(:u) do
      Integer :id
      Integer :w
    end
    @db[:u].multi_insert([ { id: 1, w: 10 }, { id: 4, w: 40 } ])
  end

  def teardown
    @db&.disconnect
    super
  end

  def t
    @db[:t]
  end

  def test_distinct_on
    rows = t.distinct(Sequel.function(:length, :g)).order(Sequel.function(:length, :g), Sequel.desc(:v)).select_map(:id)

    assert_equal [ 2, 3 ], rows.sort
  end

  def test_group_rollup
    rows = t.select(Sequel.function(:length, :g).as(:n), Sequel.function(:sum, :v).as(:s))
            .group(Sequel.function(:length, :g)).group_rollup.order(:n).map { |r| r.values_at(:n, :s) }

    assert_equal [ [ 3, 6 ], [ 4, 2 ], [ nil, 8 ] ], rows
  end

  def test_group_cube
    rows = t.select(:g, Sequel.function(:sum, :v).as(:s)).group(:g).group_cube.order(:g).map { |r| r.values_at(:g, :s) }

    assert_equal [ [ "abc", 1 ], [ "xAbc", 2 ], [ "zzz", 5 ], [ nil, 8 ] ], rows
  end

  def test_grouping_sets
    ds = t.select(:g, :id, Sequel.function(:sum, :v).as(:s)).group([ :g ], [ :id ]).grouping_sets

    assert_match(/GROUP BY GROUPING SETS/, ds.sql)
    assert_equal 6, ds.count
  end

  def test_lateral_subquery
    lateral = @db[:u].where(Sequel[:u][:id] => Sequel[:t][:id]).select(:w).lateral
    rows = t.cross_join(lateral.as(:l)).order(Sequel[:t][:id]).select_map([ Sequel[:t][:id], :w ])

    assert_equal [ [ 1, 10 ] ], rows
  end

  def test_window_clause
    ds = t.window(:w, partition: Sequel.function(:length, :g))
          .select(:id, Sequel.function(:sum, :v).over(window: :w).as(:s)).order(:id)

    assert_match(/WINDOW "w" AS/, ds.sql)
    assert_equal [ 6, 2, 6 ], ds.map(:s)
  end

  def test_merge
    t.merge_using(:u, Sequel[:u][:id] => Sequel[:t][:id])
     .merge_update(v: Sequel[:u][:w])
     .merge_insert(id: Sequel[:u][:id], g: "m", v: Sequel[:u][:w])
     .merge

    assert_equal [ [ 1, 10 ], [ 2, 2 ], [ 3, 5 ], [ 4, 40 ] ], t.order(:id).select_map(%i[id v])
  end

  def test_merge_delete
    t.merge_using(:u, Sequel[:u][:id] => Sequel[:t][:id]).merge_delete.merge

    assert_equal [ 2, 3 ], t.order(:id).select_map(:id)
  end

  def test_regexp
    assert_equal [ 1, 2 ], t.where(g: /bc/).order(:id).select_map(:id)
    assert_equal [ 1 ], t.where(g: /^a/).select_map(:id)
    assert_equal [ 2 ], t.where(g: /^XA/i).select_map(:id)
  end

  def test_negated_regexp
    assert_equal [ 3 ], t.exclude(g: /bc/).select_map(:id)
    assert_equal [ 1, 3 ], t.exclude(g: /^XA/i).order(:id).select_map(:id)
  end

  def test_create_table_if_not_exists
    2.times { @db.create_table?(:x) { Integer :a } }

    assert @db.table_exists?(:x)
    assert_match(/CREATE TABLE IF NOT EXISTS/, @db.send(:create_table_sql, :x, @db.create_table_generator { Integer :a }, if_not_exists: true))
  end

  def test_drop_table_if_exists
    @db.create_table(:x) { Integer :a }
    2.times { @db.drop_table?(:x) }

    refute @db.table_exists?(:x)
  end

  # Sequel calls supports_cte?(:insert), (:update) and (:delete), and writes the CTE
  # before the statement (bead sequel-duckdb-b7v).
  def cte_counting_large_values
    @db.select(Sequel[@db[:big].select { count(1) }].as(:n)).with(:big, t.where { v > 1 }, materialized: true)
  end

  def test_insert_from_a_dataset_with_a_cte
    @db.create_table(:etl) { Integer :n }
    @db[:etl].insert(cte_counting_large_values)
    @db[:etl].insert(cte_counting_large_values.from_self)

    assert_match(/\AWITH "big" AS MATERIALIZED/, @db[:etl].insert_sql(cte_counting_large_values))
    assert_equal [ 2, 2 ], @db[:etl].select_map(:n)
  end

  def test_update_with_a_cte
    t.with(:x, @db.select(Sequel.as(1, :id))).where(id: @db[:x].select(:id)).update(v: 9)

    assert_equal [ [ 1, 9 ], [ 2, 2 ], [ 3, 5 ] ], t.order(:id).select_map(%i[id v])
  end

  def test_delete_with_a_cte
    t.with(:x, @db.select(Sequel.as(2, :id))).where(id: @db[:x].select(:id)).delete

    assert_equal [ 1, 3 ], t.order(:id).select_map(:id)
  end
end
