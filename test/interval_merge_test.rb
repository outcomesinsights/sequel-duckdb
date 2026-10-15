# frozen_string_literal: true

require_relative "spec_helper"

# DuckDB judges INTERVAL 1 MONTH equal to INTERVAL 30 DAY. Its optimizer then merges
# d + INTERVAL 30 DAY with d + INTERVAL 1 MONTH into one expression, and both columns
# get the value of whichever comes first. date_add_sql_append emits month-based intervals
# as a subtraction of the negated value to keep the two expressions apart. These tests run
# against a real DuckDB, so they fail if a later DuckDB merges the two forms again.
class IntervalMergeTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.extension :date_arithmetic
  end

  def teardown
    @db&.disconnect
    super
  end

  def dates(date_string, *exprs)
    ds = @db.from(@db.select(Sequel.lit("DATE '#{date_string}'").as(:d)).as(:t))
    row = ds.select(*exprs.each_with_index.map { |e, i| Sequel.as(e, :"c#{i}") }).first
    row.values.map(&:to_date)
  end

  def test_30_days_and_1_month_in_one_select
    assert_equal [ Date.new(2008, 6, 16), Date.new(2008, 6, 17) ],
                 dates("2008-05-17", Sequel.date_add(:d, days: 30), Sequel.date_add(:d, months: 1))
  end

  def test_1_month_and_30_days_in_one_select
    assert_equal [ Date.new(2008, 6, 17), Date.new(2008, 6, 16) ],
                 dates("2008-05-17", Sequel.date_add(:d, months: 1), Sequel.date_add(:d, days: 30))
  end

  def test_aggregates_of_30_days_and_1_month
    assert_equal [ Date.new(2008, 6, 16), Date.new(2008, 6, 17) ],
                 dates("2008-05-17",
                       Sequel.function(:max, Sequel.date_add(:d, days: 30)),
                       Sequel.function(:max, Sequel.date_add(:d, months: 1)))
  end

  def test_360_days_and_1_year_in_one_select
    assert_equal [ Date.new(2009, 5, 12), Date.new(2009, 5, 17) ],
                 dates("2008-05-17", Sequel.date_add(:d, days: 360), Sequel.date_add(:d, years: 1))
  end

  def test_date_sub_30_days_and_1_month_in_one_select
    assert_equal [ Date.new(2008, 2, 16), Date.new(2008, 2, 17) ],
                 dates("2008-03-17", Sequel.date_sub(:d, days: 30), Sequel.date_sub(:d, months: 1))
  end

  def test_month_end_results
    assert_equal [ Date.new(2008, 4, 30), Date.new(2008, 2, 29) ],
                 dates("2008-03-31", Sequel.date_add(:d, months: 1), Sequel.date_sub(:d, months: 1))
  end

  def test_where_clause_with_both_intervals
    ds = @db.from(@db.select(Sequel.lit("DATE '2008-05-17'").as(:d)).as(:t))
            .where(Sequel.date_add(:d, days: 30) < Sequel.date_add(:d, months: 1))

    assert_equal 1, ds.count
  end

  def test_expression_month_value
    ds = @db.from(@db.select(Sequel.lit("DATE '2008-05-17'").as(:d), Sequel.lit("1").as(:n)).as(:t))
    row = ds.select(Sequel.date_add(:d, days: 30).as(:a), Sequel.date_add(:d, months: Sequel[:n]).as(:b),
                    Sequel.date_sub(:d, months: Sequel[:n]).as(:c)).first

    assert_equal [ Date.new(2008, 6, 16), Date.new(2008, 6, 17), Date.new(2008, 4, 17) ],
                 row.values.map(&:to_date)
  end

  def test_sql_form
    db = Sequel.mock(host: :duckdb)
    db.extension :date_arithmetic

    assert_equal 'SELECT CAST(("d" - INTERVAL (-1) MONTH) AS timestamp) FROM "t"',
                 db[:t].select(Sequel.date_add(:d, months: 1)).sql
    assert_equal 'SELECT CAST(("d" - INTERVAL 1 YEAR) AS timestamp) FROM "t"',
                 db[:t].select(Sequel.date_sub(:d, years: 1)).sql
    assert_equal 'SELECT CAST(("d" + INTERVAL 30 DAY) AS timestamp) FROM "t"',
                 db[:t].select(Sequel.date_add(:d, days: 30)).sql
  end
end
