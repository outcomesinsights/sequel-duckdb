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

  # The same merge happens between a day interval and a time interval of equal length.
  # The results differ only for a TIMESTAMPTZ across a daylight-saving change:
  # 2026-03-08 is the change in America/Los_Angeles, so 1 day after noon on 03-07 is
  # noon local time, and 24 hours after it is 13:00 local time.
  def local_times(*exprs)
    @db.run("SET GLOBAL TimeZone = 'America/Los_Angeles'")
    ds = @db.from(@db.select(Sequel.lit("TIMESTAMPTZ '2026-03-07 12:00:00-08'").as(:d)).as(:t))
    row = ds.select(*exprs.each_with_index.map { |e, i| Sequel.as(e, :"c#{i}") }).first
    row.values.map { |t| t.strftime("%m-%d %H:%M") }
  end

  def test_1_day_and_24_hours_across_daylight_saving
    assert_equal [ "03-08 12:00", "03-08 13:00" ],
                 local_times(Sequel.date_add(:d, days: 1), Sequel.date_add(:d, hours: 24))
    assert_equal [ "03-08 13:00", "03-08 12:00" ],
                 local_times(Sequel.date_add(:d, hours: 24), Sequel.date_add(:d, days: 1))
  end

  def test_aggregates_of_1_day_and_24_hours
    assert_equal [ "03-08 12:00", "03-08 13:00" ],
                 local_times(Sequel.function(:max, Sequel.date_add(:d, days: 1)),
                             Sequel.function(:max, Sequel.date_add(:d, hours: 24)))
  end

  # A time interval written as a subtraction, like months, would merge with months.
  def test_720_hours_with_1_month_and_with_30_days
    assert_equal [ "04-07 12:00", "04-06 13:00", "04-06 12:00" ],
                 local_times(Sequel.date_add(:d, months: 1), Sequel.date_add(:d, hours: 720),
                             Sequel.date_add(:d, days: 30))
  end

  def test_minutes_seconds_and_several_units
    assert_equal [ "03-08 13:00", "03-08 13:00", "03-08 15:00" ],
                 local_times(Sequel.date_add(:d, minutes: 1440), Sequel.date_add(:d, seconds: 86_400),
                             Sequel.date_add(:d, days: 1, hours: 3))
  end

  def test_date_sub_hours
    assert_equal [ "03-07 11:00", "03-06 12:00" ],
                 local_times(Sequel.date_sub(:d, hours: 1), Sequel.date_sub(:d, days: 1))
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
    assert_equal 'SELECT CAST((INTERVAL 24 HOUR + "d") AS timestamp) FROM "t"',
                 db[:t].select(Sequel.date_add(:d, hours: 24)).sql
    assert_equal 'SELECT CAST((INTERVAL (-5) MINUTE + "d") AS timestamp) FROM "t"',
                 db[:t].select(Sequel.date_sub(:d, minutes: 5)).sql
  end
end
