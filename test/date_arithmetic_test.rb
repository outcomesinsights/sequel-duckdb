# frozen_string_literal: true

require_relative "spec_helper"

# Test Sequel's date_arithmetic extension with DuckDB adapter
class DateArithmeticTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.extension :date_arithmetic

    # Create test table with various date/time columns
    @db.create_table(:events) do
      Integer :id, primary_key: true
      Date :event_date
      DateTime :created_at
      DateTime :expires_at
      Integer :duration_days
    end

    # Insert test data
    @db[:events].insert(
      id: 1,
      event_date: Date.new(2024, 1, 15),
      created_at: Time.new(2024, 1, 15, 10, 30, 0),
      expires_at: Time.new(2024, 2, 15, 10, 30, 0),
      duration_days: 7
    )
  end

  def teardown
    @db.disconnect if @db
    super
  end

  # Test basic addition with single units

  def test_date_add_years
    result = @db[:events]
      .select(Sequel.date_add(:event_date, years: 1).as(:future_date))
      .first

    expected = Time.new(2025, 1, 15)
    assert_equal expected, result[:future_date]
  end

  def test_date_add_months
    result = @db[:events]
      .select(Sequel.date_add(:event_date, months: 2).as(:future_date))
      .first

    expected = Time.new(2024, 3, 15)
    assert_equal expected, result[:future_date]
  end

  def test_date_add_days
    result = @db[:events]
      .select(Sequel.date_add(:event_date, days: 5).as(:future_date))
      .first

    expected = Time.new(2024, 1, 20)
    assert_equal expected, result[:future_date]
  end

  def test_date_add_hours
    result = @db[:events]
      .select(Sequel.date_add(:created_at, hours: 5).as(:future_time))
      .first

    expected = Time.new(2024, 1, 15, 15, 30, 0)
    assert_equal expected, result[:future_time]
  end

  def test_date_add_minutes
    result = @db[:events]
      .select(Sequel.date_add(:created_at, minutes: 30).as(:future_time))
      .first

    expected = Time.new(2024, 1, 15, 11, 0, 0)
    assert_equal expected, result[:future_time]
  end

  def test_date_add_seconds
    result = @db[:events]
      .select(Sequel.date_add(:created_at, seconds: 90).as(:future_time))
      .first

    expected = Time.new(2024, 1, 15, 10, 31, 30)
    assert_equal expected, result[:future_time]
  end

  # Test multiple unit intervals

  def test_date_add_multiple_units
    result = @db[:events]
      .select(Sequel.date_add(:event_date, years: 1, months: 2, days: 3).as(:future_date))
      .first

    expected = Time.new(2025, 3, 18)
    assert_equal expected, result[:future_date]
  end

  def test_date_add_time_units_combined
    result = @db[:events]
      .select(Sequel.date_add(:created_at, hours: 2, minutes: 15, seconds: 30).as(:future_time))
      .first

    expected = Time.new(2024, 1, 15, 12, 45, 30)
    assert_equal expected, result[:future_time]
  end

  def test_date_add_all_units
    result = @db[:events]
      .select(Sequel.date_add(:created_at,
        years: 1, months: 1, days: 1, hours: 1, minutes: 1, seconds: 1
      ).as(:future_time))
      .first

    expected = Time.new(2025, 2, 16, 11, 31, 1)
    assert_equal expected, result[:future_time]
  end

  # Test subtraction

  def test_date_sub_days
    result = @db[:events]
      .select(Sequel.date_sub(:event_date, days: 5).as(:past_date))
      .first

    expected = Time.new(2024, 1, 10)
    assert_equal expected, result[:past_date]
  end

  def test_date_sub_months
    result = @db[:events]
      .select(Sequel.date_sub(:event_date, months: 1).as(:past_date))
      .first

    expected = Time.new(2023, 12, 15)
    assert_equal expected, result[:past_date]
  end

  def test_date_sub_hours_and_minutes
    result = @db[:events]
      .select(Sequel.date_sub(:created_at, hours: 2, minutes: 15).as(:past_time))
      .first

    expected = Time.new(2024, 1, 15, 8, 15, 0)
    assert_equal expected, result[:past_time]
  end

  # Test cast option

  def test_date_add_cast_to_date
    result = @db[:events]
      .select(Sequel.date_add(:event_date, {days: 7}, cast: :date).as(:future_date))
      .first

    # Cast to date should return Date object
    assert_instance_of Date, result[:future_date]
    assert_equal Date.new(2024, 1, 22), result[:future_date]
  end

  def test_date_add_default_cast_to_timestamp
    result = @db[:events]
      .select(Sequel.date_add(:event_date, days: 1).as(:future_date))
      .first

    # Default cast should be timestamp (Time)
    assert_instance_of Time, result[:future_date]
  end

  # Test with SQL expressions as values

  def test_date_add_with_column_reference
    result = @db[:events]
      .select(Sequel.date_add(:event_date, days: Sequel[:duration_days]).as(:future_date))
      .first

    expected = Time.new(2024, 1, 22)
    assert_equal expected, result[:future_date]
  end

  def test_date_add_with_expression
    result = @db[:events]
      .select(Sequel.date_add(:event_date, days: Sequel[:duration_days] * 2).as(:future_date))
      .first

    expected = Time.new(2024, 1, 29)
    assert_equal expected, result[:future_date]
  end

  # Test in WHERE clause

  def test_date_add_in_where_clause
    # Find events that expire within 24 hours
    @db[:events].insert(
      id: 2,
      event_date: Date.today,
      created_at: Time.now,
      expires_at: Time.now + (23 * 3600), # 23 hours from now
      duration_days: 1
    )

    # Events where (now + 24 hours) >= expires_at AND created_at is recent
    # This ensures we only get the newly inserted event, not the old one from setup
    results = @db[:events]
      .where(Sequel.date_add(Sequel::CURRENT_TIMESTAMP, hours: 24) >= :expires_at)
      .where { created_at > Sequel.date_sub(Sequel::CURRENT_TIMESTAMP, hours: 1) }
      .all

    assert_equal 1, results.length
    assert_equal 2, results[0][:id]
  end

  def test_date_sub_in_where_clause
    # Find events created in the last hour
    @db[:events].insert(
      id: 3,
      event_date: Date.today,
      created_at: Time.now,
      expires_at: Time.now + 3600,
      duration_days: 1
    )

    # Events where created_at > (now - 1 hour)
    results = @db[:events]
      .where { created_at > Sequel.date_sub(Sequel::CURRENT_TIMESTAMP, hours: 1) }
      .all

    assert_equal 1, results.length
    assert_equal 3, results[0][:id]
  end

  # Test SQL generation

  def test_sql_generation_single_interval
    ds = @db[:events]
      .select(Sequel.date_add(:created_at, days: 5).as(:result))

    sql = ds.sql
    assert_includes sql, "INTERVAL"
    assert_includes sql, "DAY"
    assert_includes sql, "CAST"
    assert_match(/timestamp/i, sql)  # Case insensitive match for timestamp
  end

  def test_sql_generation_multiple_intervals
    ds = @db[:events]
      .select(Sequel.date_add(:created_at, years: 1, months: 2).as(:result))

    sql = ds.sql
    assert_includes sql, "INTERVAL"
    assert_includes sql, "YEAR"
    assert_includes sql, "MONTH"
  end

  def test_sql_generation_with_subtraction
    ds = @db[:events]
      .select(Sequel.date_sub(:created_at, hours: 12).as(:result))

    sql = ds.sql
    # date_sub should generate negative interval
    assert_includes sql, "INTERVAL"
    assert_includes sql, "-12"
    assert_includes sql, "HOUR"
  end

  def test_sql_generation_with_cast
    ds = @db[:events]
      .select(Sequel.date_add(:event_date, {days: 7}, cast: :date).as(:result))

    sql = ds.sql
    assert_includes sql, "CAST"
    assert_match(/date/i, sql)  # Case insensitive match for date
  end

  # Test edge cases

  def test_date_add_zero_interval
    # Zero intervals should still work
    result = @db[:events]
      .select(Sequel.date_add(:event_date, days: 0).as(:same_date))
      .first

    expected = Time.new(2024, 1, 15)
    assert_equal expected, result[:same_date]
  end

  def test_date_add_negative_value
    # Can use negative values with date_add
    result = @db[:events]
      .select(Sequel.date_add(:event_date, days: -5).as(:past_date))
      .first

    expected = Time.new(2024, 1, 10)
    assert_equal expected, result[:past_date]
  end

  def test_date_add_large_values
    result = @db[:events]
      .select(Sequel.date_add(:event_date, years: 100).as(:far_future))
      .first

    expected = Time.new(2124, 1, 15)
    assert_equal expected, result[:far_future]
  end

  # Test with different column types

  def test_date_add_on_date_column
    result = @db[:events]
      .select(Sequel.date_add(:event_date, days: 1).as(:tomorrow))
      .first

    assert_instance_of Time, result[:tomorrow]
  end

  def test_date_add_on_datetime_column
    result = @db[:events]
      .select(Sequel.date_add(:created_at, days: 1).as(:tomorrow))
      .first

    assert_instance_of Time, result[:tomorrow]
  end

  # Test weeks conversion (weeks should be converted to days by Sequel)

  def test_date_add_weeks
    result = @db[:events]
      .select(Sequel.date_add(:event_date, weeks: 2).as(:future_date))
      .first

    # 2 weeks = 14 days
    expected = Time.new(2024, 1, 29)
    assert_equal expected, result[:future_date]
  end
end
