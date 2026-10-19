# frozen_string_literal: true

require_relative "spec_helper"

# TIME and TIME_NS values are Sequel::SQLTime, both when a query loads them and when a
# model typecasts an assigned value. TIMETZ values stay the Time that ruby-duckdb returns.
class TimeValuesTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.run("CREATE TABLE events (id INTEGER PRIMARY KEY, starts_at TIME, ns TIME_NS, tz TIMETZ)")
    @db.run(<<~SQL)
      INSERT INTO events VALUES
        (1, '13:45:30.123456', '13:45:30.123456789', '13:45:30.123456+02:00'),
        (2, NULL, NULL, NULL)
    SQL
    @model = Class.new(Sequel::Model(@db[:events])) do
      unrestrict_primary_key
    end
  end

  def teardown
    @db&.disconnect
    super
  end

  def row(id = 1)
    @db[:events].where(id: id).first
  end

  def assert_time_of_day(expected, value)
    assert_instance_of Sequel::SQLTime, value
    assert_equal expected, [ value.hour, value.min, value.sec, value.nsec ]
  end

  def test_time_result_is_sqltime_with_fraction
    assert_time_of_day [ 13, 45, 30, 123_456_000 ], row[:starts_at]
  end

  def test_time_ns_result_is_sqltime_with_nanoseconds
    assert_time_of_day [ 13, 45, 30, 123_456_789 ], row[:ns]
  end

  def test_timetz_result_stays_a_time_with_its_offset
    value = row[:tz]

    assert_instance_of Time, value
    assert_equal [ 13, 45, 30, 123_456_000, 7200 ], [ value.hour, value.min, value.sec, value.nsec, value.utc_offset ]
  end

  def test_null_time_values_stay_nil
    assert_nil row(2)[:starts_at]
    assert_nil row(2)[:ns]
    assert_nil row(2)[:tz]
  end

  def test_model_typecasts_assigned_string_to_sqltime
    event = @model.new
    event.starts_at = "13:45:30"

    assert_time_of_day [ 13, 45, 30, 0 ], event.starts_at
  end

  def test_assigning_the_loaded_time_as_a_string_changes_nothing
    event = @model[1]
    event.starts_at = "13:45:30.123456"

    assert_empty event.changed_columns
  end

  def test_assigning_the_loaded_time_value_changes_nothing
    event = @model[1]
    event.set(starts_at: event.starts_at, ns: event.ns)

    assert_empty event.changed_columns
  end

  def test_saved_sqltime_round_trips_with_fraction
    @model.create(id: 3, starts_at: "08:09:10.5")

    assert_time_of_day [ 8, 9, 10, 500_000_000 ], row(3)[:starts_at]
  end

  def test_sqltime_literal_is_a_time_of_day
    assert_equal "'01:02:03.000004'", @db.literal(Sequel::SQLTime.create(1, 2, 3, 4))
  end

  def test_where_on_sqltime_matches_the_row
    assert_equal 1, @db[:events].where(starts_at: Sequel::SQLTime.create(13, 45, 30, 123_456)).count
  end

  # A plain Time is a timestamp, whatever its date. A Time on 1970-01-01 was once
  # written as a time of day; Sequel::SQLTime is the class for that.
  def test_time_on_1970_01_01_is_written_as_a_timestamp
    time = Time.local(1970, 1, 1, 9, 30, 0)

    assert_equal "'1970-01-01 09:30:00'", @db.literal(time)
    assert_time_of_day [ 9, 30, 0, 0 ], @db.get(Sequel.cast(time, :time))
  end
end
