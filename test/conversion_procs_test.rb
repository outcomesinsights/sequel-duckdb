# frozen_string_literal: true

require "json"
require_relative "spec_helper"

# Database#add_conversion_proc registers a converter for one DuckDB column type.
# Dataset fetches apply it to the non-NULL values of the columns of that type only.
class ConversionProcsTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.run(<<~SQL)
      CREATE TABLE t (i INTEGER, b BIGINT, s VARCHAR, tm TIME, j JSON, l STRUCT(a INTEGER)[])
    SQL
    @db.run(<<~SQL)
      INSERT INTO t VALUES
        (1, 10, 'x', '12:34:56', '{"k": 1}', [{'a': 1}, {'a': 2}]),
        (NULL, NULL, NULL, NULL, NULL, NULL)
    SQL
  end

  def teardown
    @db&.disconnect
    super
  end

  def rows
    @db[:t].order(Sequel.asc(:i, nulls: :last)).all
  end

  def test_converter_changes_only_its_column_type
    unconverted = rows
    @db.add_conversion_proc(:integer) { |value| value * 100 }

    converted = rows

    assert_equal 100, converted.first[:i]
    %i[b s tm j l].each do |column|
      assert_equal unconverted.first[column], converted.first[column], "column #{column} changed"
    end
  end

  def test_null_values_never_reach_the_converter
    seen = []
    @db.add_conversion_proc(:integer) do |value|
      seen << value
      value
    end

    assert_nil rows.last[:i]
    assert_equal [ 1 ], seen
  end

  def test_converter_receives_the_whole_nested_value
    seen = []
    @db.add_conversion_proc(:list) do |value|
      seen << value
      :converted
    end

    assert_equal :converted, rows.first[:l]
    assert_equal [ [ { a: 1 }, { a: 2 } ] ], seen
  end

  def test_type_alias_is_looked_up_before_the_type
    @db.add_conversion_proc(:json) { |value| JSON.parse(value) }
    @db.add_conversion_proc(:varchar, &:upcase)

    row = rows.first

    assert_equal({ "k" => 1 }, row[:j])
    assert_equal "X", row[:s]
  end

  def test_aliased_type_without_its_own_converter_uses_the_type_converter
    @db.add_conversion_proc(:varchar) { |value| "<#{value}>" }

    assert_equal '<{"k": 1}>', rows.first[:j]
  end

  def test_callable_argument_and_string_type_name
    @db.add_conversion_proc("TIME", ->(value) { value.strftime("%H:%M:%S") })

    assert_equal "12:34:56", rows.first[:tm]
  end

  def test_later_registration_replaces_earlier_one
    @db.add_conversion_proc(:integer) { |value| value + 1 }
    @db.add_conversion_proc(:integer) { |value| value + 2 }

    assert_equal 3, rows.first[:i]
  end

  def test_converters_belong_to_one_database
    other = create_db
    other.run("CREATE TABLE t (i INTEGER)")
    other.run("INSERT INTO t VALUES (1)")
    @db.add_conversion_proc(:integer) { |value| value * 100 }

    assert_equal 1, other[:t].get(:i)
  ensure
    other&.disconnect
  end
end
