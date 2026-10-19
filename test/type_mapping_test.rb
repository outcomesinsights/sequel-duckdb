# frozen_string_literal: true

require_relative "spec_helper"

# db.schema's :type comes from the type name that information_schema.columns reports,
# which is not always the name the column was declared with (REAL is reported as FLOAT).
# Each column here is declared in DuckDB, so the test checks the reported names.
class TypeMappingTest < SequelDuckDBTest::TestCase
  EXPECTED = {
    "BOOLEAN" => :boolean,
    "TINYINT" => :integer, "SMALLINT" => :integer, "INTEGER" => :integer,
    "UTINYINT" => :integer, "USMALLINT" => :integer,
    "BIGINT" => :bigint, "UINTEGER" => :bigint, "UBIGINT" => :bigint,
    "HUGEINT" => :bigint, "UHUGEINT" => :bigint, "VARINT" => :bigint,
    "FLOAT" => :float, "REAL" => :float, "DOUBLE" => :float,
    "DECIMAL(10,2)" => :decimal, "NUMERIC" => :decimal,
    "DATE" => :date,
    "TIME" => :time, "TIMETZ" => :time_timezone, "TIME_NS" => :time,
    "TIMESTAMP" => :datetime, "TIMESTAMPTZ" => :datetime, "TIMESTAMP_S" => :datetime,
    "TIMESTAMP_MS" => :datetime, "TIMESTAMP_NS" => :datetime,
    "INTERVAL" => :interval,
    "VARCHAR" => :string, "BLOB" => :blob, "UUID" => :uuid
  }.freeze

  def setup
    super
    @db = create_db
    columns = EXPECTED.keys.each_with_index.map { |type, i| "c#{i} #{type}" }
    @db.run("CREATE TABLE types (#{columns.join(", ")})")
  end

  def teardown
    @db&.disconnect
    super
  end

  def test_schema_type_of_each_duckdb_type
    actual = @db.schema(:types).map { |_, info| [ info[:db_type], info[:type] ] }

    assert_equal EXPECTED.values, actual.map(&:last),
                 EXPECTED.keys.zip(actual).map { |declared, (reported, type)| "#{declared} (#{reported}) => #{type.inspect}" }.join("\n")
  end

  def test_model_typecasts_a_float_column_to_float
    @db.create_table(:measures) do
      primary_key :id
      column :amount, "FLOAT"
    end
    model = Class.new(Sequel::Model(@db[:measures]))

    assert_equal 1.5, model.new(amount: "1.5").amount
  end
end
