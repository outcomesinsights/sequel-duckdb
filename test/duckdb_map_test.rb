# frozen_string_literal: true

require_relative "spec_helper"

# The opt-in duckdb_map extension writes Sequel.duckdb_map values as DuckDB map
# literals and reads MAP columns back as DuckDBMap objects.
class DuckDBMapTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.extension :duckdb_map
    @db.run(<<~SQL)
      CREATE TABLE t (id INTEGER PRIMARY KEY, m MAP(VARCHAR, INTEGER), mi MAP(INTEGER, VARCHAR),
                      ml MAP(VARCHAR, INTEGER[]), lm MAP(VARCHAR, INTEGER)[])
    SQL
    @model = Class.new(Sequel::Model(@db[:t])) do
      unrestrict_primary_key
    end
  end

  def teardown
    @db&.disconnect
    super
  end

  def value(column, id = 1)
    @db[:t].where(id: id).get(column)
  end

  def test_literal_is_a_duckdb_map
    ds = mock_db.dataset

    assert_equal "MAP {'k': 1, 'it''s': NULL}", ds.literal(Sequel.duckdb_map("k" => 1, "it's" => nil))
    assert_equal "MAP {1: 'x'}", ds.literal(Sequel.duckdb_map(1 => "x"))
    assert_equal "MAP {}", ds.literal(Sequel.duckdb_map({}))
  end

  def test_extension_loads_on_a_mock_database
    db = SequelDuckDBTest.create_mock_db
    db.extension :duckdb_map

    assert_equal "INSERT INTO \"t\" (\"m\") VALUES (MAP {'k': 1})", db[:t].insert_sql(m: Sequel.duckdb_map("k" => 1))
  end

  def test_insert_reads_back_a_duckdb_map
    @db[:t].insert(id: 1, m: Sequel.duckdb_map("k" => 1))

    assert_instance_of Sequel::DuckDB::DuckDBMap, value(:m)
    assert_equal({ "k" => 1 }, value(:m))
  end

  def test_integer_keyed_map_round_trips
    @db[:t].insert(id: 1, mi: Sequel.duckdb_map(1 => "x", 2 => "y"))

    assert_equal({ 1 => "x", 2 => "y" }, value(:mi))
  end

  def test_empty_map_round_trips
    @db[:t].insert(id: 1, m: Sequel.duckdb_map({}))

    assert_instance_of Sequel::DuckDB::DuckDBMap, value(:m)
    assert_empty value(:m)
  end

  def test_nested_wrappers_are_written
    @db.extension :duckdb_array
    @db[:t].insert(id: 1, ml: Sequel.duckdb_map("k" => Sequel.duckdb_array([ 1, 2 ])))

    assert_equal({ "k" => [ 1, 2 ] }, value(:ml))
  end

  def test_null_stays_nil
    @db[:t].insert(id: 1)

    assert_nil value(:m)
  end

  def test_schema_reports_map
    types = @db.schema(:t).to_h { |name, info| [ name, info[:type] ] }

    assert_equal :map, types[:m]
    assert_equal :map, types[:mi]
    assert_equal :map, types[:ml]
    assert_equal :string, types[:lm]
  end

  def test_schema_reports_a_list_of_maps_with_duckdb_array
    @db.extension :duckdb_array

    assert_equal :map_array, @db.schema(:t).to_h { |name, info| [ name, info[:type] ] }[:lm]
  end

  def test_model_saves_an_assigned_hash
    @model.create(id: 1, m: { "k" => 1 })

    assert_equal({ "k" => 1 }, value(:m))
  end

  def test_model_typecasts_a_hash_to_a_map
    row = @model.new(mi: { 1 => "x" })

    assert_instance_of Sequel::DuckDB::DuckDBMap, row.mi
    assert_equal({ 1 => "x" }, row.mi)
  end

  def test_typecast_rejects_a_non_hash
    assert_raises(Sequel::InvalidValue) { @db.typecast_value(:map, 1) }
  end

  def test_assigning_the_loaded_value_changes_nothing
    @model.create(id: 1, m: { "k" => 1 })
    row = @model[1]
    row.m = { "k" => 1 }

    assert_empty row.changed_columns
  end

  def test_auto_validations_accept_the_value
    @model.plugin :auto_validations

    assert_predicate @model.new(id: 1, m: { "k" => 1 }), :valid?
  end

  def test_auto_validations_reject_a_non_hash
    @model.plugin :auto_validations

    refute_predicate @model.new(id: 1, m: 1), :valid?
  end

  def test_without_the_extension_values_and_schema_do_not_change
    db = create_db
    db.run("CREATE TABLE t (m MAP(VARCHAR, INTEGER))")
    db.run("INSERT INTO t VALUES (MAP {'k': 1})")

    assert_instance_of Hash, db[:t].get(:m)
    assert_equal :string, db.schema(:t).first.last[:type]
  ensure
    db&.disconnect
  end
end
