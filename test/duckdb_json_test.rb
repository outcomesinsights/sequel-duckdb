# frozen_string_literal: true

require_relative "spec_helper"

# The opt-in duckdb_json extension writes Sequel.duckdb_json values as JSON text cast
# to JSON and reads JSON columns back as DuckDBJSONHash and DuckDBJSONArray objects.
class DuckDBJSONTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.extension :duckdb_json
    @db.run("CREATE TABLE t (id INTEGER PRIMARY KEY, j JSON, v VARCHAR, ja JSON[])")
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

  def test_literal_is_json_text_cast_to_json
    ds = mock_db.dataset

    assert_equal %q('{"a":1,"b":null}'::JSON), ds.literal(Sequel.duckdb_json("a" => 1, "b" => nil))
    assert_equal %q('[1,"x"]'::JSON), ds.literal(Sequel.duckdb_json([ 1, "x" ]))
    assert_equal %q('{"it''s":[]}'::JSON), ds.literal(Sequel.duckdb_json("it's" => []))
  end

  def test_extension_loads_on_a_mock_database
    db = SequelDuckDBTest.create_mock_db
    db.extension :duckdb_json

    assert_equal %q(INSERT INTO "t" ("j") VALUES ('{"a":1}'::JSON)), db[:t].insert_sql(j: Sequel.duckdb_json("a" => 1))
  end

  def test_constructor_returns_a_wrapper_unchanged
    json = Sequel.duckdb_json([ 1 ])

    assert_same json, Sequel.duckdb_json(json)
  end

  def test_constructor_rejects_a_scalar
    assert_raises(Sequel::Error) { Sequel.duckdb_json(1) }
  end

  def test_insert_reads_back_a_json_hash
    @db[:t].insert(id: 1, j: Sequel.duckdb_json("a" => 1))

    assert_instance_of Sequel::DuckDB::DuckDBJSONHash, value(:j)
    assert_equal({ "a" => 1 }, value(:j))
  end

  def test_json_array_round_trips
    @db[:t].insert(id: 1, j: Sequel.duckdb_json([ 1, { "b" => [ 2 ] }, nil ]))

    assert_instance_of Sequel::DuckDB::DuckDBJSONArray, value(:j)
    assert_equal [ 1, { "b" => [ 2 ] }, nil ], value(:j)
  end

  def test_nested_wrappers_are_written_as_json
    @db[:t].insert(id: 1, j: Sequel.duckdb_json("a" => Sequel.duckdb_json([ 1 ])))

    assert_equal({ "a" => [ 1 ] }, value(:j))
  end

  def test_json_scalars_read_back_as_plain_values
    @db.run(%q(INSERT INTO t (id, j) VALUES (1, '5'), (2, '"x"'), (3, 'true'), (4, 'null')))

    assert_equal [ 5, "x", true, nil ], @db[:t].order(:id).select_map(:j)
  end

  def test_varchar_holding_json_text_is_not_parsed
    @db.run(%q(INSERT INTO t (id, v) VALUES (1, '{"a":1}')))

    assert_equal '{"a":1}', value(:v)
  end

  def test_json_expression_result_is_parsed
    assert_equal({ "b" => 2 }, @db.get(Sequel.function(:json_extract, Sequel.duckdb_json("a" => { "b" => 2 }), "$.a")))
  end

  def test_null_stays_nil
    @db[:t].insert(id: 1)

    assert_nil value(:j)
  end

  def test_schema_reports_json
    types = @db.schema(:t).to_h { |name, info| [ name, info[:type] ] }

    assert_equal :json, types[:j]
    assert_equal :string, types[:v]
    assert_equal :string, types[:ja]
  end

  def test_schema_reports_a_json_list_with_duckdb_array
    @db.extension :duckdb_array

    assert_equal :json_array, @db.schema(:t).to_h { |name, info| [ name, info[:type] ] }[:ja]
  end

  def test_model_saves_a_json_list_of_hashes_with_duckdb_array
    @db.extension :duckdb_array
    # Sequel::Model(@db[:t]) is cached with the schema read before duckdb_array.
    db = @db
    model = Class.new(Sequel::Model) do
      set_dataset(db[:t])
      unrestrict_primary_key
    end
    model.create(id: 1, ja: [ { "a" => 1 }, Sequel.duckdb_json([ 2 ]) ])

    assert_equal [ '{"a":1}', "[2]" ], value(:ja)
  end

  def test_model_saves_an_assigned_hash
    @model.create(id: 1, j: { "a" => 1 })

    assert_equal({ "a" => 1 }, value(:j))
  end

  def test_model_saves_an_assigned_array
    @model.create(id: 1, j: [ 1, 2 ])

    assert_equal [ 1, 2 ], value(:j)
  end

  def test_model_typecasts_a_hash_to_a_json_hash
    row = @model.new(j: { "a" => 1 })

    assert_instance_of Sequel::DuckDB::DuckDBJSONHash, row.j
  end

  def test_model_parses_a_string
    row = @model.new(j: '{"a":[1]}')

    assert_instance_of Sequel::DuckDB::DuckDBJSONHash, row.j
    assert_equal({ "a" => [ 1 ] }, row.j)
  end

  def test_typecast_rejects_invalid_json_text
    assert_raises(Sequel::InvalidValue) { @db.typecast_value(:json, "{not json") }
  end

  def test_typecast_rejects_a_non_json_value
    assert_raises(Sequel::InvalidValue) { @db.typecast_value(:json, Object.new) }
  end

  def test_assigning_the_loaded_value_changes_nothing
    @model.create(id: 1, j: { "a" => 1 })
    row = @model[1]
    row.j = { "a" => 1 }

    assert_empty row.changed_columns
  end

  def test_auto_validations_accept_the_value
    @model.plugin :auto_validations

    assert_predicate @model.new(id: 1, j: { "a" => 1 }), :valid?
  end

  def test_auto_validations_reject_invalid_json_text
    @model.plugin :auto_validations

    refute_predicate @model.new(id: 1, j: "{not json"), :valid?
  end

  def test_without_the_extension_values_and_schema_do_not_change
    db = create_db
    db.run("CREATE TABLE t (j JSON)")
    db.run(%q(INSERT INTO t VALUES ('{"a":1}')))

    assert_equal '{"a":1}', db[:t].get(:j)
    assert_equal :string, db.schema(:t).first.last[:type]
  ensure
    db&.disconnect
  end
end
