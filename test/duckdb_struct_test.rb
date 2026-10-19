# frozen_string_literal: true

require_relative "spec_helper"

# The opt-in duckdb_struct extension writes Sequel.duckdb_struct values as DuckDB
# struct literals and reads STRUCT columns back as DuckDBStruct objects.
class DuckDBStructTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.extension :duckdb_struct
    @db.run(<<~SQL)
      CREATE TABLE t (id INTEGER PRIMARY KEY, s STRUCT(a INTEGER, b VARCHAR), q STRUCT("it's" INTEGER),
                      n STRUCT(sub STRUCT(c INTEGER), l INTEGER[]), ls STRUCT(a INTEGER)[])
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

  def test_literal_is_a_duckdb_struct
    ds = mock_db.dataset

    assert_equal "{'a': 1, 'b': 'x', 'c': NULL}", ds.literal(Sequel.duckdb_struct(a: 1, b: "x", c: nil))
    assert_equal "{'it''s': 1}", ds.literal(Sequel.duckdb_struct("it's" => 1))
  end

  def test_extension_loads_on_a_mock_database
    db = SequelDuckDBTest.create_mock_db
    db.extension :duckdb_struct

    assert_equal "INSERT INTO \"t\" (\"s\") VALUES ({'a': 1})", db[:t].insert_sql(s: Sequel.duckdb_struct(a: 1))
  end

  def test_keys_become_symbols
    assert_equal({ a: 1 }, Sequel.duckdb_struct("a" => 1).to_hash)
  end

  def test_insert_reads_back_a_duckdb_struct
    @db[:t].insert(id: 1, s: Sequel.duckdb_struct(a: 1, b: "x"))

    assert_instance_of Sequel::DuckDB::DuckDBStruct, value(:s)
    assert_equal({ a: 1, b: "x" }, value(:s))
  end

  def test_key_with_a_single_quote_round_trips
    @db[:t].insert(id: 1, q: Sequel.duckdb_struct("it's" => 1))

    assert_equal({ "it's": 1 }, value(:q))
  end

  def test_nested_wrappers_are_written
    @db.extension :duckdb_array
    @db[:t].insert(id: 1, n: Sequel.duckdb_struct(sub: Sequel.duckdb_struct(c: 2), l: Sequel.duckdb_array([ 1, 2 ])))

    assert_equal({ sub: { c: 2 }, l: [ 1, 2 ] }, value(:n))
  end

  def test_null_stays_nil
    @db[:t].insert(id: 1)

    assert_nil value(:s)
  end

  def test_schema_reports_struct
    types = @db.schema(:t).to_h { |name, info| [ name, info[:type] ] }

    assert_equal :struct, types[:s]
    assert_equal :struct, types[:n]
    assert_equal :string, types[:ls]
  end

  def test_schema_reports_a_list_of_structs_with_duckdb_array
    @db.extension :duckdb_array

    assert_equal :struct_array, @db.schema(:t).to_h { |name, info| [ name, info[:type] ] }[:ls]
  end

  def test_model_saves_an_assigned_hash
    @model.create(id: 1, s: { a: 1, b: "x" })

    assert_equal({ a: 1, b: "x" }, value(:s))
  end

  def test_model_typecasts_a_hash_to_a_struct
    row = @model.new(s: { "a" => 1 })

    assert_instance_of Sequel::DuckDB::DuckDBStruct, row.s
    assert_equal({ a: 1 }, row.s)
  end

  def test_typecast_rejects_a_non_hash
    assert_raises(Sequel::InvalidValue) { @db.typecast_value(:struct, 1) }
  end

  def test_assigning_the_loaded_value_changes_nothing
    @model.create(id: 1, s: { a: 1, b: "x" })
    row = @model[1]
    row.s = { a: 1, b: "x" }

    assert_empty row.changed_columns
  end

  def test_auto_validations_accept_the_value
    @model.plugin :auto_validations

    assert_predicate @model.new(id: 1, s: { a: 1, b: "x" }), :valid?
  end

  def test_auto_validations_reject_a_non_hash
    @model.plugin :auto_validations

    refute_predicate @model.new(id: 1, s: 1), :valid?
  end

  def test_without_the_extension_values_and_schema_do_not_change
    db = create_db
    db.run("CREATE TABLE t (s STRUCT(a INTEGER))")
    db.run("INSERT INTO t VALUES ({'a': 1})")

    assert_instance_of Hash, db[:t].get(:s)
    assert_equal :string, db.schema(:t).first.last[:type]
  ensure
    db&.disconnect
  end
end
