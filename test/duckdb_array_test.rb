# frozen_string_literal: true

require_relative "spec_helper"

# The opt-in duckdb_array extension writes Sequel.duckdb_array values as DuckDB list
# literals and reads LIST and ARRAY columns back as DuckDBArray objects.
class DuckDBArrayTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.extension :duckdb_array
    @db.run("CREATE TABLE t (id INTEGER PRIMARY KEY, l INTEGER[], ll INTEGER[][], a INTEGER[3], b BIGINT[], ls STRUCT(a INTEGER)[])")
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

  def test_literal_is_a_duckdb_list
    ds = mock_db.dataset

    assert_equal "[1, 'x', NULL]", ds.literal(Sequel.duckdb_array([ 1, "x", nil ]))
    assert_equal "[[1], []]", ds.literal(Sequel.duckdb_array([ [ 1 ], [] ]))
    assert_equal "[]", ds.literal(Sequel.duckdb_array([]))
  end

  def test_extension_loads_on_a_mock_database
    db = SequelDuckDBTest.create_mock_db
    db.extension :duckdb_array

    assert_equal "INSERT INTO \"t\" (\"l\") VALUES ([1, 2])", db[:t].insert_sql(l: Sequel.duckdb_array([ 1, 2 ]))
  end

  def test_insert_reads_back_a_duckdb_array
    @db[:t].insert(id: 1, l: Sequel.duckdb_array([ 1, 2 ]))

    assert_instance_of Sequel::DuckDB::DuckDBArray, value(:l)
    assert_equal [ 1, 2 ], value(:l)
  end

  def test_nested_and_empty_arrays_round_trip
    @db[:t].insert(id: 1, l: Sequel.duckdb_array([]), ll: Sequel.duckdb_array([ [ 1 ], [], [ 2, 3 ] ]))

    assert_equal [], value(:l)
    assert_instance_of Sequel::DuckDB::DuckDBArray, value(:l)
    assert_equal [ [ 1 ], [], [ 2, 3 ] ], value(:ll)
  end

  def test_fixed_size_array_round_trips
    @db[:t].insert(id: 1, a: Sequel.duckdb_array([ 1, 2, 3 ]))

    assert_instance_of Sequel::DuckDB::DuckDBArray, value(:a)
    assert_equal [ 1, 2, 3 ], value(:a)
  end

  def test_null_stays_nil
    @db[:t].insert(id: 1)

    assert_nil value(:l)
  end

  def test_schema_reports_element_array_types
    types = @db.schema(:t).to_h { |name, info| [ name, info[:type] ] }

    assert_equal :integer_array, types[:l]
    assert_equal :integer_array, types[:ll]
    assert_equal :integer_array, types[:a]
    assert_equal :bigint_array, types[:b]
    assert_equal :array, types[:ls]
  end

  def test_model_saves_an_assigned_array
    @model.create(id: 1, l: [ 1, 2 ], b: [ "3" ])

    assert_equal [ 1, 2 ], value(:l)
    assert_equal [ 3 ], value(:b)
  end

  def test_model_typecasts_each_element
    row = @model.new(l: [ "1", nil ], ll: [ [ "2" ] ])

    assert_instance_of Sequel::DuckDB::DuckDBArray, row.l
    assert_equal [ 1, nil ], row.l
    assert_equal [ [ 2 ] ], row.ll
  end

  def test_typecast_rejects_a_non_array
    assert_raises(Sequel::InvalidValue) { @db.typecast_value(:integer_array, 1) }
  end

  def test_assigning_the_loaded_value_changes_nothing
    @model.create(id: 1, l: [ 1, 2 ])
    row = @model[1]
    row.l = [ 1, 2 ]

    assert_empty row.changed_columns
  end

  def test_auto_validations_accept_the_value
    @model.plugin :auto_validations

    assert_predicate @model.new(id: 1, l: [ 1, 2 ]), :valid?
  end

  def test_auto_validations_reject_a_non_array
    @model.plugin :auto_validations

    refute_predicate @model.new(id: 1, l: 1), :valid?
  end

  # Without the duckdb_struct extension, a STRUCT element has no Sequel type, so the
  # model leaves the elements as they are and does not typecast them to String.
  def test_list_of_structs_keeps_its_elements
    struct = Sequel.lit("{'a': 1}")
    row = @model.new(id: 1, ls: [ struct ])

    assert_same struct, row.ls.first
    row.save_changes
    assert_equal [ { a: 1 } ], value(:ls)
  end

  def test_without_the_extension_values_and_schema_do_not_change
    db = create_db
    db.run("CREATE TABLE t (l INTEGER[])")
    db.run("INSERT INTO t VALUES ([1, 2])")

    assert_instance_of Array, db[:t].get(:l)
    assert_equal :string, db.schema(:t).first.last[:type]
  ensure
    db&.disconnect
  end
end
