# frozen_string_literal: true

require_relative "spec_helper"
require "open3"
require "rbconfig"
require "tmpdir"
require "fileutils"

# The opt-in duckdb_ops extension. Loading it here is global for the whole test
# process, so the "not loaded" checks run in a subprocess.
Sequel.extension :duckdb_ops

class DuckDBOpsLoadTest < SequelDuckDBTest::TestCase
  LIB = File.expand_path("../lib", __dir__)

  # Run ruby code in a fresh process with only sequel and the adapter loaded.
  def run_isolated(code)
    out, status = Open3.capture2e(RbConfig.ruby, "-I", LIB, "-e", <<~RUBY)
      require "sequel"
      require "sequel/adapters/duckdb"
      #{code}
    RUBY

    assert_predicate status, :success?, out
    out
  end

  def test_sequel_extension_adds_try_cast_only_when_loaded
    out = run_isolated(<<~RUBY)
      puts Sequel.respond_to?(:try_cast)
      Sequel.extension :duckdb_ops
      puts Sequel.respond_to?(:try_cast)
      puts Sequel.mock(host: "duckdb").literal(Sequel.try_cast(:x, :DOUBLE))
    RUBY

    assert_equal [ "false", "true", "TRY_CAST(\"x\" AS DOUBLE)" ], out.lines.map(&:chomp)
  end

  def test_database_extension_loads_it
    out = run_isolated(<<~RUBY)
      puts Sequel.respond_to?(:try_cast)
      db = Sequel.mock(host: "duckdb")
      db.extension :duckdb_ops
      puts Sequel.respond_to?(:try_cast)
      puts db.literal(Sequel.try_cast(:x, :DOUBLE))
    RUBY

    assert_equal [ "false", "true", "TRY_CAST(\"x\" AS DOUBLE)" ], out.lines.map(&:chomp)
  end

  def test_sequel_extension_adds_duckdb_lambda_only_when_loaded
    out = run_isolated(<<~RUBY)
      puts Sequel.respond_to?(:duckdb_lambda)
      Sequel.extension :duckdb_ops
      puts Sequel.respond_to?(:duckdb_lambda)
      puts Sequel.mock(host: "duckdb").literal(Sequel.duckdb_lambda(:x) { |x| x + 1 })
    RUBY

    assert_equal [ "false", "true", "lambda \"x\": (\"x\" + 1)" ], out.lines.map(&:chomp)
  end
end

class DuckDBOpsTryCastTest < SequelDuckDBTest::TestCase
  def test_try_cast_with_a_symbol_type
    assert_equal "TRY_CAST(\"x\" AS DOUBLE)", mock_db.literal(Sequel.try_cast(:x, :DOUBLE))
  end

  def test_try_cast_with_a_string_type_passes_through
    assert_equal "TRY_CAST(\"x\" AS DECIMAL(10,2))", mock_db.literal(Sequel.try_cast(:x, "DECIMAL(10,2)"))
  end

  def test_try_cast_with_a_ruby_class_uses_the_adapter_cast_type_literal
    assert_equal "TRY_CAST(\"x\" AS double precision)", mock_db.literal(Sequel.try_cast(:x, Float))
    assert_equal "TRY_CAST(\"x\" AS integer)", mock_db.literal(Sequel.try_cast(:x, Integer))
  end

  def test_try_cast_matches_cast_apart_from_the_keyword
    cast = mock_db.literal(Sequel.cast(:x, Float))

    assert_equal "TRY_#{cast}", mock_db.literal(Sequel.try_cast(:x, Float))
  end

  def test_try_cast_on_real_duckdb_returns_nil_on_failure
    db = SequelDuckDBTest.create_test_db

    assert_nil db.get(Sequel.try_cast("abc", :DOUBLE))
    assert_in_delta 1.5, db.get(Sequel.try_cast("1.5", :DOUBLE))
    assert_in_delta 1.5, db.get(Sequel.try_cast("1.5", Float))
  ensure
    db&.disconnect
  end
end

class DuckDBOpsNamedArgumentsTest < SequelDuckDBTest::TestCase
  def named_sql(**args)
    mock_db.literal(Sequel.function(:f, :x, Sequel.duckdb_named(**args)))
  end

  def test_scalar_values_use_the_dataset_literal
    assert_equal "f(\"x\", a = TRUE, b = FALSE, c = 3, d = 1.5, e = 'text', g = NULL)",
                 named_sql(a: true, b: false, c: 3, d: 1.5, e: "text", g: nil)
  end

  def test_array_value_is_a_list_literal
    assert_equal "f(\"x\", a = ['p', 1, TRUE])", named_sql(a: [ "p", 1, true ])
  end

  def test_hash_value_is_a_struct_literal
    assert_equal "f(\"x\", hive_types = {'year': BIGINT, 'o''k': 'INTEGER', 'n': 2})",
                 named_sql(hive_types: { year: :BIGINT, "o'k" => "INTEGER", n: 2 })
  end

  def test_names_keep_the_given_order
    assert_equal "f(\"x\", z = 1, a = 2, m = 3)", named_sql(z: 1, a: 2, m: 3)
  end

  def test_invalid_names_raise
    [ :Bad, :"a-b", :"1a", :"a b", :"a;drop" ].each do |name|
      assert_raises(ArgumentError, name.inspect) { Sequel.duckdb_named(name => 1) }
    end
  end

  def test_no_names_raise
    assert_raises(ArgumentError) { Sequel.duckdb_named }
  end

  def test_read_helpers_emit_a_path_list_without_a_trailing_comma
    assert_equal "read_parquet(['/d/*.parquet'])", mock_db.literal(Sequel.duckdb_read_parquet("/d/*.parquet"))
    assert_equal "read_csv(['/a.csv', '/b.csv'])", mock_db.literal(Sequel.duckdb_read_csv([ "/a.csv", "/b.csv" ]))
    assert_equal "read_json(['/a.json'])", mock_db.literal(Sequel.duckdb_read_json("/a.json"))
  end

  def test_read_helpers_with_named_parameters
    assert_equal "read_parquet(['/d/**/*.parquet'], hive_partitioning = TRUE, union_by_name = TRUE)",
                 mock_db.literal(Sequel.duckdb_read_parquet("/d/**/*.parquet", hive_partitioning: true, union_by_name: true))
    assert_equal "read_csv(['/a.csv'], header = TRUE, delim = '|')",
                 mock_db.literal(Sequel.duckdb_read_csv("/a.csv", header: true, delim: "|"))
    assert_equal "read_json(['/a.json'], columns = {'a': 'INTEGER'})",
                 mock_db.literal(Sequel.duckdb_read_json("/a.json", columns: { a: "INTEGER" }))
  end

  def test_apostrophe_in_a_path_is_doubled
    assert_equal "read_parquet(['/d/o''neil/*.parquet'])", mock_db.literal(Sequel.duckdb_read_parquet("/d/o'neil/*.parquet"))
  end

  def test_read_helpers_reject_non_string_paths
    assert_raises(ArgumentError) { Sequel.duckdb_read_parquet(:t) }
    assert_raises(ArgumentError) { Sequel.duckdb_read_parquet([]) }
  end

  def test_read_helper_can_be_aliased_in_from
    ds = mock_db.from(Sequel.duckdb_read_parquet("/d/**/*.parquet", hive_partitioning: true).as(:t))

    assert_equal "SELECT * FROM read_parquet(['/d/**/*.parquet'], hive_partitioning = TRUE) AS \"t\"", ds.sql
  end
end

class DuckDBOpsReadFunctionsRealTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = SequelDuckDBTest.create_test_db
    @dir = Dir.mktmpdir
  end

  def teardown
    @db.disconnect
    FileUtils.remove_entry(@dir)
    super
  end

  def column_types(ds)
    @db.fetch("DESCRIBE #{ds.sql}").to_h { |r| [ r[:column_name], r[:column_type] ] }
  end

  def test_read_parquet_with_hive_partitioning_and_hive_types
    @db.run("CREATE TABLE t AS SELECT * FROM (VALUES (1, 2020), (2, 2021), (3, 2021)) v(id, year)")
    out = File.join(@dir, "parts")
    @db.copy_to(@db[:t], out, format: :parquet, partition_by: :year)
    glob = File.join(out, "*", "*.parquet")

    inferred = @db.from(Sequel.duckdb_read_parquet(glob, hive_partitioning: true).as(:p))
    typed = @db.from(Sequel.duckdb_read_parquet(glob, hive_partitioning: true, hive_types: { year: :VARCHAR }).as(:p))

    assert_equal "BIGINT", column_types(inferred)["year"]
    assert_equal "VARCHAR", column_types(typed)["year"]
    assert_equal [ [ 1, "2020" ], [ 2, "2021" ], [ 3, "2021" ] ], typed.order(:id).select_map(%i[id year])
  end

  def test_read_parquet_union_by_name_across_two_files
    a = File.join(@dir, "a.parquet")
    b = File.join(@dir, "b.parquet")
    @db.copy_to("SELECT 1 AS id, 'x' AS a", a)
    @db.copy_to("SELECT 2 AS id, 'y' AS b", b)

    ds = @db.from(Sequel.duckdb_read_parquet([ a, b ], union_by_name: true).as(:u)).order(:id)

    assert_equal [ { id: 1, a: "x", b: nil }, { id: 2, a: nil, b: "y" } ], ds.all
  end

  def test_read_json_with_columns_and_an_apostrophe_in_the_path
    path = File.join(@dir, "it's.json")
    File.write(path, %({"a": "1", "b": "x"}\n{"a": "2", "b": "y"}\n))

    ds = @db.from(Sequel.duckdb_read_json(path, columns: { a: "INTEGER", b: "VARCHAR" }).as(:j)).order(:a)

    assert_equal({ "a" => "INTEGER", "b" => "VARCHAR" }, column_types(ds))
    assert_equal [ { a: 1, b: "x" }, { a: 2, b: "y" } ], ds.all
  end

  def test_read_csv_with_named_parameters
    path = File.join(@dir, "a.csv")
    File.write(path, "x|y\n1|p\n")

    ds = @db.from(Sequel.duckdb_read_csv(path, header: true, delim: "|").as(:c))

    assert_equal [ { x: 1, y: "p" } ], ds.all
  end
end

class DuckDBOpsLambdaTest < SequelDuckDBTest::TestCase
  def sql(expr)
    mock_db.literal(expr)
  end

  def test_block_form_with_one_parameter
    assert_equal "lambda \"x\": (\"x\" + 1)", sql(Sequel.duckdb_lambda(:x) { |x| x + 1 })
  end

  def test_block_form_with_two_parameters
    assert_equal "lambda \"acc\", \"x\": (\"acc\" + \"x\")", sql(Sequel.duckdb_lambda(:acc, :x) { |acc, x| acc + x })
  end

  def test_block_receives_one_identifier_per_parameter_in_order
    seen = nil
    Sequel.duckdb_lambda(:acc, "x") { |*ids| seen = ids }

    assert_equal [ Sequel::SQL::Identifier ] * 2, seen.map(&:class)
    assert_equal [ :acc, "x" ], seen.map(&:value)
  end

  def test_body_form_with_an_array_of_names
    assert_equal "lambda \"acc\", \"x\": (\"acc\" + \"x\")", sql(Sequel.duckdb_lambda([ :acc, :x ], Sequel[:acc] + :x))
  end

  def test_body_form_with_a_single_symbol
    assert_equal "lambda \"x\": (\"x\" + 1)", sql(Sequel.duckdb_lambda(:x, Sequel[:x] + 1))
  end

  def test_body_form_with_a_plain_ruby_value
    assert_equal "lambda \"x\": 'text'", sql(Sequel.duckdb_lambda(:x, "text"))
  end

  def test_both_forms_give_identical_sql
    assert_equal sql(Sequel.duckdb_lambda(:x) { |x| x + 1 }), sql(Sequel.duckdb_lambda(:x, Sequel[:x] + 1))
    assert_equal sql(Sequel.duckdb_lambda(:acc, :x) { |acc, x| (acc * 31) + x }),
                 sql(Sequel.duckdb_lambda([ :acc, :x ], (Sequel[:acc] * 31) + :x))
  end

  def test_parameter_names_use_identifier_quoting
    assert_equal "lambda \"a\"\"b\": 1", sql(Sequel.duckdb_lambda("a\"b", 1))
  end

  def test_no_parameters_raise
    assert_raises(ArgumentError) { Sequel.duckdb_lambda { 1 } }
    assert_raises(ArgumentError) { Sequel.duckdb_lambda([], 1) }
  end

  def test_invalid_parameter_names_raise
    [ 1, nil, Sequel[:x], :"", "" ].each do |name|
      assert_raises(ArgumentError, name.inspect) { Sequel.duckdb_lambda([ name ], 1) }
      assert_raises(ArgumentError, name.inspect) { Sequel.duckdb_lambda(name) { 1 } }
    end
  end

  def test_duplicate_parameter_names_raise
    assert_raises(ArgumentError) { Sequel.duckdb_lambda(:x, :x) { |x, _| x } }
    assert_raises(ArgumentError) { Sequel.duckdb_lambda([ :x, "x" ], 1) }
  end

  def test_block_with_a_body_argument_raises
    [ -> { Sequel.duckdb_lambda(:x, Sequel[:x] + 1) { |x| x } },
      -> { Sequel.duckdb_lambda([ :x ], 1) { |x| x } } ].each do |call|
      assert_match(/not a body argument/, assert_raises(ArgumentError, &call).message)
    end
  end

  def test_body_form_needs_exactly_two_arguments
    assert_raises(ArgumentError) { Sequel.duckdb_lambda(:x) }
    assert_raises(ArgumentError) { Sequel.duckdb_lambda(:acc, :x, Sequel[:acc] + :x) }
  end
end

class DuckDBOpsLambdaRealTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = SequelDuckDBTest.create_test_db
  end

  def teardown
    @db.disconnect
    super
  end

  def list(*values)
    Sequel.function(:list_value, *values)
  end

  def test_list_transform
    assert_equal [ 2, 3, 4 ], @db.get(Sequel.function(:list_transform, list(1, 2, 3), Sequel.duckdb_lambda(:x) { |x| x + 1 }))
  end

  def test_list_reduce_with_an_initial_value
    reduce = Sequel.function(:list_reduce, list(1, 2, 3),
                             Sequel.duckdb_lambda(:acc, :x) { |acc, x| acc + x },
                             Sequel.cast(0, :BIGINT))

    assert_equal 6, @db.get(reduce)
  end

  def test_let_idiom
    body = Sequel.duckdb_lambda(:v) { |v| (v * v) + v }
    let = Sequel.subscript(Sequel.function(:list_transform, list(Sequel.lit("40 + 2")), body), 1)

    assert_equal 1806, @db.get(let)
  end

  def test_struct_field_access_in_the_body
    structs = Sequel.lit("[{'rows': 3}, {'rows': 4}]")
    rows = Sequel.function(:list_transform, structs, Sequel.duckdb_lambda(:p, Sequel.qualify(:p, :rows)))

    assert_equal 7, @db.get(Sequel.function(:list_sum, rows))
  end

  def test_nested_lambda_refers_to_the_outer_parameter
    outer = Sequel.duckdb_lambda(:x) do |x|
      inner = Sequel.duckdb_lambda(:y) { |y| x + y }
      Sequel.function(:list_sum, Sequel.function(:list_transform, list(10, 20), inner))
    end

    assert_equal [ 32, 34 ], @db.get(Sequel.function(:list_transform, list(1, 2), outer))
  end
end
