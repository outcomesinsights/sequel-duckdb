# frozen_string_literal: true

require_relative "spec_helper"
require "open3"
require "rbconfig"

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
