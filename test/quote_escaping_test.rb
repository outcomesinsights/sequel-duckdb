# frozen_string_literal: true

require_relative "spec_helper"
require "pathname"
require "tmpdir"

# Schema, table and catalog names go into information_schema and duckdb_indexes() queries
# as string literals, and set_pragma puts its value into a string literal. A name or value
# with a single quote in it must be escaped there, or the SQL breaks.
class QuoteEscapingTest < SequelDuckDBTest::TestCase
  SCHEMA = "it's"
  TABLE = :"o'brien"
  CATALOG = "cat's"

  def setup
    super
    @db = create_db
    @db.run(%(CREATE SCHEMA "#{SCHEMA}"))
    @db.run(%(ATTACH ':memory:' AS "#{CATALOG}"))
    [ "main", SCHEMA ].each do |schema|
      @db.run(%(CREATE TABLE "#{schema}"."#{TABLE}" (id INTEGER PRIMARY KEY, name VARCHAR)))
      @db.run(%(CREATE INDEX "#{TABLE}_name" ON "#{schema}"."#{TABLE}" (name)))
      @db.run(%(CREATE VIEW "#{schema}"."#{TABLE}_v" AS SELECT * FROM "#{schema}"."#{TABLE}"))
    end
    @db.run(%(CREATE TABLE "#{CATALOG}".main."#{TABLE}" (id INTEGER)))
  end

  def teardown
    @db&.disconnect
    super
  end

  def test_table_exists
    assert @db.table_exists?(TABLE)
    assert @db.table_exists?(TABLE, schema: SCHEMA)
    assert @db.table_exists?(Sequel[SCHEMA.to_sym][TABLE])
  end

  def test_tables
    assert_includes @db.tables, TABLE
    assert_includes @db.tables(schema: SCHEMA), TABLE
    assert_includes @db.tables(schema: Sequel[CATALOG.to_sym][:main]), TABLE
  end

  def test_views
    assert_includes @db.views, :"o'brien_v"
    assert_includes @db.views(schema: SCHEMA), :"o'brien_v"
  end

  def test_schema_with_primary_key
    columns = @db.schema(TABLE, reload: true).to_h

    assert_equal %i[id name], columns.keys
    assert columns[:id][:primary_key]
  end

  def test_schema_in_quoted_schema
    columns = @db.send(:schema_parse_table, TABLE, schema: SCHEMA).to_h

    assert_equal %i[id name], columns.keys
    assert columns[:id][:primary_key]
  end

  def test_indexes
    assert_includes @db.indexes(TABLE).keys, :"o'brien_name"
    assert_includes @db.send(:schema_parse_indexes, TABLE, schema: SCHEMA).keys, :"o'brien_name"
  end

  def test_schema_exists
    assert @db.schema_exists?(SCHEMA)
  end

  def test_schemas_of_a_quoted_catalog
    assert_includes @db.schemas(catalog: CATALOG), :main
  end

  def test_set_pragma_with_a_non_string_value_that_holds_a_quote
    Dir.mktmpdir do |dir|
      path = Pathname.new(dir).join("o'brien")
      @db.set_pragma(:temp_directory, path)

      assert_equal path.to_s, @db.get(Sequel.function(:current_setting, "temp_directory"))
    end
  end
end
