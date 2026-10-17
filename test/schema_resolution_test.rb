# frozen_string_literal: true

require_relative "spec_helper"

# schema and indexes must read the table that a query with the same name reads: each
# Sequel identifier form, the search_path, temporary tables, attached catalogs, and the
# first match when several schemas hold a table with that name.
class SchemaResolutionTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.run("CREATE TABLE main.t2 (z INTEGER PRIMARY KEY)")
    @db.run("CREATE SCHEMA s")
    @db.run("CREATE TABLE s.t2 (a INTEGER PRIMARY KEY, b VARCHAR)")
    @db.run("CREATE INDEX t2_b ON s.t2 (b)")
  end

  def teardown
    @db&.disconnect
    super
  end

  def columns(name, opts = {})
    @db.schema(name, opts).map(&:first)
  end

  def test_unqualified_name_without_search_path_is_main
    assert_equal [ :z ], columns(:t2)
    assert_empty @db.indexes(:t2)
  end

  def test_unqualified_name_follows_search_path
    @db.run("SET search_path = 's'")

    assert_equal %i[a b], columns(:t2)
    assert_equal [ :a ], @db.schema(:t2).select { |_, info| info[:primary_key] }.map(&:first)
    assert_equal [ :t2_b ], @db.indexes(:t2).keys
  end

  def test_identifier_forms
    assert_equal [ :z ], columns(Sequel.identifier("t2"))
    assert_equal [ :z ], columns("t2")
    assert_equal %i[a b], columns(Sequel[:s][:t2])
    assert_equal %i[a b], columns(Sequel.identifier("t2").qualify(:s))
    assert_equal %i[a b], columns(:t2, schema: "s")
    assert_equal %i[a b], columns(Sequel.identifier("t2"), schema: :s)
    assert_equal [ :t2_b ], @db.indexes(Sequel[:s][:t2]).keys
    assert_equal [ :t2_b ], @db.indexes(:t2, schema: "s").keys
  end

  def test_name_case_does_not_matter
    assert_equal [ :z ], columns(:T2)
  end

  def test_temporary_table_comes_first
    @db.run("CREATE TEMP TABLE t2 (tmp INTEGER)")

    assert_equal [ :tmp ], columns(:t2)
    assert_equal [ :tmp ], columns(Sequel[:main][:t2]), "DuckDB reads temp.main.t2 for main.t2"
    assert_equal [ :z ], columns(Sequel[:memory][:main][:t2])
  end

  def test_view_that_selects_from_a_table_with_the_same_name
    @db.run("CREATE SCHEMA v")
    @db.run("CREATE VIEW v.t2 AS SELECT * FROM main.t2")

    assert @db.schema(:t2).to_h[:z][:primary_key], "without search_path, t2 is the table main.t2"

    @db.run("SET search_path = 'v'")

    refute @db.schema(:t2).to_h[:z][:primary_key], "with search_path v, t2 is the view v.t2"
  end

  def test_attached_catalog
    @db.run("ATTACH ':memory:' AS other")
    @db.run("CREATE TABLE other.main.t2 (o INTEGER PRIMARY KEY, p INTEGER)")
    @db.run("CREATE INDEX t2_p ON other.main.t2 (p)")

    assert_equal %i[o p], columns(Sequel[:other][:main][:t2])
    assert_equal [ :o ], @db.schema(Sequel[:other][:main][:t2]).select { |_, info| info[:primary_key] }.map(&:first)
    assert_equal [ :t2_p ], @db.indexes(Sequel[:other][:main][:t2]).keys
    assert_equal [ :z ], columns(:t2)
  end

  def test_missing_table_raises
    assert_raises(Sequel::DatabaseError) { @db.schema(:missing) }
    assert_raises(Sequel::DatabaseError) { @db.indexes(:missing) }
    assert_raises(Sequel::DatabaseError) { @db.schema(Sequel[:s][:missing]) }
  end

  def test_missing_table_inside_a_transaction_keeps_the_transaction
    @db.transaction do
      @db[:t2].insert(z: 1)
      assert_raises(Sequel::DatabaseError) { @db.schema(:missing) }
      @db[:t2].insert(z: 2)
    end

    assert_equal [ 1, 2 ], @db[:t2].order(:z).select_map(:z)
  end
end
