# frozen_string_literal: true

require_relative "spec_helper"

# table_exists? must find a table by every name that a query can use: each Sequel
# identifier form, the search_path, temporary tables, views and attached catalogs.
class TableExistsTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
    @db.create_table(:t1) { Integer :a }
    @db.run("CREATE SCHEMA s")
    @db.run("CREATE TABLE s.t2 (a INTEGER)")
  end

  def teardown
    @db&.disconnect
    super
  end

  def test_identifier_forms
    assert @db.table_exists?(:t1)
    assert @db.table_exists?("t1")
    assert @db.table_exists?(Sequel.identifier("t1"))
    assert @db.table_exists?(Sequel.qualify(:main, :t1))
    assert @db.table_exists?(Sequel.identifier("t1").qualify(:main))
    assert @db.table_exists?(Sequel[:s][:t2])
  end

  def test_missing_tables
    refute @db.table_exists?(:missing)
    refute @db.table_exists?(Sequel.identifier("missing"))
    refute @db.table_exists?(Sequel[:s][:t1])
    refute @db.table_exists?(Sequel[:nope][:t1])
    refute @db.table_exists?(:t2)
  end

  def test_schema_option
    assert @db.table_exists?(:t2, schema: "s")
    assert @db.table_exists?(Sequel.identifier("t2"), schema: :s)
    refute @db.table_exists?(:t1, schema: "s")
  end

  def test_unqualified_name_follows_search_path
    @db.run("SET search_path = 's'")

    assert @db.table_exists?(:t2)
    assert @db.table_exists?(Sequel.identifier("t2"))
    assert @db.table_exists?(:t1), "DuckDB still resolves main after a SET search_path"
  end

  def test_temporary_table_and_view
    @db.run("CREATE TEMP TABLE tt (a INTEGER)")
    @db.create_view(:v1, @db[:t1])

    assert @db.table_exists?(:tt)
    assert @db.table_exists?(:v1)
  end

  def test_attached_catalog
    @db.run("ATTACH ':memory:' AS other")
    @db.run("CREATE TABLE other.main.t3 (a INTEGER)")

    assert @db.table_exists?(Sequel[:other][:main][:t3])
    refute @db.table_exists?(:t3)
  end

  def test_missing_table_inside_a_transaction_keeps_the_transaction
    @db.transaction do
      @db[:t1].insert(a: 1)
      refute @db.table_exists?(:missing)
      @db[:t1].insert(a: 2)
    end

    assert_equal [ 1, 2 ], @db[:t1].order(:a).select_map(:a)
  end

  # The user's own failed query is a version difference that the adapter keeps:
  # DuckDB 2.x aborts the transaction and DuckDB 1.x does not.
  def test_failed_user_query_aborts_the_transaction_on_duckdb_2
    skip "DuckDB 1.x keeps the transaction" if ::DuckDB::LIBRARY_VERSION.to_i < 2

    @db.transaction(rollback: :always) do
      assert_raises(Sequel::DatabaseError) { @db[:missing].all }
      error = assert_raises(Sequel::DatabaseError) { @db[:t1].insert(a: 1) }
      assert_match(/transaction is aborted/, error.message)
    end
  end

  def test_failed_user_query_keeps_the_transaction_on_duckdb_1
    skip "DuckDB 2.x aborts the transaction" if ::DuckDB::LIBRARY_VERSION.to_i >= 2

    @db.transaction(rollback: :always) do
      assert_raises(Sequel::DatabaseError) { @db[:missing].all }
      @db[:t1].insert(a: 1)
    end
  end

  # On DuckDB 2.x the lookup changes the transaction's invalidation policy while it runs.
  def test_missing_table_inside_a_transaction_restores_the_invalidation_policy
    policy = Sequel.function(:current_setting, "current_transaction_invalidation_policy")
    @db.transaction do
      before = @db.get(policy)
      refute @db.table_exists?(:missing)
      assert_equal before, @db.get(policy)
    end
  end
end
