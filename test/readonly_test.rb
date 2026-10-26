# frozen_string_literal: true

require_relative "spec_helper"
require "tmpdir"

# The readonly option (readonly: true, or ?readonly=true in the URL) opens a DuckDB
# file with access_mode READ_ONLY. The README documented it before the adapter read it,
# so writes went through.
class ReadonlyTest < SequelDuckDBTest::TestCase
  def setup
    super
    @dir = Dir.mktmpdir("sequel-duckdb-readonly")
    @path = File.join(@dir, "data.duckdb")
    Sequel.connect(adapter: :duckdb, database: @path).tap do |db|
      db.create_table(:t) { Integer :a }
      db[:t].insert(1)
      db.disconnect
    end
    @dbs = []
  end

  def teardown
    @dbs.each(&:disconnect)
    FileUtils.rm_rf(@dir)
    super
  end

  def open_db(**opts)
    Sequel.connect(adapter: :duckdb, database: @path, **opts).tap { |db| @dbs << db }
  end

  def test_readonly_reads
    assert_equal [ 1 ], open_db(readonly: true)[:t].select_map(:a)
  end

  def test_readonly_refuses_writes
    db = open_db(readonly: true)

    assert_raises(Sequel::DatabaseError) { db[:t].insert(2) }
    assert_equal "read_only", db.get(Sequel.function(:current_setting, "access_mode"))
  end

  def test_readonly_in_the_url_refuses_writes
    db = Sequel.connect("duckdb://#{@path}?readonly=true").tap { |d| @dbs << d }

    assert_raises(Sequel::DatabaseError) { db[:t].insert(2) }
  end

  def test_readonly_false_allows_writes
    db = Sequel.connect("duckdb://#{@path}?readonly=false").tap { |d| @dbs << d }
    db[:t].insert(2)

    assert_equal 2, db[:t].count
  end

  def test_readonly_with_a_matching_access_mode_is_accepted
    db = open_db(readonly: true, config: { access_mode: "read_only" })

    assert_raises(Sequel::DatabaseError) { db[:t].insert(2) }
  end

  def test_readonly_with_a_conflicting_access_mode_raises
    error = assert_raises(Sequel::DatabaseConnectionError) { open_db(readonly: true, config: { access_mode: "READ_WRITE" }) }
    assert_match(/readonly/, error.message)
  end

  # The settings of the first opener of a file apply to all Sequel::Databases on it in
  # this process, as for any config: (see SharedInstanceTest).
  def test_readonly_after_a_read_write_open_raises
    open_db.get(1)

    assert_raises(Sequel::DatabaseConnectionError) { open_db(readonly: true) }
  end
end
