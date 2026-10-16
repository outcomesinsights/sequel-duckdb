# frozen_string_literal: true

require_relative "spec_helper"
require "tmpdir"
require "rbconfig"

# Every Sequel::Database on one DuckDB file in one process shares one ::DuckDB::Database
# (Sequel::DuckDB::InstanceRegistry). Before, each opened its own instance, and writes
# through one did not reliably reach the others (bead sequel-duckdb-zn2).
class SharedInstanceTest < SequelDuckDBTest::TestCase
  Registry = Sequel::DuckDB::InstanceRegistry

  def setup
    super
    @dir = Dir.mktmpdir("sequel-duckdb-shared")
    @path = File.join(@dir, "shared.duckdb")
    @dbs = []
  end

  def teardown
    @dbs.each(&:disconnect)
    FileUtils.rm_rf(@dir)
    super
  end

  def open_db(path = @path, **opts)
    Sequel.connect(adapter: :duckdb, database: path, **opts).tap { |db| @dbs << db }
  end

  def setting(db, name)
    db.get(Sequel.function(:current_setting, name))
  end

  def table?(db, schema)
    db.fetch("SELECT count(*) AS n FROM information_schema.tables WHERE table_schema = ?", schema).single_value == 1
  end

  def duckdb_database(db)
    db.instance_variable_get(:@duckdb_database)
  end

  # Opens the file read-write in another process. That works only when no process
  # holds the file, because DuckDB's file lock is per process.
  def other_process_can_open?
    script = "require 'duckdb'; DuckDB::Database.open(ARGV[0]).close"
    system(RbConfig.ruby, "-e", script, @path, err: File::NULL)
  end

  def test_three_databases_on_one_file_see_each_others_tables
    a = open_db
    a.run("CREATE SCHEMA s1")
    a.run("CREATE TABLE s1.t (x INTEGER)")
    a.disconnect
    b = open_db
    b.run("CREATE SCHEMA s2")
    b.run("CREATE TABLE s2.t (x INTEGER)")
    c = open_db

    assert table?(b, "s1")
    assert table?(c, "s2")
    assert table?(a, "s2")
  end

  def test_two_live_databases_share_one_instance
    a = open_db
    b = open_db

    assert_same duckdb_database(a), duckdb_database(b)
    assert_equal 2, Registry.references(@path)
  end

  def test_rows_written_through_one_database_are_read_through_another
    a = open_db
    b = open_db
    a.create_table(:t) { Integer :x }
    a[:t].insert(x: 7)

    assert_equal [ 7 ], b[:t].select_map(:x)
  end

  def test_last_disconnect_closes_the_file
    a = open_db
    b = open_db
    a.run("CREATE TABLE t (x INTEGER)")
    a.disconnect

    assert_equal 1, Registry.references(@path)
    refute other_process_can_open?

    b.disconnect

    assert_equal 0, Registry.references(@path)
    assert other_process_can_open?
  end

  def test_query_after_disconnect_opens_the_file_again
    a = open_db
    a.create_table(:t) { Integer :x }
    a[:t].insert(x: 1)
    a.disconnect

    assert_equal 0, Registry.references(@path)
    assert_equal 1, a[:t].count
    assert_equal 1, Registry.references(@path)
  end

  def test_disconnect_does_not_close_the_file_under_a_checked_out_connection
    a = open_db
    a.create_table(:t) { Integer :x }

    a.synchronize do |conn|
      a.disconnect

      assert_equal 1, Registry.references(@path)
      assert_equal [ [ 0 ] ], conn.query("SELECT count(*) FROM t").to_a
    end
    a.disconnect

    assert_equal 0, Registry.references(@path)
  end

  def test_two_spellings_of_one_path_share_one_instance
    a = open_db
    b = open_db(File.join(@dir, ".", "shared.duckdb"))

    assert_same duckdb_database(a), duckdb_database(b)
  end

  def test_in_memory_databases_are_separate
    a = Sequel.connect("duckdb::memory:").tap { |db| @dbs << db }
    b = Sequel.connect("duckdb::memory:").tap { |db| @dbs << db }
    a.create_table(:t) { Integer :x }

    refute_same duckdb_database(a), duckdb_database(b)
    refute b.table_exists?(:t)
  end

  def test_in_memory_data_survives_disconnect
    a = Sequel.connect("duckdb::memory:").tap { |db| @dbs << db }
    a.create_table(:t) { Integer :x }
    a[:t].insert(x: 1)
    a.disconnect

    assert_equal 1, a[:t].count
  end

  def test_concurrent_opens_share_one_instance
    dbs = Array.new(8) { Thread.new { open_db.tap { |db| db.get(1) } } }.map(&:value)

    assert_equal 1, dbs.map { |db| duckdb_database(db).object_id }.uniq.size
    assert_equal 8, Registry.references(@path)
  end

  # The README documents config:, and before this the adapter ignored it.
  def test_config_reaches_duckdb
    assert_equal 2, setting(open_db(config: { threads: 2 }), "threads")
  end

  def test_config_reaches_an_in_memory_database
    db = Sequel.connect(adapter: :duckdb, database: ":memory:", config: { threads: 3 }).tap { |d| @dbs << d }

    assert_equal 3, setting(db, "threads")
  end

  def test_same_config_shares_the_instance
    a = open_db(config: { threads: 2 })
    b = open_db(config: { "threads" => "2" })

    assert_same duckdb_database(a), duckdb_database(b)
  end

  # Python, JDBC and R raise here too: the first opener fixes the settings.
  def test_different_config_raises
    open_db(config: { threads: 2 })

    error = assert_raises(Sequel::DatabaseConnectionError) { open_db(config: { threads: 4 }) }
    assert_match(/different DuckDB settings/, error.message)
    assert_equal 1, Registry.references(@path)
  end

  def test_no_config_after_config_raises
    open_db(config: { threads: 2 })

    assert_raises(Sequel::DatabaseConnectionError) { open_db }
  end

  def test_config_after_no_config_raises
    open_db

    assert_raises(Sequel::DatabaseConnectionError) { open_db(config: { threads: 2 }) }
  end

  def test_reopen_with_different_config_raises
    a = open_db(config: { threads: 2 })
    a.get(1)
    a.disconnect
    open_db(config: { threads: 4 })

    assert_raises(Sequel::DatabaseConnectionError) { a.get(1) }
  end

  def test_new_config_is_accepted_after_the_file_closes
    open_db(config: { threads: 2 }).disconnect

    assert_equal 4, setting(open_db(config: { threads: 4 }), "threads")
  end

  def test_forked_child_does_not_reuse_the_parents_instance
    open_db

    pid = fork { exit!(Registry.references(@path).zero? ? 0 : 1) }
    _, status = Process.wait2(pid)

    assert_predicate status, :success?
  end
end
