# frozen_string_literal: true

require_relative "spec_helper"
require "tmpdir"

# Database#attach and #detach run DuckDB ATTACH and DETACH. attach_sql and detach_sql
# return the same SQL without running it.
class AttachSqlTest < SequelDuckDBTest::TestCase
  def test_attach_sql_quotes_the_path_and_the_alias
    assert_equal %(ATTACH '/data/it''s.duckdb' AS "dataset"), mock_db.attach_sql("/data/it's.duckdb", as: :dataset)
  end

  def test_attach_sql_read_only_and_if_not_exists
    assert_equal %(ATTACH IF NOT EXISTS '/data/d.duckdb' AS "dataset" (READ_ONLY)),
                 mock_db.attach_sql("/data/d.duckdb", as: :dataset, read_only: true, if_not_exists: true)
  end

  def test_attach_sql_passes_other_options
    assert_equal %(ATTACH '/data/d.sqlite' AS "lite" (TYPE SQLITE, READ_ONLY)),
                 mock_db.attach_sql("/data/d.sqlite", as: :lite, type: :sqlite, read_only: true)
  end

  def test_attach_sql_leaves_out_false_options
    assert_equal %(ATTACH '/data/d.duckdb' AS "dataset"), mock_db.attach_sql("/data/d.duckdb", as: :dataset, read_only: false)
  end

  def test_attach_sql_requires_an_alias
    assert_raises(ArgumentError) { mock_db.attach_sql("/data/d.duckdb") }
  end

  def test_attach_runs_the_sql
    db = mock_db
    db.attach("/data/d.duckdb", as: :dataset, read_only: true)

    assert_equal [ %(ATTACH '/data/d.duckdb' AS "dataset" (READ_ONLY)) ], db.sqls
  end

  def test_detach_sql
    assert_equal %(DETACH DATABASE "dataset"), mock_db.detach_sql(:dataset)
    assert_equal %(DETACH DATABASE IF EXISTS "it""s"), mock_db.detach_sql(%(it"s), if_exists: true)
  end

  def test_detach_runs_the_sql
    db = mock_db
    db.detach(:dataset, if_exists: true)

    assert_equal [ %(DETACH DATABASE IF EXISTS "dataset") ], db.sqls
  end
end

class AttachTest < SequelDuckDBTest::TestCase
  def setup
    super
    @dir = Dir.mktmpdir
    @path = File.join(@dir, "dataset's.duckdb")
    source = Sequel.connect("duckdb://#{@path}")
    source.create_table(:people) { Integer :id }
    source[:people].insert(7)
    source.disconnect
    @db = create_db
  end

  def teardown
    @db&.disconnect
    FileUtils.remove_entry(@dir)
    super
  end

  def test_attached_table_is_read_through_the_alias
    @db.attach(@path, as: :dataset, read_only: true)

    assert_equal [ 7 ], @db[Sequel[:dataset][:people]].select_map(:id)
  end

  def test_read_only_attach_refuses_writes
    @db.attach(@path, as: :dataset, read_only: true)

    assert_raises(Sequel::DatabaseError) { @db[Sequel[:dataset][:people]].insert(8) }
  end

  def test_attach_if_not_exists_can_repeat
    @db.attach(@path, as: :dataset, read_only: true)
    @db.attach(@path, as: :dataset, read_only: true, if_not_exists: true)

    assert_equal 1, @db[Sequel[:dataset][:people]].count
  end

  def test_detach_removes_the_catalog
    @db.attach(@path, as: :dataset, read_only: true)
    @db.detach(:dataset)

    refute @db.table_exists?(Sequel[:dataset][:people])
    @db.detach(:dataset, if_exists: true)
  end

  def test_attach_is_shared_by_all_pool_connections
    held = Queue.new
    release = Queue.new
    db = Sequel.connect("duckdb::memory:", max_connections: 2)
    db.attach(@path, as: :dataset, read_only: true)
    other = Thread.new { db.synchronize { held << true; release.pop } }
    held.pop

    assert_equal 1, db[Sequel[:dataset][:people]].count
    assert_equal 2, db.pool.size
  ensure
    release << true
    other&.join
    db&.disconnect
  end
end
