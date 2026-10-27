# frozen_string_literal: true

require_relative "spec_helper"
require "logger"
require "stringio"
require "tmpdir"

# DuckDB supports CREATE OR REPLACE VIEW, so create_or_replace_view must use it.
# Sequel's fallback for other databases runs DROP VIEW first and logs its failure
# at ERROR, and an unqualified DROP VIEW can resolve to a view in another catalog.
class CreateOrReplaceViewSqlTest < SequelDuckDBTest::TestCase
  def test_supports_create_or_replace_view
    assert_predicate mock_db, :supports_create_or_replace_view?
  end

  def test_dataset_source_uses_or_replace
    mock_db.create_or_replace_view(:v, mock_db[:items])

    assert_equal [ %(CREATE OR REPLACE VIEW "v" AS SELECT * FROM "items") ], mock_db.sqls
  end

  def test_string_source_uses_or_replace
    mock_db.create_or_replace_view(:v, "SELECT 1 AS a")

    assert_equal [ %(CREATE OR REPLACE VIEW "v" AS SELECT 1 AS a) ], mock_db.sqls
  end

  def test_temporary_view_uses_or_replace
    mock_db.create_or_replace_view(:v, mock_db[:items], temp: true)

    assert_equal [ %(CREATE OR REPLACE TEMPORARY VIEW "v" AS SELECT * FROM "items") ], mock_db.sqls
  end

  # A Hash source carries the file options. The :replace and :temp options that
  # create_or_replace_view passes must survive next to them.
  def test_path_source_keeps_or_replace
    mock_db.create_or_replace_view(:v, { path: "/data/f.parquet" }, temp: true)

    assert_equal [ %(CREATE OR REPLACE TEMPORARY VIEW "v" AS SELECT * FROM read_parquet(['/data/f.parquet'])) ],
                 mock_db.sqls
  end

  def test_using_source_keeps_or_replace
    mock_db.create_or_replace_view(:v, { using: "parquet", options: { path: "/data/f.parquet" } })

    assert_equal [ %(CREATE OR REPLACE VIEW "v" AS SELECT * FROM read_parquet(['/data/f.parquet'])) ], mock_db.sqls
  end

  def test_create_view_with_path_source_is_unchanged
    mock_db.create_view(:v, path: "/data/f.parquet")

    assert_equal [ %(CREATE VIEW "v" AS SELECT * FROM read_parquet(['/data/f.parquet'])) ], mock_db.sqls
  end
end

class CreateOrReplaceViewTest < SequelDuckDBTest::TestCase
  def setup
    super
    @dir = Dir.mktmpdir
    @log = StringIO.new
    @db = create_db
    @db.loggers << Logger.new(@log)
    @db.create_table(:items) do
      Integer :id
      String :name
    end
    @db[:items].insert(id: 1, name: "a")
  end

  def teardown
    @db&.disconnect
    FileUtils.remove_entry(@dir)
    super
  end

  def test_new_view_logs_no_error
    @db.create_or_replace_view(:v, @db[:items].select(:id))

    assert_equal [ { id: 1 } ], @db[:v].all
    refute_match(/DROP VIEW/, @log.string)
    refute_match(/ERROR/, @log.string)
  end

  def test_replaces_an_existing_view
    @db.create_view(:v, @db[:items].select(:id))
    @db.create_or_replace_view(:v, @db[:items])

    assert_equal [ { id: 1, name: "a" } ], @db[:v].all
    refute_match(/DROP VIEW/, @log.string)
  end

  def test_replaces_an_existing_temporary_view
    @db.create_view(:v, @db[:items].select(:id), temp: true)
    @db.create_or_replace_view(:v, @db[:items], temp: true)

    assert_equal [ { id: 1, name: "a" } ], @db[:v].all
    refute_match(/ERROR/, @log.string)
  end

  def test_replaces_a_view_with_a_path_source
    path = File.join(@dir, "items.parquet")
    @db.copy_to(@db[:items], path)
    @db.create_view(:v, @db[:items].select(:id))
    @db.create_or_replace_view(:v, path: path)

    assert_equal [ { id: 1, name: "a" } ], @db[:v].all
    refute_match(/DROP VIEW/, @log.string)
  end

  # sequelizer's make_readyable does this for each table of a read-only dataset
  # catalog that is the current catalog. The DROP VIEW fallback failed there.
  def test_temporary_view_over_a_read_only_catalog_logs_no_error
    path = File.join(@dir, "dataset.duckdb")
    source = Sequel.connect("duckdb://#{path}")
    source.create_table(:people) { Integer :id }
    source[:people].insert(7)
    source.disconnect
    @db.attach(path, as: :dataset, read_only: true)
    @db.run("USE dataset")

    @db.create_or_replace_view(:people, @db[Sequel[:dataset][:people]], temp: true)

    assert_equal [ 7 ], @db[:people].select_map(:id)
    refute_match(/ERROR/, @log.string)
  end

  private

  def create_db
    SequelDuckDBTest.create_test_db
  end
end
