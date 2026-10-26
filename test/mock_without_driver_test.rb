# frozen_string_literal: true

require "minitest/autorun"
require "open3"
require "rbconfig"

class MockWithoutDriverTest < Minitest::Test
  RUBY = RbConfig.ruby

  # The child keeps the parent's Bundler environment, so it loads the same Gemfile
  # and lockfile. Clearing only BUNDLE_GEMFILE once made the child pair the root
  # Gemfile with gemfiles/duckdb-1.5.gemfile.lock (Bundler 4 passes BUNDLE_LOCKFILE),
  # and it rewrote that lockfile. The code below blocks require "duckdb" itself.
  def run_ruby(code)
    Open3.capture3(
      RUBY,
      "-Ilib",
      "-e",
      code,
      chdir: File.expand_path("..", __dir__),
    )
  end

  def test_mock_duckdb_does_not_require_native_driver
    code = <<~RUBY
      require "rubygems"
      module Kernel
        alias __orig_require__ require

        def require(path)
          raise LoadError, "blocked duckdb" if path == "duckdb"

          __orig_require__(path)
        end
      end

      require "sequel"
      require_relative "lib/sequel/adapters/shared/duckdb"

      db = Sequel.mock(host: :duckdb)
      abort "wrong database type" unless db.database_type == :duckdb
      puts db[:items].with(:x, db[:items]).sql
    RUBY

    stdout, stderr, status = run_ruby(code)

    assert_predicate status, :success?, "stdout=#{stdout}\nstderr=#{stderr}"
    assert_includes stdout, 'WITH "x" AS'
  end
end
