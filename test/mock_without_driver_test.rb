# frozen_string_literal: true

require "minitest/autorun"
require "open3"
require "rbconfig"

class MockWithoutDriverTest < Minitest::Test

  RUBY = RbConfig.ruby

  def run_ruby(code)
    Open3.capture3(
      {
        "RUBYOPT" => nil.to_s,
        "BUNDLE_GEMFILE" => nil.to_s,
      },
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
