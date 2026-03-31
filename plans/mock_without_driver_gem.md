# DuckDB Mock-Without-Driver Refactor Plan

## Goal

Make DuckDB's Sequel adapter behave like Sequel's PostgreSQL adapter:

- `Sequel.mock(host: :duckdb)` must work without the `duckdb` gem installed.
- DuckDB-specific SQL generation and dataset behavior must remain available in mock mode.
- Real DuckDB connections (`Sequel.connect("duckdb:...")`) must still require the `duckdb` gem and fail clearly if it is missing.

This plan is intentionally detailed enough for a less capable agent to implement directly.

## Why This Refactor Exists

Today [`lib/sequel/adapters/shared/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/adapters/shared/duckdb.rb) starts with:

```ruby
require "duckdb"
```

That means the shared adapter layer is not actually shared. It pulls in the native driver even when Sequel only wants mock SQL generation via `Sequel.mock(host: :duckdb)`.

Sequel PostgreSQL does **not** do that. Its structure is:

- `lib/sequel/adapters/shared/postgres.rb`: shared SQL + mock support
- `lib/sequel/adapters/postgres.rb`: loads `pg` and real connection code

DuckDB should follow that pattern.

## Current Files Involved

- [`lib/sequel/adapters/shared/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/adapters/shared/duckdb.rb)
- [`lib/sequel/adapters/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/adapters/duckdb.rb)
- [`lib/sequel/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/duckdb.rb)
- [`test/mock_adapter_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/mock_adapter_test.rb)
- [`test/sql_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/sql_test.rb)
- [`test/core_sql_generation_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/core_sql_generation_test.rb)
- [`test/advanced_sql_generation_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/advanced_sql_generation_test.rb)
- [`test/date_arithmetic_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/date_arithmetic_test.rb)
- [`test/database_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/database_test.rb)
- [`test/spec_helper.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/spec_helper.rb)

## Design

Split the current adapter into two layers:

1. Shared layer
   - pure SQL generation
   - mock adapter setup
   - helper-module load order
   - no `require "duckdb"`

2. Driver-backed layer
   - `require "duckdb"`
   - real `connect`, `disconnect_connection`, `valid_connection?`, execution methods
   - anything that directly references `::DuckDB::*`

## Required Outcome

These must all be true after the refactor:

1. `ruby -e 'require "sequel"; db = Sequel.mock(host: :duckdb); puts db.database_type'`
   returns `duckdb` without loading the native gem.
2. Mock SQL behavior still works:
   - CTE support
   - date arithmetic
   - dataset SQL generation
   - helper constants/modules under `Sequel::DuckDB`
3. `Sequel.connect("duckdb::memory:")` still works when the `duckdb` gem is present.
4. Real connection setup fails clearly when the `duckdb` gem is absent.

## Implementation Steps

### Step 1: Remove driver loading from the shared layer

Edit [`lib/sequel/adapters/shared/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/adapters/shared/duckdb.rb).

Remove this line:

```ruby
require "duckdb"
```

The shared adapter file must not reference `::DuckDB::Database`, `::DuckDB::Connection`, or `::DuckDB::Error` anywhere after this change.

If it currently does, move those methods out into the real adapter file in Step 2.

### Step 2: Move all native-driver-dependent code into the real adapter file

Edit [`lib/sequel/adapters/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/adapters/duckdb.rb).

This file should be the only place that does:

```ruby
require "duckdb"
require_relative "shared/duckdb"
```

If needed, add a small driver-specific module to keep the split obvious:

```ruby
module Sequel
  module DuckDB
    module DriverDatabaseMethods
      def connect(server)
        opts = server_opts(server)
        database_path = opts[:database]

        begin
          db =
            if database_path == ":memory:" || database_path.nil?
              ::DuckDB::Database.open(":memory:")
            else
              database_path = "/#{database_path}" if database_path.match?(/^[a-zA-Z]/) && !database_path.start_with?(":")
              ::DuckDB::Database.open(database_path)
            end

          db.connect
        rescue ::DuckDB::Error => e
          raise Sequel::DatabaseConnectionError, "Failed to connect to DuckDB database: #{e.message}"
        rescue StandardError => e
          raise Sequel::DatabaseConnectionError, "Unexpected error connecting to DuckDB: #{e.message}"
        end
      end

      def disconnect_connection(conn)
        return unless conn

        begin
          conn.close
        rescue ::DuckDB::Error
        end
      end

      def valid_connection?(conn)
        return false unless conn

        begin
          conn.query("SELECT 1")
          true
        rescue ::DuckDB::Error
          false
        end
      end

      private

      def database_error_classes
        [::DuckDB::Error]
      end

      def result_column_names(result)
        result.columns.map { |c| c.respond_to?(:name) ? c.name.to_s : c.to_s }
      end
    end
  end
end
```

Then include both modules in `Database`:

```ruby
class Database < Sequel::Database
  include Sequel::DuckDB::DatabaseMethods
  include Sequel::DuckDB::DriverDatabaseMethods
end
```

Do the same for any dataset methods that truly require `::DuckDB` classes. If `Dataset#fetch_rows` only works with a real connection/result object, it can stay in the real adapter file.

### Step 3: Keep the shared module responsible for mock adapter setup

In [`lib/sequel/adapters/shared/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/adapters/shared/duckdb.rb), keep:

- `Sequel::Database.set_shared_adapter_scheme(:duckdb, Sequel::DuckDB)`
- `mock_adapter_setup`
- `DatabaseMethods`
- `DatasetMethods`

The shared file should be loadable in an environment where `duckdb` is unavailable.

### Step 4: Make the lightweight namespace file stay lightweight

[`lib/sequel/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/duckdb.rb) should remain a namespace/version file only. Do not make it require the native driver.

That file can continue to define:

```ruby
module Sequel
  module DuckDB
    class Error < StandardError; end
  end
end
```

That placeholder error class is acceptable for mock-mode loadability. The real adapter file can overwrite or augment constants after `require "duckdb"`.

### Step 5: Add an explicit driver-absence test

Add a new test file:

- [`test/mock_without_driver_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/mock_without_driver_test.rb)

This test must execute in a subprocess with the `duckdb` gem artificially blocked. Do **not** rely on the local bundle naturally lacking `duckdb`; this repo normally includes it.

Recommended approach:

```ruby
require "open3"
require "rbconfig"
require_relative "spec_helper"

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
    assert status.success?, "stdout=#{stdout}\nstderr=#{stderr}"
    assert_includes stdout, 'WITH "x" AS'
  end
end
```

The exact subprocess harness can vary, but the point is mandatory:

- if `require "duckdb"` happens during shared/mock setup, this test must fail
- after the refactor, it must pass

### Step 6: Tighten the existing mock adapter test

Update [`test/mock_adapter_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/mock_adapter_test.rb).

Add assertions beyond `refute_nil db`:

```ruby
def test_mock_duckdb_connection_sets_database_type
  db = Sequel.mock(host: :duckdb)
  assert_equal :duckdb, db.database_type
end

def test_mock_duckdb_dataset_supports_ctes
  db = Sequel.mock(host: :duckdb)
  assert_equal true, db.dataset.send(:supports_cte?)
end

def test_mock_duckdb_uses_duckdb_interval_sql
  db = Sequel.mock(host: :duckdb)
  ds = db[:items].select(Sequel.date_add(:start_date, days: 2).as(:shifted))
  assert_match(/INTERVAL 2 DAY|INTERVAL 2 days|INTERVAL '2 day'/i, ds.sql)
end
```

Use the actual SQL shape produced by the adapter after implementation. Do not force the old string if the current adapter intentionally emits a different-but-valid DuckDB form.

### Step 7: Ensure SQL-generation tests use mock DBs only

Review these tests:

- [`test/sql_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/sql_test.rb)
- [`test/core_sql_generation_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/core_sql_generation_test.rb)
- [`test/advanced_sql_generation_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/advanced_sql_generation_test.rb)
- [`test/date_arithmetic_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/date_arithmetic_test.rb)

Each of these should use `Sequel.mock(host: :duckdb)` or the helper that builds such a DB.

If any of them currently rely on `Sequel.connect("duckdb::memory:")` just to test SQL rendering, move them to mock mode.

Keep integration/driver tests separate.

### Step 8: Keep real integration tests real

These tests should continue using a real DuckDB connection:

- [`test/database_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/database_test.rb)
- [`test/schema_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/schema_test.rb)
- [`test/schema_metadata_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/schema_metadata_test.rb)
- [`test/schema_introspection_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/schema_introspection_test.rb)
- [`test/type_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/type_test.rb)
- [`test/model_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/model_test.rb)
- [`test/end_to_end_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/end_to_end_test.rb)

Those tests should continue to exercise the real driver-backed adapter and should still require `duckdb`.

## Expected File-Level Changes

### Files likely edited

- [`lib/sequel/adapters/shared/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/adapters/shared/duckdb.rb)
- [`lib/sequel/adapters/duckdb.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/lib/sequel/adapters/duckdb.rb)
- [`test/mock_adapter_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/mock_adapter_test.rb)
- [`test/spec_helper.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/spec_helper.rb)

### Files likely added

- [`test/mock_without_driver_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/mock_without_driver_test.rb)

### Files to inspect for fallout

- [`test/date_arithmetic_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/date_arithmetic_test.rb)
- [`test/core_sql_generation_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/core_sql_generation_test.rb)
- [`test/advanced_sql_generation_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-duckdb/test/advanced_sql_generation_test.rb)

## Verification Plan

Run these after implementation:

1. Mock-only tests

```bash
bundle exec ruby -Itest test/mock_adapter_test.rb
bundle exec ruby -Itest test/mock_without_driver_test.rb
bundle exec ruby -Itest test/sql_test.rb
bundle exec ruby -Itest test/core_sql_generation_test.rb
bundle exec ruby -Itest test/advanced_sql_generation_test.rb
bundle exec ruby -Itest test/date_arithmetic_test.rb
```

2. Real-driver tests

```bash
bundle exec ruby -Itest test/database_test.rb
bundle exec ruby -Itest test/schema_test.rb
bundle exec ruby -Itest test/schema_metadata_test.rb
```

3. Full suite

```bash
bundle exec rake test
```

## Definition Of Done

- Shared DuckDB adapter can be loaded in a process where `require "duckdb"` would fail.
- `Sequel.mock(host: :duckdb)` works and exercises DuckDB SQL behavior.
- Real DuckDB connections still work when the driver is installed.
- Real connection path, not mock path, is the only place that requires the native driver.
