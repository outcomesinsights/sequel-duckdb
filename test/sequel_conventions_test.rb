# frozen_string_literal: true

require_relative "spec_helper"

# Test Sequel conventions compliance for DuckDB adapter
# Requirements: 13.2, 13.3, 13.4, 13.5, 13.7
class SequelConventionsTest < SequelDuckDBTest::TestCase
  def test_adapter_follows_sequel_exception_hierarchy
    # Test that DuckDB adapter uses Sequel's standard exception hierarchy
    db = create_db

    # Test connection errors map to Sequel::DatabaseConnectionError
    assert_raises(Sequel::DatabaseConnectionError) do
      Sequel.connect("duckdb:/invalid/path/database.db")
    end

    # Test constraint violations map to appropriate Sequel exceptions
    db.create_table(:test_constraints) do
      Integer :id, primary_key: true
      String :name, null: false, unique: true
    end

    # Test NOT NULL constraint violation
    assert_raises(Sequel::NotNullConstraintViolation) do
      db[:test_constraints].insert(id: 1, name: nil)
    end

    # Test UNIQUE constraint violation
    db[:test_constraints].insert(id: 1, name: "test")
    assert_raises(Sequel::UniqueConstraintViolation) do
      db[:test_constraints].insert(id: 2, name: "test")
    end

    # Test generic database errors
    assert_raises(Sequel::DatabaseError) do
      db.execute("INVALID SQL SYNTAX")
    end
  end

  def test_configuration_options_follow_sequel_patterns
    # Test that configuration options follow Sequel patterns

    # Test standard connection options
    db = Sequel.connect(
      adapter: "duckdb",
      database: ":memory:"
    )
    assert_connection_valid(db)

    # Test connection string format
    db2 = Sequel.connect("duckdb::memory:")
    assert_connection_valid(db2)

    # Test file database connection
    db3 = Sequel.connect("duckdb:test_db.duckdb")
    assert_connection_valid(db3)

    # Clean up test database file
    File.delete("test_db.duckdb") if File.exist?("test_db.duckdb")
  end

  def test_adapter_registration_follows_sequel_conventions
    # Test that adapter is properly registered with Sequel

    # Test that connection creates correct database class
    db = Sequel.connect("duckdb::memory:")
    assert_instance_of Sequel::DuckDB::Database, db

    # Test that datasets are correct class
    dataset = db[:test_table]
    assert_instance_of Sequel::DuckDB::Dataset, dataset

    # Test adapter scheme is set correctly
    assert_equal :duckdb, db.adapter_scheme
  end

  def test_ruby_version_compatibility
    # Test Ruby 3.1+ compatibility
    ruby_version = RUBY_VERSION.split(".").map(&:to_i)
    major = ruby_version[0]
    minor = ruby_version[1]

    assert major >= 3, "Ruby version should be 3.0 or higher"
    assert minor >= 1, "Ruby 3.x version should be 3.1 or higher" if major == 3

    # Test that modern Ruby features work
    db = create_db

    # Test keyword arguments (Ruby 3.0+ requirement)
    db.create_table(:ruby_features, if_not_exists: true) do
      primary_key :id
      String :name
    end

    # Test pattern matching (Ruby 3.0+ feature)
    result = case db.database_type
             in :duckdb
               "DuckDB adapter working"
             else
               "Unknown adapter"
             end

    assert_equal "DuckDB adapter working", result
  end

  def test_sequel_dataset_api_compliance
    # Test that Dataset class follows Sequel API conventions
    db = create_db
    create_test_table(db)
    insert_test_data(db)

    dataset = db[:test_table]

    # Test standard dataset methods exist and work
    assert_respond_to dataset, :all
    assert_respond_to dataset, :first
    assert_respond_to dataset, :count
    assert_respond_to dataset, :where
    assert_respond_to dataset, :order
    assert_respond_to dataset, :limit
    assert_respond_to dataset, :offset
    assert_respond_to dataset, :select
    assert_respond_to dataset, :insert
    assert_respond_to dataset, :update
    assert_respond_to dataset, :delete

    # Test method chaining works (Sequel convention)
    chained = dataset.where(active: true).order(:name).limit(1)
    assert_instance_of Sequel::DuckDB::Dataset, chained

    # Test that results are properly formatted
    all_records = dataset.all
    assert_instance_of Array, all_records
    assert_instance_of Hash, all_records.first if all_records.any?

    first_record = dataset.first
    assert_instance_of Hash, first_record if first_record

    count = dataset.count
    assert_instance_of Integer, count
  end

  def test_sequel_database_api_compliance
    # Test that Database class follows Sequel API conventions
    db = create_db

    # Test standard database methods exist and work
    assert_respond_to db, :tables
    assert_respond_to db, :schema
    assert_respond_to db, :indexes
    assert_respond_to db, :table_exists?
    assert_respond_to db, :create_table
    assert_respond_to db, :drop_table
    assert_respond_to db, :execute
    assert_respond_to db, :transaction
    assert_respond_to db, :test_connection
    assert_respond_to db, :disconnect

    # Test schema introspection follows Sequel format
    db.create_table(:schema_test) do
      Integer :id, primary_key: true
      String :name, null: false
      Integer :age
      Boolean :active, default: true
    end

    tables = db.tables
    assert_instance_of Array, tables
    assert_includes tables, :schema_test

    schema = db.schema(:schema_test)
    assert_instance_of Array, schema

    # Test schema format follows Sequel conventions
    schema.each do |column_name, column_info|
      assert_instance_of Symbol, column_name
      assert_instance_of Hash, column_info

      # Required keys in Sequel schema format
      assert_includes column_info.keys, :type
      assert_includes column_info.keys, :db_type
      assert_includes column_info.keys, :allow_null
      assert_includes column_info.keys, :default
      assert_includes column_info.keys, :primary_key
    end
  end

  def test_transaction_api_compliance
    # Test that transaction handling follows Sequel conventions
    db = create_db
    create_test_table(db)

    # Test basic transaction
    result = db.transaction do
      db[:test_table].insert(id: 100, name: "Transaction Test", age: 30)
      "success"
    end
    assert_equal "success", result

    # Verify record was inserted
    assert_record_exists db[:test_table], name: "Transaction Test"

    # Test transaction rollback
    initial_count = db[:test_table].count

    begin
      db.transaction do
        db[:test_table].insert(id: 101, name: "Rollback Test", age: 25)
        raise "Force rollback"
      end
    rescue StandardError
      # Expected to raise
    end

    # Verify rollback worked
    final_count = db[:test_table].count
    assert_equal initial_count, final_count

    # Test explicit rollback
    db.transaction do
      db[:test_table].insert(id: 102, name: "Explicit Rollback", age: 35)
      raise Sequel::Rollback
    end

    # Verify explicit rollback worked
    refute db[:test_table].where(name: "Explicit Rollback").first
  end

  def test_logging_integration
    # Test that logging follows Sequel conventions
    require "logger"

    log_output = StringIO.new
    logger = Logger.new(log_output)

    db = create_db
    db.loggers = [logger]

    # Execute some operations to generate logs
    db.create_table(:log_test, if_not_exists: true) do
      Integer :id, primary_key: true
      String :name
    end

    db[:log_test].insert(id: 1, name: "Log Test")
    db[:log_test].count

    # Check that SQL was logged
    log_content = log_output.string
    assert_includes log_content, "CREATE TABLE", "CREATE TABLE should be logged"
    assert_includes log_content, "INSERT", "INSERT should be logged"
    assert_includes log_content, "SELECT", "SELECT should be logged"
  end

  def test_connection_pooling_compliance
    # Test that connection pooling follows Sequel conventions
    db = Sequel.connect(
      adapter: "duckdb",
      database: ":memory:",
      max_connections: 2
    )

    # Test that connection pool is working
    assert_connection_valid(db)

    # Test concurrent access (basic test)
    threads = []
    results = []

    3.times do |i|
      threads << Thread.new do
        db.create_table(:"thread_test_#{i}", if_not_exists: true) do
          primary_key :id
          String :name
        end
        results << "success_#{i}"
      rescue StandardError => e
        results << "error_#{i}: #{e.message}"
      end
    end

    threads.each(&:join)

    # All operations should succeed
    success_count = results.count { |r| r.start_with?("success") }
    assert_equal 3, success_count, "All threaded operations should succeed"
  end

  def test_identifier_quoting_compliance
    # Test that identifier quoting follows Sequel conventions
    db = create_db

    # Test table with special characters
    db.create_table(:"test-table") do
      Integer :id, primary_key: true
      String :"column-name"
      String :select # SQL keyword
    end

    # Test that operations work with quoted identifiers
    db[:"test-table"].insert(id: 1, "column-name": "test", "select": "value")

    record = db[:"test-table"].first
    assert_equal "test", record[:"column-name"]
    assert_equal "value", record[:select]

    # Test schema introspection with quoted identifiers
    schema = db.schema(:"test-table")
    column_names = schema.map(&:first)
    assert_includes column_names, :"column-name"
    assert_includes column_names, :select
  end
end
