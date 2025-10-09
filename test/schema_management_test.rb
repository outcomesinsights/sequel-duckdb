# frozen_string_literal: true

require_relative "spec_helper"

describe "Schema Management" do
  before do
    @db = Sequel.connect("duckdb::memory:")
  end

  after do
    @db.disconnect
  end

  describe "create_schema" do
    it "creates a basic schema" do
      @db.create_schema(:test_schema)
      _(@db.schemas).must_include(:test_schema)
    end

    it "creates schema with IF NOT EXISTS" do
      @db.create_schema(:test_schema, if_not_exists: true)
      # Should not raise error when creating again
      @db.create_schema(:test_schema, if_not_exists: true)
      _(@db.schemas).must_include(:test_schema)
    end

    it "creates schema with OR REPLACE" do
      @db.create_schema(:test_schema)
      @db.create_schema(:test_schema, or_replace: true)
      _(@db.schemas).must_include(:test_schema)
    end

    it "raises error for duplicate schema without options" do
      @db.create_schema(:test_schema)
      _ { @db.create_schema(:test_schema) }.must_raise(Sequel::DatabaseError)
    end

    it "handles schema names that require quoting" do
      @db.create_schema(:"test-schema")
      _(@db.schemas).must_include(:"test-schema")
    end

    it "handles schema names that are reserved words" do
      @db.create_schema(:order, if_not_exists: true)
      _(@db.schemas).must_include(:order)
    end
  end

  describe "create_schema_sql" do
    it "generates basic CREATE SCHEMA SQL" do
      sql = @db.create_schema_sql(:test_schema)
      _(sql).must_equal('CREATE SCHEMA "test_schema"')
    end

    it "generates CREATE SCHEMA IF NOT EXISTS SQL" do
      sql = @db.create_schema_sql(:test_schema, if_not_exists: true)
      _(sql).must_equal('CREATE SCHEMA IF NOT EXISTS "test_schema"')
    end

    it "generates CREATE OR REPLACE SCHEMA SQL" do
      sql = @db.create_schema_sql(:test_schema, or_replace: true)
      _(sql).must_equal('CREATE OR REPLACE SCHEMA "test_schema"')
    end

    it "properly quotes schema names with special characters" do
      sql = @db.create_schema_sql(:"test-schema")
      _(sql).must_equal('CREATE SCHEMA "test-schema"')
    end
  end

  describe "drop_schema" do
    it "drops an empty schema" do
      @db.create_schema(:test_schema)
      @db.drop_schema(:test_schema)
      _(@db.schemas).wont_include(:test_schema)
    end

    it "drops schema with IF EXISTS" do
      # Should not raise error when dropping non-existent schema
      @db.drop_schema(:non_existent, if_exists: true)
    end

    it "drops schema with CASCADE" do
      @db.create_schema(:test_schema)
      @db.create_table(Sequel[:test_schema][:test_table]) do
        primary_key :id
        String :name
      end

      @db.drop_schema(:test_schema, cascade: true)
      _(@db.schemas).wont_include(:test_schema)
    end

    it "raises error when dropping non-existent schema without IF EXISTS" do
      _ { @db.drop_schema(:non_existent) }.must_raise(Sequel::DatabaseError)
    end

    it "raises error when dropping schema with objects without CASCADE" do
      @db.create_schema(:test_schema)
      @db.create_table(Sequel[:test_schema][:test_table]) do
        primary_key :id
        String :name
      end

      _ { @db.drop_schema(:test_schema) }.must_raise(Sequel::DatabaseError)
    end

    it "clears schema cache after drop" do
      @db.create_schema(:test_schema)
      @db.create_table(Sequel[:test_schema][:test_table]) do
        primary_key :id
        String :name
      end

      # Verify table exists
      tables = @db.tables(schema: "test_schema")
      _(tables).must_include(:test_table)

      @db.drop_schema(:test_schema, cascade: true)

      # Verify schema was dropped
      _(@db.schema_exists?(:test_schema)).must_equal(false)
    end
  end

  describe "drop_schema_sql" do
    it "generates basic DROP SCHEMA SQL" do
      sql = @db.drop_schema_sql(:test_schema)
      _(sql).must_equal('DROP SCHEMA "test_schema"')
    end

    it "generates DROP SCHEMA IF EXISTS SQL" do
      sql = @db.drop_schema_sql(:test_schema, if_exists: true)
      _(sql).must_equal('DROP SCHEMA IF EXISTS "test_schema"')
    end

    it "generates DROP SCHEMA CASCADE SQL" do
      sql = @db.drop_schema_sql(:test_schema, cascade: true)
      _(sql).must_equal('DROP SCHEMA "test_schema" CASCADE')
    end

    it "generates DROP SCHEMA IF EXISTS CASCADE SQL" do
      sql = @db.drop_schema_sql(:test_schema, if_exists: true, cascade: true)
      _(sql).must_equal('DROP SCHEMA IF EXISTS "test_schema" CASCADE')
    end
  end

  describe "schemas" do
    it "lists all schemas including default main schema" do
      schemas = @db.schemas
      _(schemas).must_include(:main)
    end

    it "includes custom schemas" do
      @db.create_schema(:analytics)
      @db.create_schema(:staging)

      schemas = @db.schemas
      _(schemas).must_include(:main)
      _(schemas).must_include(:analytics)
      _(schemas).must_include(:staging)
    end

    it "returns schemas as symbols" do
      @db.create_schema(:test_schema)
      schemas = @db.schemas
      _(schemas.all? { |s| s.is_a?(Symbol) }).must_equal(true)
    end
  end

  describe "schema_exists?" do
    it "returns true for existing schema" do
      _(@db.schema_exists?(:main)).must_equal(true)
    end

    it "returns false for non-existent schema" do
      _(@db.schema_exists?(:non_existent)).must_equal(false)
    end

    it "returns true for custom schemas" do
      @db.create_schema(:test_schema)
      _(@db.schema_exists?(:test_schema)).must_equal(true)
    end

    it "returns false after schema is dropped" do
      @db.create_schema(:test_schema)
      @db.drop_schema(:test_schema)
      _(@db.schema_exists?(:test_schema)).must_equal(false)
    end

    it "handles string and symbol schema names" do
      @db.create_schema(:test_schema)
      _(@db.schema_exists?(:test_schema)).must_equal(true)
      _(@db.schema_exists?("test_schema")).must_equal(true)
    end
  end

  describe "schema usage" do
    before do
      @db.create_schema(:analytics)
    end

    after do
      @db.drop_schema(:analytics, cascade: true, if_exists: true)
    end

    it "creates tables in custom schemas" do
      @db.create_table(Sequel[:analytics][:sales]) do
        primary_key :id
        String :product
        column :amount, "DECIMAL(10,2)"
      end

      tables = @db.tables(schema: "analytics")
      _(tables).must_include(:sales)
    end

    it "allows cross-schema queries" do
      @db.create_table(:products) do
        Integer :id, primary_key: true
        String :name
      end

      @db.create_table(Sequel[:analytics][:sales]) do
        Integer :id, primary_key: true
        Integer :product_id
        column :amount, "DECIMAL(10,2)"
      end

      @db[:products].insert(id: 1, name: "Widget")

      # Use qualified identifier for analytics schema table
      @db.execute("INSERT INTO analytics.sales (id, product_id, amount) VALUES (1, 1, 99.99)")

      result = @db.fetch("SELECT * FROM analytics.sales").first
      _(result[:amount].to_f).must_be_close_to(99.99, 0.01)
    end

    it "retrieves schema information for tables in custom schemas" do
      @db.create_table(Sequel[:analytics][:reports]) do
        primary_key :id
        String :title
        column :created_at, "TIMESTAMP"
      end

      # Use information_schema to get column information
      columns = @db.fetch("SELECT column_name FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'reports'").all
      column_names = columns.map { |c| c[:column_name].to_sym }

      _(column_names).must_include(:id)
      _(column_names).must_include(:title)
      _(column_names).must_include(:created_at)
    end
  end

  describe "edge cases" do
    it "raises error for mutually exclusive options" do
      # DuckDB doesn't support both OR REPLACE and IF NOT EXISTS together
      _ { @db.create_schema(:test_schema, if_not_exists: true, or_replace: true) }.must_raise(Sequel::Error)
    end

    it "handles schema names with underscores" do
      @db.create_schema(:my_test_schema)
      _(@db.schema_exists?(:my_test_schema)).must_equal(true)
      @db.drop_schema(:my_test_schema)
    end

    it "handles schema names with numbers" do
      @db.create_schema(:schema123)
      _(@db.schema_exists?(:schema123)).must_equal(true)
      @db.drop_schema(:schema123)
    end
  end

  describe "remove_all_cached_schemas" do
    it "clears schema cache" do
      @db.create_schema(:test_schema)
      @db.create_table(Sequel[:test_schema][:test_table]) do
        primary_key :id
        String :name
      end

      # Access tables to potentially cache schema info
      @db.tables

      # Clear cache
      @db.remove_all_cached_schemas

      # Verify schemas method still works after cache clear
      schemas = @db.schemas
      _(schemas).must_include(:test_schema)
    end

    it "allows Sequel's remove_cached_schema to work after clearing cache" do
      @db.create_schema(:test_schema)
      @db.create_table(Sequel[:test_schema][:test_table]) do
        primary_key :id
        String :name
      end

      # Clear all cached schemas
      @db.remove_all_cached_schemas

      # This should not raise an error - Sequel's remove_cached_schema
      # expects @schemas to be a Hash, not nil
      # This is what happens internally when create_view is called
      _(@db.send(:remove_cached_schema, Sequel[:test_schema][:test_table])).must_be_nil

      # Verify we can still work with the database
      tables = @db.tables(schema: "test_schema")
      _(tables).must_include(:test_table)
    end

    it "properly initializes cache variables as hashes" do
      @db.remove_all_cached_schemas

      # Access instance variables to verify they're hashes, not nil
      schema_cache = @db.instance_variable_get(:@schema_cache)
      schemas = @db.instance_variable_get(:@schemas)
      primary_keys = @db.instance_variable_get(:@primary_keys)
      primary_key_sequences = @db.instance_variable_get(:@primary_key_sequences)

      _(schema_cache).must_be_kind_of(Hash)
      _(schemas).must_be_kind_of(Hash)
      _(primary_keys).must_be_kind_of(Hash)
      _(primary_key_sequences).must_be_kind_of(Hash)

      _(schema_cache).must_be_empty
      _(schemas).must_be_empty
      _(primary_keys).must_be_empty
      _(primary_key_sequences).must_be_empty
    end
  end
end
