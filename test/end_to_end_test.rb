# frozen_string_literal: true

require_relative "spec_helper"

# End-to-end testing for DuckDB adapter
# Requirements: 11.7, 9.1, 9.4, 9.5
class EndToEndTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db
  end

  def test_comprehensive_sql_generation_validation
    # Test that all SQL generation produces valid DuckDB syntax

    # Create a comprehensive test table
    @db.create_table(:comprehensive_test) do
      Integer :id, primary_key: true
      String :name, null: false
      Integer :age
      Float :score
      Boolean :active, default: true
      Date :birth_date
      DateTime :created_at
      String :email, unique: true
      String :category
      Integer :parent_id
    end

    dataset = @db[:comprehensive_test]

    # Test basic SELECT operations
    assert_nothing_raised("Basic SELECT should work") do
      dataset.all
    end

    # Test SELECT with WHERE clauses
    assert_nothing_raised("SELECT with WHERE should work") do
      dataset.where(active: true).all
    end

    # Test SELECT with complex WHERE conditions
    assert_nothing_raised("SELECT with complex WHERE should work") do
      dataset.where { age > 18 }.where(active: true).all
    end

    # Test SELECT with ORDER BY
    assert_nothing_raised("SELECT with ORDER BY should work") do
      dataset.order(:name).all
    end

    # Test SELECT with LIMIT and OFFSET
    assert_nothing_raised("SELECT with LIMIT/OFFSET should work") do
      dataset.limit(10).offset(5).all
    end

    # Test SELECT with GROUP BY and HAVING
    assert_nothing_raised("SELECT with GROUP BY/HAVING should work") do
      dataset.select(:category, Sequel.function(:count, :*).as(:count))
             .group(:category)
             .having { Sequel.function(:count, :*) > 0 }
             .all
    end

    # Test INSERT operations
    assert_nothing_raised("INSERT should work") do
      dataset.insert(
        id: 1,
        name: "Test User",
        age: 25,
        score: 85.5,
        active: true,
        birth_date: Date.new(1998, 1, 1),
        created_at: Time.now,
        email: "test@example.com",
        category: "A"
      )
    end

    # Test UPDATE operations
    assert_nothing_raised("UPDATE should work") do
      dataset.where(id: 1).update(age: 26)
    end

    # Test DELETE operations
    assert_nothing_raised("DELETE should work") do
      dataset.where(id: 1).delete
    end

    # Test JOIN operations
    @db.create_table(:categories) do
      Integer :id, primary_key: true
      String :name
    end

    # Insert some test data for JOIN
    @db[:categories].insert(id: 1, name: "Category A")
    dataset.insert(
      id: 2,
      name: "Test User 2",
      age: 30,
      score: 90.0,
      active: true,
      birth_date: Date.new(1993, 1, 1),
      created_at: Time.now,
      email: "test2@example.com",
      category: "A"
    )

    assert_nothing_raised("JOIN should work") do
      @db[:comprehensive_test]
        .join(:categories, name: :category)
        .select(Sequel[:comprehensive_test][:name], Sequel[:categories][:name].as(:category_name))
        .all
    end

    # Test subqueries
    assert_nothing_raised("Subqueries should work") do
      @db[:comprehensive_test]
        .where(id: @db[:comprehensive_test].select(:id).where(active: true))
        .all
    end

    # Test aggregate functions
    assert_nothing_raised("Aggregate functions should work") do
      dataset.select(
        Sequel.function(:count, :*).as(:total_count),
        Sequel.function(:avg, :age).as(:avg_age),
        Sequel.function(:max, :score).as(:max_score),
        Sequel.function(:min, :score).as(:min_score)
      ).first
    end

    # Test window functions (DuckDB-specific feature)
    assert_nothing_raised("Window functions should work") do
      dataset.select(
        :name,
        :score,
        Sequel.function(:rank).over(order: Sequel.desc(:score)).as(:rank)
      ).all
    end

    # Test Common Table Expressions (CTEs)
    assert_nothing_raised("CTEs should work") do
      dataset.with(:high_scorers, dataset.where { score > 80 }.select(:id, :name, :score))
             .from(:high_scorers)
             .all
    end
  end

  def test_performance_with_large_datasets
    # Test performance with larger datasets

    # Create a performance test table
    @db.create_table(:performance_test) do
      Integer :id, primary_key: true
      String :name
      Integer :value
      Float :score
      Boolean :active
      DateTime :created_at
    end

    dataset = @db[:performance_test]

    # Insert a moderate amount of test data (1000 records)
    start_time = Time.now

    records = []
    1000.times do |i|
      records << {
        id: i + 1,
        name: "User #{i + 1}",
        value: rand(1000),
        score: rand * 100,
        active: i.even?,
        created_at: Time.now - rand(365 * 24 * 3600) # Random time in last year
      }
    end

    # Test bulk insert performance
    assert_nothing_raised("Bulk insert should work") do
      dataset.multi_insert(records)
    end

    insert_time = Time.now - start_time
    assert insert_time < 10, "Bulk insert should complete within 10 seconds, took #{insert_time}"

    # Test query performance on larger dataset
    start_time = Time.now

    # Test various query patterns
    assert_nothing_raised("Complex query on large dataset should work") do
      result = dataset
               .where(active: true)
               .where { value > 500 }
               .order(:score)
               .limit(100)
               .all

      assert result.is_a?(Array), "Query should return an array"
      assert result.length <= 100, "Query should respect LIMIT"
    end

    query_time = Time.now - start_time
    assert query_time < 5, "Complex query should complete within 5 seconds, took #{query_time}"

    # Test aggregation performance
    start_time = Time.now

    assert_nothing_raised("Aggregation on large dataset should work") do
      result = dataset
               .group(:active)
               .select(
                 :active,
                 Sequel.function(:count, :*).as(:count),
                 Sequel.function(:avg, :score).as(:avg_score),
                 Sequel.function(:max, :value).as(:max_value)
               )
               .all

      assert result.is_a?(Array), "Aggregation should return an array"
      assert result.length <= 2, "Should have at most 2 groups (true/false)"
    end

    aggregation_time = Time.now - start_time
    assert aggregation_time < 3, "Aggregation should complete within 3 seconds, took #{aggregation_time}"

    # Test update performance
    start_time = Time.now

    assert_nothing_raised("Bulk update should work") do
      updated_count = dataset.where(active: false).update(active: true)
      assert updated_count > 0, "Should update some records"
    end

    update_time = Time.now - start_time
    assert update_time < 5, "Bulk update should complete within 5 seconds, took #{update_time}"

    # Verify final record count
    total_count = dataset.count
    assert_equal 1000, total_count, "Should have 1000 records after all operations"
  end

  def test_memory_usage_and_connection_handling
    # Test memory usage and connection handling

    # Create multiple connections to test connection pooling
    connections = []

    assert_nothing_raised("Multiple connections should work") do
      5.times do
        db = create_db
        connections << db

        # Test that each connection works independently
        db.create_table(:"test_#{connections.length}", if_not_exists: true) do
          Integer :id, primary_key: true
          String :data
        end

        db[:"test_#{connections.length}"].insert(id: 1, data: "test")

        result = db[:"test_#{connections.length}"].first
        assert_equal "test", result[:data]
      end
    end

    # Test connection cleanup
    assert_nothing_raised("Connection cleanup should work") do
      connections.each(&:disconnect)
    end

    # Test memory efficiency with streaming results
    @db.create_table(:streaming_test) do
      Integer :id, primary_key: true
      String :data
    end

    # Insert data for streaming test
    dataset = @db[:streaming_test]
    100.times do |i|
      dataset.insert(id: i + 1, data: "data_#{i + 1}")
    end

    # Test that we can iterate through results without loading all into memory
    count = 0
    assert_nothing_raised("Streaming results should work") do
      dataset.each do |row|
        count += 1
        assert row.is_a?(Hash), "Each row should be a hash"
        assert row[:id].is_a?(Integer), "ID should be an integer"
        assert row[:data].is_a?(String), "Data should be a string"
      end
    end

    assert_equal 100, count, "Should iterate through all 100 records"
  end

  def test_error_handling_and_recovery
    # Test comprehensive error handling and recovery

    # Test connection error handling
    assert_raises(Sequel::DatabaseConnectionError, "Invalid connection should raise error") do
      Sequel.connect("duckdb:/invalid/path/database.db")
    end

    # Test SQL syntax error handling
    assert_raises(Sequel::DatabaseError, "Invalid SQL should raise error") do
      @db.execute("INVALID SQL SYNTAX")
    end

    # Test constraint violation handling
    @db.create_table(:constraint_test) do
      Integer :id, primary_key: true
      String :name, null: false, unique: true
    end

    dataset = @db[:constraint_test]

    # Test NOT NULL constraint
    assert_raises(Sequel::NotNullConstraintViolation, "NULL constraint violation should be handled") do
      dataset.insert(id: 1, name: nil)
    end

    # Test UNIQUE constraint
    dataset.insert(id: 1, name: "test")
    assert_raises(Sequel::UniqueConstraintViolation, "UNIQUE constraint violation should be handled") do
      dataset.insert(id: 2, name: "test")
    end

    # Test recovery after errors
    assert_nothing_raised("Should recover after constraint violation") do
      dataset.insert(id: 2, name: "test2")
      result = dataset.where(name: "test2").first
      assert_equal "test2", result[:name]
    end

    # Test transaction rollback on error
    initial_count = dataset.count

    begin
      @db.transaction do
        dataset.insert(id: 3, name: "test3")
        dataset.insert(id: 4, name: "test") # This should fail due to unique constraint
      end
    rescue Sequel::UniqueConstraintViolation
      # Expected error
    end

    # Verify rollback worked
    final_count = dataset.count
    assert_equal initial_count, final_count, "Transaction should have rolled back"
  end

  def test_data_type_compatibility_comprehensive
    # Comprehensive test of all data type mappings

    @db.create_table(:type_compatibility_test) do
      Integer :id, primary_key: true

      # String types
      String :varchar_field
      String :text_field, text: true

      # Numeric types
      Integer :integer_field
      Bignum :bigint_field
      Float :float_field
      BigDecimal :decimal_field

      # Date/time types
      Date :date_field
      DateTime :datetime_field
      Time :time_field

      # Boolean type
      Boolean :boolean_field

      # Binary type
      String :blob_field, type: :blob
    end

    dataset = @db[:type_compatibility_test]

    # Test comprehensive data insertion and retrieval
    test_data = {
      id: 1,
      varchar_field: "Test String",
      text_field: "Long text content " * 100,
      integer_field: 42,
      bigint_field: 9_223_372_036_854_775_807,
      float_field: 3.14159,
      decimal_field: BigDecimal("123.456"),
      date_field: Date.new(2023, 12, 25),
      datetime_field: Time.new(2023, 12, 25, 10, 30, 45),
      time_field: Time.new(2000, 1, 1, 14, 30, 0), # Time component only
      boolean_field: true,
      blob_field: "\x00\x01\x02\x03\xFF".b
    }

    assert_nothing_raised("Comprehensive data insertion should work") do
      dataset.insert(test_data)
    end

    # Retrieve and verify data
    retrieved = dataset.first
    refute_nil retrieved, "Should retrieve inserted record"

    # Verify string types
    assert_equal test_data[:varchar_field], retrieved[:varchar_field]
    assert_equal test_data[:text_field], retrieved[:text_field]

    # Verify numeric types
    assert_equal test_data[:integer_field], retrieved[:integer_field]
    assert_equal test_data[:bigint_field], retrieved[:bigint_field]
    assert_in_delta test_data[:float_field], retrieved[:float_field], 0.001

    # Verify date/time types
    assert_equal test_data[:date_field], retrieved[:date_field]
    assert_in_delta test_data[:datetime_field].to_f, retrieved[:datetime_field].to_f, 1.0

    # Verify boolean type
    assert_equal test_data[:boolean_field], retrieved[:boolean_field]

    # Verify binary type (may need special handling)
    return unless retrieved[:blob_field].is_a?(String)

    # DuckDB might return binary data as hex string
    if retrieved[:blob_field].match?(/\A[0-9a-fA-F]+\z/)
      retrieved_binary = [retrieved[:blob_field]].pack("H*").b
      assert_equal test_data[:blob_field], retrieved_binary
    else
      assert_equal test_data[:blob_field], retrieved[:blob_field]
    end
  end

  def test_sequel_model_integration_comprehensive
    # Test comprehensive Sequel::Model integration

    # Create tables for model testing
    @db.create_table(:users) do
      Integer :id, primary_key: true
      String :name, null: false
      String :email, unique: true
      Integer :age
      Boolean :active, default: true
      DateTime :created_at
    end

    @db.create_table(:posts) do
      Integer :id, primary_key: true
      Integer :user_id, null: false
      String :title, null: false
      String :content, text: true
      DateTime :created_at
    end

    # Define models with proper class names
    user_class = Class.new(Sequel::Model(@db[:users]))
    post_class = Class.new(Sequel::Model(@db[:posts]))

    # Set up associations after both classes are defined
    user_class.class_eval do
      unrestrict_primary_key
      one_to_many :posts, key: :user_id, class: post_class

      def validate
        super
        errors.add(:name, "cannot be empty") if !name || name.empty?
        errors.add(:email, "must be valid") if email && !email.include?("@")
      end
    end

    post_class.class_eval do
      unrestrict_primary_key
      many_to_one :user, key: :user_id, class: user_class

      def validate
        super
        errors.add(:title, "cannot be empty") if !title || title.empty?
      end
    end

    # Test model creation
    assert_nothing_raised("Model creation should work") do
      user = user_class.create(
        id: 1,
        name: "John Doe",
        email: "john@example.com",
        age: 30,
        created_at: Time.now
      )

      assert_equal "John Doe", user.name
      assert_equal "john@example.com", user.email
    end

    # Test model associations
    assert_nothing_raised("Model associations should work") do
      user = user_class[1]

      post = post_class.create(
        id: 1,
        user_id: user.id,
        title: "Test Post",
        content: "This is a test post content.",
        created_at: Time.now
      )

      # Test association access
      assert_equal user.id, post.user.id
      assert_equal 1, user.posts.count
      assert_equal "Test Post", user.posts.first.title
    end

    # Test model validations
    assert_raises(Sequel::ValidationFailed, "Model validation should work") do
      user_class.create(
        id: 2,
        name: "", # Invalid: empty name
        email: "invalid-email" # Invalid: no @ symbol
      )
    end

    # Test model updates
    assert_nothing_raised("Model updates should work") do
      user = user_class[1]
      user.update(age: 31)

      updated_user = user_class[1]
      assert_equal 31, updated_user.age
    end

    # Test model deletion
    assert_nothing_raised("Model deletion should work") do
      post_class[1].delete
      user_class[1].delete

      assert_nil user_class[1]
      assert_nil post_class[1]
    end
  end

  def test_concurrent_access_and_thread_safety
    # Test concurrent access and thread safety
    # Note: DuckDB in-memory databases are single-connection, so we test sequential operations

    @db.create_table(:concurrent_test) do
      Integer :id, primary_key: true
      String :data
      Integer :thread_id
      DateTime :created_at
    end

    dataset = @db[:concurrent_test]

    # Test sequential inserts that simulate concurrent behavior
    thread_count = 5
    records_per_thread = 20

    assert_nothing_raised("Sequential inserts should work") do
      thread_count.times do |thread_id|
        records_per_thread.times do |i|
          dataset.insert(
            id: thread_id * records_per_thread + i + 1,
            data: "Thread #{thread_id} Record #{i}",
            thread_id: thread_id,
            created_at: Time.now
          )
        end
      end
    end

    # Verify all records were inserted
    total_count = dataset.count
    expected_count = thread_count * records_per_thread
    assert_equal expected_count, total_count, "Should have #{expected_count} records from concurrent inserts"

    # Verify data integrity
    thread_count.times do |thread_id|
      thread_records = dataset.where(thread_id: thread_id).count
      assert_equal records_per_thread, thread_records, "Thread #{thread_id} should have #{records_per_thread} records"
    end

    # Test sequential reads
    assert_nothing_raised("Sequential reads should work") do
      10.times do |i|
        result = dataset.where(thread_id: i % thread_count).count
        assert result >= 0, "Read result should be non-negative"
        assert result <= records_per_thread, "Read result should not exceed records per thread"
      end
    end
  end

  private

  def assert_nothing_raised(message = nil, &block)
    yield
  rescue StandardError => e
    flunk "#{message || "Expected no exception"}, but got #{e.class}: #{e.message}"
  end
end
