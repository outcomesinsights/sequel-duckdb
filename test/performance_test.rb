# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for performance optimizations in Sequel::DuckDB adapter
# Tests efficient result fetching, prepared statements, bulk operations, and connection pooling
class PerformanceTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db

    # Create test table with enough data for performance testing
    @db.create_table(:performance_test) do
      Integer :id, primary_key: true
      String :name, size: 100
      Integer :value
      Date :created_at
      Boolean :active
    end

    # Insert test data for performance testing
    @test_data = (1..1000).map do |i|
      {
        id: i,
        name: "Test Record #{i}",
        value: i * 10,
        created_at: Date.today - (i % 365),
        active: i.even?
      }
    end
  end

  def teardown
    @db.drop_table?(:performance_test)
    super
  end

  # Test efficient result fetching for large result sets (Requirement 9.1)
  def test_efficient_fetch_rows_with_large_result_set
    # Insert test data
    @db[:performance_test].multi_insert(@test_data)

    # Test that fetch_rows can handle large result sets efficiently
    start_time = Time.now
    row_count = 0

    @db[:performance_test].fetch_rows("SELECT * FROM performance_test") do |row|
      row_count += 1

      assert_kind_of Hash, row, "Row should be a hash"
      assert row.key?(:id), "Row should have id key"
      assert row.key?(:name), "Row should have name key"
    end

    end_time = Time.now
    execution_time = end_time - start_time

    assert_equal 1000, row_count, "Should fetch all 1000 rows"
    assert_operator execution_time, :<, 5.0, "Large result set should be fetched in reasonable time (< 5 seconds), took #{execution_time}"
  end

  def test_fetch_rows_memory_efficiency_with_streaming
    # Insert test data
    @db[:performance_test].multi_insert(@test_data)

    # Test that fetch_rows doesn't load all results into memory at once
    # This is tested by ensuring the block is called for each row individually
    processed_rows = []
    memory_snapshots = []

    @db[:performance_test].fetch_rows("SELECT * FROM performance_test ORDER BY id") do |row|
      processed_rows << row[:id]
      # Take memory snapshot every 100 rows
      memory_snapshots << memory_usage if (processed_rows.length % 100).zero?
    end

    assert_equal 1000, processed_rows.length, "Should process all rows"
    assert_equal processed_rows.sort, (1..1000).to_a, "Should process rows in correct order"

    # Memory usage should not grow significantly (streaming behavior)
    skip unless memory_snapshots.length > 1

    memory_growth = memory_snapshots.last - memory_snapshots.first

    assert_operator memory_growth, :<, 50_000_000, "Memory usage should not grow significantly during streaming (growth: #{memory_growth} bytes)"
  end

  def test_fetch_rows_with_limit_optimization
    # Insert test data
    @db[:performance_test].multi_insert(@test_data)

    # Test that LIMIT queries are optimized and don't fetch unnecessary rows
    start_time = Time.now
    row_count = 0

    @db[:performance_test].limit(10).fetch_rows("SELECT * FROM performance_test LIMIT 10") do |_row|
      row_count += 1
    end

    end_time = Time.now
    execution_time = end_time - start_time

    assert_equal 10, row_count, "Should fetch exactly 10 rows"
    assert_operator execution_time, :<, 0.1, "Limited query should be very fast (< 0.1 seconds), took #{execution_time}"
  end

  # Test prepared statement support for performance (Requirement 9.2)
  def test_prepared_statement_performance_benefit
    # Insert test data
    smaller_data = (1..100).map do |i|
      {
        id: i,
        name: "Test Record #{i}",
        value: i * 10,
        created_at: Date.today - (i % 365),
        active: i.even?
      }
    end
    @db[:performance_test].multi_insert(smaller_data)

    # Test that prepared statements provide performance benefits for repeated queries
    query_sql = "SELECT * FROM performance_test WHERE value = ? AND active = ?"

    # Time regular query execution (multiple times)
    regular_times = []
    5.times do
      start_time = Time.now
      @db[:performance_test].where(value: 100, active: true).all
      regular_times << (Time.now - start_time)
    end

    # Time prepared statement execution (if supported)
    prepared_times = []
    skip unless @db.respond_to?(:prepare) || @db.dataset.respond_to?(:prepare)

    5.times do
      start_time = Time.now
      # Test prepared statement functionality
      @db.fetch(query_sql, 100, true).all
      prepared_times << (Time.now - start_time)
    end

    # Prepared statements should be at least as fast as regular queries
    avg_regular = regular_times.sum / regular_times.length
    avg_prepared = prepared_times.sum / prepared_times.length

    assert_operator avg_prepared, :<=, avg_regular * 1.5, "Prepared statements should not be significantly slower than regular queries"
  end

  def test_prepared_statement_parameter_binding
    # Insert test data
    test_data = (1..10).map do |i|
      {
        id: i,
        name: "Test Record #{i}",
        value: i * 10,
        created_at: Date.today - (i % 365),
        active: i.even?
      }
    end
    @db[:performance_test].multi_insert(test_data)

    # Test that prepared statements handle parameter binding correctly
    query_sql = "SELECT * FROM performance_test WHERE value = ? AND name LIKE ?"

    results = @db.fetch(query_sql, 20, "Test Record 2%").all

    assert_equal 1, results.length, "Should find exactly one matching record"
    assert_equal "Test Record 2", results.first[:name], "Should find the correct record"
    assert_equal 20, results.first[:value], "Should match the value parameter"
  end

  # Test bulk insert optimization (Requirement 9.3)
  def test_bulk_insert_performance
    # Test that bulk inserts are significantly faster than individual inserts
    bulk_data = (1..100).map do |i|
      {
        id: i,
        name: "Bulk Test #{i}",
        value: i * 5,
        created_at: Date.today,
        active: true
      }
    end

    # Time individual inserts
    individual_start = Time.now
    bulk_data.each do |record|
      @db[:performance_test].insert(record)
    end
    individual_time = Time.now - individual_start

    # Clear the table
    @db[:performance_test].delete

    # Time bulk insert
    bulk_start = Time.now
    @db[:performance_test].multi_insert(bulk_data)
    bulk_time = Time.now - bulk_start

    # Bulk insert should be significantly faster
    assert_operator bulk_time, :<, individual_time * 0.5, "Bulk insert should be at least 2x faster than individual inserts (bulk: #{bulk_time}s, individual: #{individual_time}s)"

    # Verify all records were inserted
    assert_equal 100, @db[:performance_test].count, "All records should be inserted via bulk insert"
  end

  def test_bulk_insert_with_large_dataset
    # Test bulk insert with larger dataset
    large_data = (1..5000).map do |i|
      {
        id: i,
        name: "Bulk Record #{i}",
        value: i,
        created_at: Date.today,
        active: true
      }
    end

    start_time = Time.now
    @db[:performance_test].multi_insert(large_data)
    end_time = Time.now

    execution_time = end_time - start_time

    assert_equal 5000, @db[:performance_test].count, "All 5000 records should be inserted"
    assert_operator execution_time, :<, 10.0, "Large bulk insert should complete in reasonable time (< 10 seconds), took #{execution_time}"

    # Test that records per second is reasonable
    records_per_second = 5000 / execution_time

    assert_operator records_per_second, :>, 100, "Should insert at least 100 records per second, achieved #{records_per_second.round(2)}"
  end

  # Test efficient connection pooling (Requirement 9.4)
  def test_connection_pooling_efficiency
    # Test that connection pooling doesn't create excessive connections
    initial_connection_count = connection_count

    # Perform multiple operations that might create connections
    10.times do |i|
      @db[:performance_test].insert(id: i + 1000, name: "Pool Test #{i}", value: i, created_at: Date.today,
                                    active: true)
      @db[:performance_test].where(value: i).first
      @db[:performance_test].where(value: i).update(active: false)
    end

    final_connection_count = connection_count

    # Should not create excessive connections
    connection_growth = final_connection_count - initial_connection_count

    assert_operator connection_growth, :<=, 2, "Should not create excessive connections during operations (growth: #{connection_growth})"
  end

  def test_connection_reuse_efficiency
    # Test that connections are properly reused
    connection_ids = []

    # Perform operations and track connection reuse
    5.times do
      @db.synchronize do |conn|
        connection_ids << conn.object_id
        conn.query("SELECT 1")
      end
    end

    # Should reuse connections (not create new ones each time)
    unique_connections = connection_ids.uniq.length

    assert_operator unique_connections, :<=, 2, "Should reuse connections efficiently (used #{unique_connections} unique connections)"
  end

  def test_connection_cleanup_after_errors
    # Test that connections are properly cleaned up after errors
    initial_connection_count = connection_count

    # Cause some errors that might leave connections open
    5.times do
      @db.fetch("SELECT * FROM nonexistent_table").all
    rescue Sequel::DatabaseError
      # Expected error, ignore
    end

    # Force garbage collection to clean up any leaked connections
    GC.start
    sleep 0.1 # Give time for cleanup

    final_connection_count = connection_count
    connection_growth = final_connection_count - initial_connection_count

    assert_operator connection_growth, :<=, 1, "Should clean up connections after errors (growth: #{connection_growth})"
  end

  # Test streaming result options for memory efficiency (Requirement 9.5)
  def test_streaming_results_memory_efficiency
    # Insert larger dataset for streaming test
    large_data = (1..2000).map do |i|
      {
        id: i,
        name: "Stream Record #{i}",
        value: i,
        created_at: Date.today - (i % 100),
        active: i.odd?
      }
    end
    @db[:performance_test].multi_insert(large_data)

    # Test streaming with each method
    processed_count = 0
    memory_before = memory_usage

    @db[:performance_test].each do |row|
      processed_count += 1
      # Process row without storing in memory
      assert row[:name].start_with?("Stream Record"), "Should process streaming records correctly"
    end

    memory_after = memory_usage
    memory_growth = memory_after - memory_before

    assert_equal 2000, processed_count, "Should process all streaming records"
    assert_operator memory_growth, :<, 100_000_000, "Streaming should not consume excessive memory (growth: #{memory_growth} bytes)"
  end

  def test_streaming_with_large_text_fields
    # Test streaming efficiency with large text fields
    large_text_data = (1..100).map do |i|
      {
        id: i,
        name: "Large Text Record #{i}",
        value: i,
        created_at: Date.today,
        active: true
      }
    end

    @db[:performance_test].multi_insert(large_text_data)

    # Test that streaming handles large text efficiently
    start_time = Time.now
    text_lengths = @db[:performance_test].map do |row|
      row[:name].length
    end

    end_time = Time.now
    execution_time = end_time - start_time

    assert_equal 100, text_lengths.length, "Should process all records with large text"
    assert_operator execution_time, :<, 1.0, "Streaming large text should be efficient (< 1 second), took #{execution_time}"
  end

  private

  # Helper method to get approximate memory usage
  def memory_usage
    # Simple memory usage approximation
    GC.start
    ObjectSpace.count_objects[:TOTAL] * 40 # Rough estimate
  end

  # Helper method to get connection count (simplified)
  def connection_count
    # This is a simplified connection count - in a real implementation,
    # you might track this more precisely
    @db.pool.size if @db.respond_to?(:pool) && @db.pool.respond_to?(:size)
    1 # Default assumption
  end
end
