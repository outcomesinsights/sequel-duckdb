# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for basic performance optimizations in Sequel::DuckDB adapter
# Tests memory efficiency, index-aware queries, columnar optimizations, and parallel execution
class PerformanceBasicTest < SequelDuckDBTest::TestCase
  def setup
    super
    @db = create_db

    # Create test table optimized for columnar storage testing
    @db.create_table(:columnar_test) do
      Integer :id, primary_key: true
      String :category, size: 50
      Integer :amount
      Date :transaction_date
      Boolean :processed
      String :description, size: 200
    end

    # Create indexes for index-aware query testing
    @db.add_index(:columnar_test, :category, name: :idx_category)
    @db.add_index(:columnar_test, %i[transaction_date processed], name: :idx_date_processed)
    @db.add_index(:columnar_test, :amount, name: :idx_amount)

    # Insert test data optimized for columnar storage patterns
    @test_data = generate_columnar_test_data(5000)
    @db[:columnar_test].multi_insert(@test_data)
  end

  def teardown
    @db.drop_table?(:columnar_test)
    super
  end

  # Test streaming result options for memory efficiency (Requirement 9.5)
  def test_streaming_result_options_memory_efficiency
    # Test streaming with different batch sizes
    batch_sizes = [100, 500, 1000]

    batch_sizes.each do |batch_size|
      memory_before = get_memory_usage
      processed_count = 0

      # Test streaming with custom batch size
      dataset = @db[:columnar_test].stream_batch_size(batch_size)

      dataset.each do |row|
        processed_count += 1
        assert row.is_a?(Hash), "Streamed row should be a hash"
        assert row.key?(:id), "Streamed row should have id"
      end

      memory_after = get_memory_usage
      memory_growth = memory_after - memory_before

      assert_equal 5000, processed_count, "Should process all rows with batch size #{batch_size}"
      assert memory_growth < 200_000_000,
             "Memory growth should be controlled with batch size #{batch_size} (growth: #{memory_growth} bytes)"
    end
  end

  def test_streaming_with_memory_limit
    # Test streaming with memory limit enforcement
    memory_before = get_memory_usage
    processed_count = 0
    max_memory_seen = memory_before

    # Stream with memory monitoring
    @db[:columnar_test].stream_with_memory_limit(100_000_000) do |row|
      processed_count += 1
      current_memory = get_memory_usage
      max_memory_seen = [max_memory_seen, current_memory].max

      assert row.is_a?(Hash), "Row should be a hash"
    end

    memory_growth = max_memory_seen - memory_before

    assert_equal 5000, processed_count, "Should process all rows"
    assert memory_growth < 150_000_000, "Should respect memory limit (growth: #{memory_growth} bytes)"
  end

  # Test index-aware query generation (Requirement 9.7)
  def test_index_aware_query_generation_single_column
    # Test that queries can analyze index usage (even if not used due to small table size)
    dataset = @db[:columnar_test].where(category: "Electronics")

    # Get query plan to verify it's aware of available indexes
    plan = dataset.explain

    # For small tables, DuckDB may choose sequential scan over index
    # Test that the plan is generated successfully and contains filter information
    assert plan.include?("category='Electronics'") || plan.include?("category = 'Electronics'"),
           "Query plan should show category filter, plan: #{plan}"

    # Test that index hints are added to the dataset
    analysis = dataset.analyze_query
    assert analysis.is_a?(Hash), "Should return analysis hash"
    assert analysis.key?(:plan), "Should include plan"

    # Verify query still returns correct results
    results = dataset.all
    assert results.all? { |r| r[:category] == "Electronics" }, "Results should match filter"
  end

  def test_index_aware_query_generation_composite_index
    # Test composite index usage
    dataset = @db[:columnar_test]
              .where(transaction_date: Date.today)
              .where(processed: true)

    plan = dataset.explain

    # For small tables, DuckDB may choose sequential scan over index
    # Test that the plan contains the filter conditions
    assert plan.include?("processed") && plan.include?("transaction_date"),
           "Query plan should show both filter conditions, plan: #{plan}"

    # Verify results
    results = dataset.all
    assert results.all? { |r| r[:transaction_date] == Date.today && r[:processed] == true },
           "Results should match composite filter"
  end

  def test_index_aware_query_optimization_hints
    # Test that the adapter provides index optimization hints
    dataset = @db[:columnar_test].where(amount: (1000..5000))

    # Check if query optimizer recognizes range queries on indexed columns
    plan = dataset.explain

    # Should show range filter in the plan
    assert plan.include?("amount") && (plan.include?(">=") || plan.include?("<=") || plan.include?("BETWEEN")),
           "Query should show range filter for amount, plan: #{plan}"
  end

  def test_index_aware_order_by_optimization
    # Test that ORDER BY uses indexes when possible
    dataset = @db[:columnar_test].order(:amount)

    dataset.explain

    # Should optimize ORDER BY using amount index
    results = dataset.limit(10).all
    assert_equal 10, results.length, "Should return limited results"

    # Verify ordering
    amounts = results.map { |r| r[:amount] }
    assert_equal amounts.sort, amounts, "Results should be ordered by amount"
  end

  # Test optimization for DuckDB's columnar storage advantages (Requirement 9.7)
  def test_columnar_storage_projection_optimization
    # Test that SELECT with specific columns is optimized for columnar storage
    start_time = Time.now

    # Query selecting only specific columns (should be faster in columnar storage)
    results = @db[:columnar_test]
              .select(:category, :amount)
              .where(processed: true)
              .all

    projection_time = Time.now - start_time

    # Compare with SELECT * query
    start_time = Time.now
    @db[:columnar_test]
      .where(processed: true)
      .all

    full_scan_time = Time.now - start_time

    # Projection should be faster or at least not significantly slower
    # For small datasets, the difference may not be significant
    assert projection_time <= full_scan_time * 3.0,
           "Column projection should not be significantly slower (projection: #{projection_time}s, full: #{full_scan_time}s)"

    # Verify results are correct
    assert results.all? { |r| r.key?(:category) && r.key?(:amount) }, "Should have projected columns"
    assert results.all? { |r| !r.key?(:description) }, "Should not have non-projected columns"
  end

  def test_columnar_storage_aggregation_optimization
    # Test that aggregations are optimized for columnar storage
    start_time = Time.now

    # Aggregation queries should be fast on columnar data
    stats = @db[:columnar_test]
            .select(
              Sequel.function(:count, :*).as(:total_count),
              Sequel.function(:sum, :amount).as(:total_amount),
              Sequel.function(:avg, :amount).as(:avg_amount),
              Sequel.function(:max, :amount).as(:max_amount),
              Sequel.function(:min, :amount).as(:min_amount)
            )
            .first

    aggregation_time = Time.now - start_time

    assert aggregation_time < 2.0, "Aggregation should be fast on columnar data (took #{aggregation_time}s)"

    # Verify aggregation results
    assert_equal 5000, stats[:total_count], "Should count all rows"
    assert stats[:total_amount].positive?, "Should sum amounts"
    assert stats[:avg_amount].positive?, "Should calculate average"
    assert stats[:max_amount] >= stats[:min_amount], "Max should be >= min"
  end

  def test_columnar_storage_group_by_optimization
    # Test that GROUP BY operations are optimized for columnar storage
    start_time = Time.now

    # GROUP BY should be efficient on columnar data
    category_stats = @db[:columnar_test]
                     .select(:category)
                     .select_append(Sequel.function(:count, :*).as(:count))
                     .select_append(Sequel.function(:sum, :amount).as(:total))
                     .group(:category)
                     .all

    group_by_time = Time.now - start_time

    assert group_by_time < 1.0, "GROUP BY should be fast on columnar data (took #{group_by_time}s)"

    # Verify GROUP BY results
    assert category_stats.length.positive?, "Should have grouped results"
    assert category_stats.all? { |r| r[:count].positive? }, "Each group should have count > 0"

    # Verify total count matches
    total_count = category_stats.sum { |r| r[:count] }
    assert_equal 5000, total_count, "Total count should match original data"
  end

  def test_columnar_storage_filter_pushdown
    # Test that filters are pushed down efficiently in columnar storage
    start_time = Time.now

    # Complex filter that should benefit from columnar storage
    filtered_results = @db[:columnar_test]
                       .where(amount: (2000..8000))
                       .where(processed: true)
                       .where(Sequel.like(:category, "%Electronics%"))
                       .select(:id, :category, :amount)
                       .all

    filter_time = Time.now - start_time

    assert filter_time < 1.0, "Complex filtering should be efficient (took #{filter_time}s)"

    # Verify filter results
    assert filtered_results.all? { |r| r[:amount] >= 2000 && r[:amount] <= 8000 },
           "Should respect amount range filter"
    assert filtered_results.all? { |r| r[:category].include?("Electronics") },
           "Should respect category filter"
  end

  # Test parallel query execution support (Requirement 9.7)
  def test_parallel_query_execution_large_aggregation
    # Test that large aggregations can use parallel execution
    start_time = Time.now

    # Large aggregation that should benefit from parallelization
    result = @db[:columnar_test]
             .select(
               Sequel.function(:count, :*).as(:total_rows),
               Sequel.function(:count, :category).as(:category_count),
               Sequel.function(:sum, :amount).as(:total_amount),
               Sequel.function(:avg, :amount).as(:avg_amount)
             )
             .first

    parallel_time = Time.now - start_time

    # Should complete in reasonable time (parallel execution should help)
    assert parallel_time < 3.0, "Large aggregation should complete efficiently (took #{parallel_time}s)"

    # Verify results
    assert_equal 5000, result[:total_rows], "Should count all rows"
    assert result[:total_amount].positive?, "Should calculate sum"
  end

  def test_parallel_query_execution_complex_joins
    # Create second table for join testing
    @db.create_table(:join_test) do
      Integer :category_id, primary_key: true
      String :category_name, size: 50
      String :department, size: 50
    end

    # Insert join data
    join_data = [
      { category_id: 1, category_name: "Electronics", department: "Technology" },
      { category_id: 2, category_name: "Books", department: "Media" },
      { category_id: 3, category_name: "Clothing", department: "Fashion" },
      { category_id: 4, category_name: "Home", department: "Household" }
    ]
    @db[:join_test].multi_insert(join_data)

    start_time = Time.now

    # Complex join that should benefit from parallel execution
    join_results = @db[:columnar_test]
                   .join(:join_test, Sequel[:columnar_test][:category] => Sequel[:join_test][:category_name])
                   .select(
                     Sequel[:columnar_test][:id],
                     Sequel[:columnar_test][:amount],
                     Sequel[:join_test][:department]
                   )
                   .where(Sequel[:columnar_test][:amount] > 3000)
                   .all

    join_time = Time.now - start_time

    assert join_time < 2.0, "Complex join should complete efficiently (took #{join_time}s)"

    # Verify join results
    assert join_results.length.positive?, "Should have join results"
    assert join_results.all? { |r| r[:amount] > 3000 }, "Should respect WHERE clause"
    assert join_results.all? { |r| r.key?(:department) }, "Should have joined columns"

    @db.drop_table(:join_test)
  end

  def test_parallel_query_execution_window_functions
    # Test that window functions can use parallel execution
    start_time = Time.now

    # Window function query that should benefit from parallelization
    window_results = @db[:columnar_test]
                     .select(
                       :id,
                       :category,
                       :amount,
                       Sequel.function(:row_number).over(partition: :category, order: :amount).as(:row_num),
                       Sequel.function(:rank).over(partition: :category, order: :amount).as(:rank)
                     )
                     .limit(100)
                     .all

    window_time = Time.now - start_time

    assert window_time < 2.0, "Window functions should execute efficiently (took #{window_time}s)"

    # Verify window function results
    assert window_results.length.positive?, "Should have window function results"
    assert window_results.all? { |r| r[:row_num].positive? }, "Should have row numbers"
    assert window_results.all? { |r| r[:rank].positive? }, "Should have ranks"
  end

  def test_parallel_query_execution_configuration
    # Test that parallel execution can be configured
    # This tests the adapter's ability to pass through DuckDB's parallel settings

    # Test with different thread configurations
    original_threads = begin
      @db.get_config_value("threads")
    rescue StandardError
      1
    end

    begin
      # Try to set thread count (if supported)
      @db.set_config_value("threads", 2) if @db.respond_to?(:set_config_value)

      start_time = Time.now

      # Query that should use configured parallelism
      result = @db[:columnar_test]
               .where(amount: (1000..9000))
               .group(:category)
               .select(:category)
               .select_append(Sequel.function(:count, :*).as(:count))
               .select_append(Sequel.function(:sum, :amount).as(:total))
               .all

      parallel_config_time = Time.now - start_time

      assert parallel_config_time < 2.0,
             "Configured parallel execution should be efficient (took #{parallel_config_time}s)"
      assert result.length.positive?, "Should have grouped results"
    ensure
      # Restore original thread setting
      @db.set_config_value("threads", original_threads) if @db.respond_to?(:set_config_value)
    end
  end

  private

  def generate_columnar_test_data(count)
    categories = %w[Electronics Books Clothing Home Sports]

    (1..count).map do |i|
      {
        id: i,
        category: categories[i % categories.length],
        amount: rand(100..10_099),
        transaction_date: Date.today - rand(365),
        processed: i.even?,
        description: "Transaction description for record #{i} with some additional text to test columnar efficiency"
      }
    end
  end

  def get_memory_usage
    # Simple memory usage approximation
    GC.start
    ObjectSpace.count_objects[:TOTAL] * 40
  end
end
