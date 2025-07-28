# frozen_string_literal: true

require_relative "spec_helper"

# Test suite for advanced SQL generation features (TDD - Red phase)
# Tests complex query support and DuckDB-specific SQL features
# These tests are written BEFORE implementation to follow TDD methodology
class AdvancedSqlGenerationTest < SequelDuckDBTest::TestCase
  # Tests for complex WHERE clause generation (Requirement 6.4)
  def test_where_with_complex_conditions
    dataset = mock_dataset(:users).where { (age > 18) & (active =~ true) }
    expected_sql = "SELECT * FROM users WHERE ((age > 18) AND (active IS TRUE))"
    assert_sql expected_sql, dataset
  end

  def test_where_with_or_conditions
    dataset = mock_dataset(:users).where(Sequel.|({ name: "John" }, { name: "Jane" }))
    expected_sql = "SELECT * FROM users WHERE ((name = 'John') OR (name = 'Jane'))"
    assert_sql expected_sql, dataset
  end

  def test_where_with_in_conditions
    dataset = mock_dataset(:users).where(name: %w[John Jane Bob])
    expected_sql = "SELECT * FROM users WHERE (name IN ('John', 'Jane', 'Bob'))"
    assert_sql expected_sql, dataset
  end

  def test_where_with_range_conditions
    dataset = mock_dataset(:users).where(age: 18..65)
    expected_sql = "SELECT * FROM users WHERE ((age >= 18) AND (age <= 65))"
    assert_sql expected_sql, dataset
  end

  def test_where_with_null_conditions
    dataset = mock_dataset(:users).where(email: nil)
    expected_sql = "SELECT * FROM users WHERE (email IS NULL)"
    assert_sql expected_sql, dataset
  end

  def test_where_with_not_null_conditions
    dataset = mock_dataset(:users).exclude(email: nil)
    expected_sql = "SELECT * FROM users WHERE (email IS NOT NULL)"
    assert_sql expected_sql, dataset
  end

  def test_where_with_like_conditions
    dataset = mock_dataset(:users).where(Sequel.like(:name, "%John%"))
    expected_sql = "SELECT * FROM users WHERE (name LIKE '%John%')"
    assert_sql expected_sql, dataset
  end

  def test_where_with_ilike_conditions
    dataset = mock_dataset(:users).where(Sequel.ilike(:name, "%john%"))
    expected_sql = "SELECT * FROM users WHERE (UPPER(name) LIKE UPPER('%john%'))"
    assert_sql expected_sql, dataset
  end

  def test_where_with_regexp_conditions
    dataset = mock_dataset(:users).where(name: /^John/)
    expected_sql = "SELECT * FROM users WHERE (regexp_matches(name, '^John'))"
    assert_sql expected_sql, dataset
  end

  def test_where_with_nested_conditions
    dataset = mock_dataset(:users).where { ((age > 18) & (age < 65)) | (status =~ "admin") }
    expected_sql = "SELECT * FROM users WHERE (((age > 18) AND (age < 65)) OR (status = 'admin'))"
    assert_sql expected_sql, dataset
  end

  # Tests for ORDER BY clause generation (Requirement 6.5)
  def test_order_by_single_column
    dataset = mock_dataset(:users).order(:name)
    expected_sql = "SELECT * FROM users ORDER BY name"
    assert_sql expected_sql, dataset
  end

  def test_order_by_multiple_columns
    dataset = mock_dataset(:users).order(:name, :age)
    expected_sql = "SELECT * FROM users ORDER BY name, age"
    assert_sql expected_sql, dataset
  end

  def test_order_by_desc
    dataset = mock_dataset(:users).order(Sequel.desc(:name))
    expected_sql = "SELECT * FROM users ORDER BY name DESC"
    assert_sql expected_sql, dataset
  end

  def test_order_by_asc_explicit
    dataset = mock_dataset(:users).order(Sequel.asc(:name))
    expected_sql = "SELECT * FROM users ORDER BY name ASC"
    assert_sql expected_sql, dataset
  end

  def test_order_by_mixed_directions
    dataset = mock_dataset(:users).order(Sequel.desc(:name), Sequel.asc(:age))
    expected_sql = "SELECT * FROM users ORDER BY name DESC, age ASC"
    assert_sql expected_sql, dataset
  end

  def test_order_by_with_nulls_first
    dataset = mock_dataset(:users).order(Sequel.desc(:name, nulls: :first))
    expected_sql = "SELECT * FROM users ORDER BY name DESC NULLS FIRST"
    assert_sql expected_sql, dataset
  end

  def test_order_by_with_nulls_last
    dataset = mock_dataset(:users).order(Sequel.asc(:name, nulls: :last))
    expected_sql = "SELECT * FROM users ORDER BY name ASC NULLS LAST"
    assert_sql expected_sql, dataset
  end

  def test_order_by_with_expressions
    dataset = mock_dataset(:users).order(Sequel.lit("LENGTH(name)"))
    expected_sql = "SELECT * FROM users ORDER BY LENGTH(name)"
    assert_sql expected_sql, dataset
  end

  # Tests for LIMIT and OFFSET support (Requirement 6.6)
  def test_limit_only
    dataset = mock_dataset(:users).limit(10)
    expected_sql = "SELECT * FROM users LIMIT 10"
    assert_sql expected_sql, dataset
  end

  def test_limit_with_offset
    dataset = mock_dataset(:users).limit(10, 20)
    expected_sql = "SELECT * FROM users LIMIT 10 OFFSET 20"
    assert_sql expected_sql, dataset
  end

  def test_offset_only
    dataset = mock_dataset(:users).offset(20)
    expected_sql = "SELECT * FROM users OFFSET 20"
    assert_sql expected_sql, dataset
  end

  def test_limit_one
    dataset = mock_dataset(:users).limit(1)
    expected_sql = "SELECT * FROM users LIMIT 1"
    assert_sql expected_sql, dataset
  end

  def test_limit_with_large_numbers
    dataset = mock_dataset(:users).limit(1_000_000, 5_000_000)
    expected_sql = "SELECT * FROM users LIMIT 1000000 OFFSET 5000000"
    assert_sql expected_sql, dataset
  end

  # Tests for GROUP BY clause generation (Requirement 6.7)
  def test_group_by_single_column
    dataset = mock_dataset(:users).select(:name, Sequel.function(:count, :*)).group(:name)
    expected_sql = "SELECT name, count(*) FROM users GROUP BY name"
    assert_sql expected_sql, dataset
  end

  def test_group_by_multiple_columns
    dataset = mock_dataset(:users).select(:name, :age, Sequel.function(:count, :*)).group(:name, :age)
    expected_sql = "SELECT name, age, count(*) FROM users GROUP BY name, age"
    assert_sql expected_sql, dataset
  end

  def test_group_by_with_expressions
    dataset = mock_dataset(:users).select(Sequel.lit("YEAR(created_at)"),
                                          Sequel.function(:count, :*)).group(Sequel.lit("YEAR(created_at)"))
    expected_sql = "SELECT YEAR(created_at), count(*) FROM users GROUP BY YEAR(created_at)"
    assert_sql expected_sql, dataset
  end

  def test_group_by_with_having
    dataset = mock_dataset(:users)
              .select(:name, Sequel.function(:count, :*))
              .group(:name)
              .having { count(:*) > 1 }
    expected_sql = "SELECT name, count(*) FROM users GROUP BY name HAVING (count(*) > 1)"
    assert_sql expected_sql, dataset
  end

  # Tests for HAVING clause generation (Requirement 6.8)
  def test_having_with_aggregate_function
    dataset = mock_dataset(:users)
              .select(:name, Sequel.function(:count, :*))
              .group(:name)
              .having { count(:*) > 5 }
    expected_sql = "SELECT name, count(*) FROM users GROUP BY name HAVING (count(*) > 5)"
    assert_sql expected_sql, dataset
  end

  def test_having_with_multiple_conditions
    dataset = mock_dataset(:users)
              .select(:name, Sequel.function(:count, :*), Sequel.function(:avg, :age))
              .group(:name)
              .having { (count(:*) > 1) & (avg(:age) > 25) }
    expected_sql = "SELECT name, count(*), avg(age) FROM users GROUP BY name HAVING ((count(*) > 1) AND (avg(age) > 25))"
    assert_sql expected_sql, dataset
  end

  def test_having_with_or_conditions
    dataset = mock_dataset(:users)
              .select(:name, Sequel.function(:count, :*))
              .group(:name)
              .having { (count(:*) > 10) | (count(:*) < 2) }
    expected_sql = "SELECT name, count(*) FROM users GROUP BY name HAVING ((count(*) > 10) OR (count(*) < 2))"
    assert_sql expected_sql, dataset
  end

  def test_having_with_sum_function
    dataset = mock_dataset(:orders)
              .select(:customer_id, Sequel.function(:sum, :amount))
              .group(:customer_id)
              .having { sum(:amount) > 1000 }
    expected_sql = "SELECT customer_id, sum(amount) FROM orders GROUP BY customer_id HAVING (sum(amount) > 1000)"
    assert_sql expected_sql, dataset
  end

  # Tests for JOIN statement generation (Requirement 6.9)
  def test_inner_join
    dataset = mock_dataset(:users).join(:profiles, user_id: :id)
    expected_sql = "SELECT * FROM users INNER JOIN profiles ON (profiles.user_id = users.id)"
    assert_sql expected_sql, dataset
  end

  def test_left_join
    dataset = mock_dataset(:users).left_join(:profiles, user_id: :id)
    expected_sql = "SELECT * FROM users LEFT JOIN profiles ON (profiles.user_id = users.id)"
    assert_sql expected_sql, dataset
  end

  def test_right_join
    dataset = mock_dataset(:users).right_join(:profiles, user_id: :id)
    expected_sql = "SELECT * FROM users RIGHT JOIN profiles ON (profiles.user_id = users.id)"
    assert_sql expected_sql, dataset
  end

  def test_full_join
    dataset = mock_dataset(:users).full_join(:profiles, user_id: :id)
    expected_sql = "SELECT * FROM users FULL JOIN profiles ON (profiles.user_id = users.id)"
    assert_sql expected_sql, dataset
  end

  def test_cross_join
    dataset = mock_dataset(:users).cross_join(:categories)
    expected_sql = "SELECT * FROM users CROSS JOIN categories"
    assert_sql expected_sql, dataset
  end

  def test_join_with_multiple_conditions
    dataset = mock_dataset(:users).join(:profiles, { user_id: :id, active: true })
    expected_sql = "SELECT * FROM users INNER JOIN profiles ON ((profiles.user_id = users.id) AND (profiles.active IS TRUE))"
    assert_sql expected_sql, dataset
  end

  def test_multiple_joins
    dataset = mock_dataset(:users)
              .join(:profiles, user_id: :id)
              .join(:orders, user_id: Sequel[:users][:id])
    expected_sql = "SELECT * FROM users INNER JOIN profiles ON (profiles.user_id = users.id) INNER JOIN orders ON (orders.user_id = users.id)"
    assert_sql expected_sql, dataset
  end

  def test_join_with_complex_conditions
    dataset = mock_dataset(:users).join(:profiles, user_id: :id).where(profiles__active: true)
    expected_sql = "SELECT * FROM users INNER JOIN profiles ON (profiles.user_id = users.id) WHERE (profiles.active IS TRUE)"
    assert_sql expected_sql, dataset
  end

  # Tests for DuckDB-specific SQL features

  # Tests for JOIN syntax (Requirement 2.6)
  def test_natural_join
    dataset = mock_dataset(:users).natural_join(:profiles)
    expected_sql = "SELECT * FROM users NATURAL JOIN profiles"
    assert_sql expected_sql, dataset
  end

  def test_join_using_clause
    dataset = mock_dataset(:users).join(:profiles, nil, using: :user_id)
    expected_sql = "SELECT * FROM users INNER JOIN profiles USING (user_id)"
    assert_sql expected_sql, dataset
  end

  def test_join_with_subquery
    subquery = mock_dataset(:orders).select(:user_id, Sequel.function(:count, :*).as(:order_count)).group(:user_id)
    dataset = mock_dataset(:users).join(subquery.as(:order_stats), user_id: :id)
    expected_sql = "SELECT * FROM users INNER JOIN (SELECT user_id, count(*) AS order_count FROM orders GROUP BY user_id) AS order_stats ON (order_stats.user_id = users.id)"
    assert_sql expected_sql, dataset
  end

  # Tests for subquery generation (Requirement 2.7)
  def test_subquery_in_select
    subquery = mock_dataset(:orders).select(Sequel.function(:count, :*)).where(user_id: :users__id)
    dataset = mock_dataset(:users).select(:name, subquery.as(:order_count))
    expected_sql = "SELECT name, (SELECT count(*) FROM orders WHERE (user_id = users.id)) AS order_count FROM users"
    assert_sql expected_sql, dataset
  end

  def test_subquery_in_where
    subquery = mock_dataset(:orders).select(:user_id).where { amount > 1000 }
    dataset = mock_dataset(:users).where(id: subquery)
    expected_sql = "SELECT * FROM users WHERE (id IN (SELECT user_id FROM orders WHERE (amount > 1000)))"
    assert_sql expected_sql, dataset
  end

  def test_exists_subquery
    subquery = mock_dataset(:orders).where(user_id: :users__id)
    dataset = mock_dataset(:users).where(subquery.exists)
    expected_sql = "SELECT * FROM users WHERE (EXISTS (SELECT * FROM orders WHERE (user_id = users.id)))"
    assert_sql expected_sql, dataset
  end

  def test_not_exists_subquery
    subquery = mock_dataset(:orders).where(user_id: :users__id)
    dataset = mock_dataset(:users).exclude(subquery.exists)
    expected_sql = "SELECT * FROM users WHERE NOT (EXISTS (SELECT * FROM orders WHERE (user_id = users.id)))"
    assert_sql expected_sql, dataset
  end

  def test_scalar_subquery
    subquery = mock_dataset(:orders).select(Sequel.function(:max, :amount)).where(user_id: :users__id)
    dataset = mock_dataset(:users).where { credit_limit > subquery }
    expected_sql = "SELECT * FROM users WHERE (credit_limit > (SELECT max(amount) FROM orders WHERE (user_id = users.id)))"
    assert_sql expected_sql, dataset
  end

  # Tests for aggregate function support (Requirement 2.8)
  def test_count_function
    dataset = mock_dataset(:users).select(Sequel.function(:count, :*))
    expected_sql = "SELECT count(*) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_count_distinct
    dataset = mock_dataset(:users).select(Sequel.function(:count, Sequel.function(:distinct, :name)))
    expected_sql = "SELECT count(distinct(name)) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_sum_function
    dataset = mock_dataset(:orders).select(Sequel.function(:sum, :amount))
    expected_sql = "SELECT sum(amount) FROM orders"
    assert_sql expected_sql, dataset
  end

  def test_avg_function
    dataset = mock_dataset(:users).select(Sequel.function(:avg, :age))
    expected_sql = "SELECT avg(age) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_min_max_functions
    dataset = mock_dataset(:users).select(Sequel.function(:min, :age), Sequel.function(:max, :age))
    expected_sql = "SELECT min(age), max(age) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_string_aggregation
    dataset = mock_dataset(:users).select(Sequel.function(:string_agg, :name, ", "))
    expected_sql = "SELECT string_agg(name, ', ') FROM users"
    assert_sql expected_sql, dataset
  end

  def test_array_aggregation
    dataset = mock_dataset(:users).select(Sequel.function(:array_agg, :name))
    expected_sql = "SELECT array_agg(name) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_statistical_functions
    dataset = mock_dataset(:users).select(
      Sequel.function(:stddev, :age),
      Sequel.function(:variance, :age)
    )
    expected_sql = "SELECT stddev(age), variance(age) FROM users"
    assert_sql expected_sql, dataset
  end

  # Tests for window functions (DuckDB-specific feature)
  def test_row_number_window_function
    dataset = mock_dataset(:users).select(:name, Sequel.function(:row_number).over(order: :name))
    expected_sql = "SELECT name, row_number() OVER (ORDER BY name) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_rank_window_function
    dataset = mock_dataset(:users).select(:name, :age, Sequel.function(:rank).over(order: :age))
    expected_sql = "SELECT name, age, rank() OVER (ORDER BY age) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_window_function_with_partition
    dataset = mock_dataset(:users).select(
      :name, :department,
      Sequel.function(:row_number).over(partition: :department, order: :name)
    )
    expected_sql = "SELECT name, department, row_number() OVER (PARTITION BY department ORDER BY name) FROM users"
    assert_sql expected_sql, dataset
  end

  def test_lag_lead_window_functions
    dataset = mock_dataset(:users).select(
      :name, :age,
      Sequel.function(:lag, :age, 1).over(order: :age).as(:prev_age),
      Sequel.function(:lead, :age, 1).over(order: :age).as(:next_age)
    )
    expected_sql = "SELECT name, age, lag(age, 1) OVER (ORDER BY age) AS prev_age, lead(age, 1) OVER (ORDER BY age) AS next_age FROM users"
    assert_sql expected_sql, dataset
  end

  # Tests for Common Table Expressions (CTE)
  def test_simple_cte
    cte = mock_dataset(:users).select(:name, :age).where { age > 18 }
    dataset = mock_dataset.with(:adults, cte).from(:adults)
    expected_sql = "WITH adults AS (SELECT name, age FROM users WHERE (age > 18)) SELECT * FROM adults"
    assert_sql expected_sql, dataset
  end

  def test_multiple_ctes
    cte1 = mock_dataset(:users).select(:name, :age).where { age > 18 }
    cte2 = mock_dataset(:orders).select(:user_id, Sequel.function(:sum, :amount).as(:total)).group(:user_id)
    dataset = mock_dataset.with(:adults, cte1).with(:user_totals, cte2).from(:adults).join(:user_totals, user_id: :id)
    expected_sql = "WITH adults AS (SELECT name, age FROM users WHERE (age > 18)), user_totals AS (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id) SELECT * FROM adults INNER JOIN user_totals ON (user_totals.user_id = adults.id)"
    assert_sql expected_sql, dataset
  end

  def test_recursive_cte
    base_case = SequelDuckDBTest::MOCK_DB.select(Sequel.as(1, :n))
    recursive_case = mock_dataset(:t).select(Sequel.lit("n + 1")).where { n < 10 }
    dataset = mock_dataset.with_recursive(:t, base_case, recursive_case).from(:t)
    expected_sql = "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE (n < 10)) SELECT * FROM t"
    assert_sql expected_sql, dataset
  end

  def test_regular_cte_still_works
    cte = mock_dataset.select(:name, :age).where { age > 18 }
    dataset = mock_dataset.with(:adults, cte).from(:adults)
    expected_sql = "WITH adults AS (SELECT name, age FROM test_table WHERE (age > 18)) SELECT * FROM adults"
    assert_sql expected_sql, dataset
  end

  def test_mixed_regular_and_recursive_ctes
    # Test that we can have both regular and recursive CTEs in the same query
    regular_cte = mock_dataset(:users).select(:id, :name).where { active =~ true }
    base_case = SequelDuckDBTest::MOCK_DB.select(Sequel.as(1, :level))
    recursive_case = mock_dataset(:levels).select(Sequel.lit("level + 1")).where { level < 3 }

    dataset = mock_dataset
              .with(:active_users, regular_cte)
              .with_recursive(:levels, base_case, recursive_case)
              .from(:active_users)
              .cross_join(:levels)

    expected_sql = "WITH RECURSIVE active_users AS (SELECT id, name FROM users WHERE (active IS TRUE)), levels AS (SELECT 1 AS level UNION ALL SELECT level + 1 FROM levels WHERE (level < 3)) SELECT * FROM active_users CROSS JOIN levels"
    assert_sql expected_sql, dataset
  end

  def test_recursive_cte_with_complex_base_case
    # Test recursive CTE with more complex base case
    base_case = mock_dataset(:employees).select(:id, :name, :manager_id, Sequel.as(0, :depth)).where(manager_id: nil)
    recursive_case = mock_dataset(:employees).select(
      Sequel.qualify(:e, :id),
      Sequel.qualify(:e, :name),
      Sequel.qualify(:e, :manager_id),
      Sequel.lit("org_chart.depth + 1")
    ).from(Sequel.as(:employees, :e))
                                             .join(:org_chart, id: :manager_id)

    dataset = mock_dataset.with_recursive(:org_chart, base_case, recursive_case)
                          .from(:org_chart)
                          .where { depth <= 3 }
                          .order(:depth, :name)

    expected_sql = "WITH RECURSIVE org_chart AS (SELECT id, name, manager_id, 0 AS depth FROM employees WHERE (manager_id IS NULL) UNION ALL SELECT e.id, e.name, e.manager_id, org_chart.depth + 1 FROM employees AS e INNER JOIN org_chart ON (org_chart.id = e.manager_id)) SELECT * FROM org_chart WHERE (depth <= 3) ORDER BY depth, name"
    assert_sql expected_sql, dataset
  end

  # Tests for complex combined queries
  def test_complex_query_with_all_features
    dataset = mock_dataset(:users)
              .select(:name, :age, Sequel.function(:count, :id).as(:order_count))
              .left_join(:orders, user_id: :id)
              .where { (age > 18) & (active =~ true) }
              .group(:id, :name, :age)
              .having { count(:id) > 0 }
              .order(Sequel.desc(:order_count), :name)
              .limit(10, 5)

    expected_sql = "SELECT name, age, count(id) AS order_count FROM users LEFT JOIN orders ON (orders.user_id = users.id) WHERE ((age > 18) AND (active IS TRUE)) GROUP BY id, name, age HAVING (count(id) > 0) ORDER BY order_count DESC, name LIMIT 10 OFFSET 5"
    assert_sql expected_sql, dataset
  end

  def test_union_queries
    dataset1 = mock_dataset(:users).select(:name, :email).where { age > 18 }
    dataset2 = mock_dataset(:admins).select(:name, :email)
    dataset = dataset1.union(dataset2)
    expected_sql = "SELECT name, email FROM users WHERE (age > 18) UNION SELECT name, email FROM admins"
    assert_sql expected_sql, dataset
  end

  def test_union_all_queries
    dataset1 = mock_dataset(:users).select(:name)
    dataset2 = mock_dataset(:admins).select(:name)
    dataset = dataset1.union(dataset2, all: true)
    expected_sql = "SELECT name FROM users UNION ALL SELECT name FROM admins"
    assert_sql expected_sql, dataset
  end

  def test_intersect_queries
    dataset1 = mock_dataset(:users).select(:email)
    dataset2 = mock_dataset(:subscribers).select(:email)
    dataset = dataset1.intersect(dataset2)
    expected_sql = "SELECT email FROM users INTERSECT SELECT email FROM subscribers"
    assert_sql expected_sql, dataset
  end

  def test_except_queries
    dataset1 = mock_dataset(:users).select(:email)
    dataset2 = mock_dataset(:unsubscribed).select(:email)
    dataset = dataset1.except(dataset2)
    expected_sql = "SELECT email FROM users EXCEPT SELECT email FROM unsubscribed"
    assert_sql expected_sql, dataset
  end
end
