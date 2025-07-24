# frozen_string_literal: true

require_relative "spec_helper"

describe "Sequel::Model integration with DuckDB adapter" do
  def create_sql_logger(pattern)
    logger = Object.new
    logger.define_singleton_method(:info) do |sql|
      @captured_sql = sql if sql.include?(pattern)
    end
    logger.define_singleton_method(:captured_sql) { @captured_sql }
    logger
  end

  def next_user_id
    @user_id_counter += 1
  end

  def next_post_id
    @post_id_counter += 1
  end

  before do
    @db = Sequel.connect("duckdb::memory:")
    @db.create_table(:users) do
      Integer :id, primary_key: true
      String :name, null: false
      String :email
      Integer :age
      Boolean :active, default: true
      DateTime :created_at
    end

    @db.create_table(:posts) do
      Integer :id, primary_key: true
      foreign_key :user_id, :users, null: false
      String :title, null: false
      String :content
      DateTime :created_at
    end

    # Initialize ID counters for manual ID management
    @user_id_counter = 0
    @post_id_counter = 0
  end

  after do
    @db.disconnect
  end

  describe "automatic schema introspection" do
    it "should automatically introspect table schema for models" do
      # Define a model class
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
      end

      # Model should automatically introspect the schema
      schema = user_class.db_schema

      # Verify schema introspection worked correctly
      assert schema.key?(:id), "Schema should include id column"
      assert schema.key?(:name), "Schema should include name column"
      assert schema.key?(:email), "Schema should include email column"
      assert schema.key?(:age), "Schema should include age column"
      assert schema.key?(:active), "Schema should include active column"
      assert schema.key?(:created_at), "Schema should include created_at column"

      # Verify column types are correctly mapped
      assert_equal :integer, schema[:id][:type], "ID should be integer type"
      assert_equal :string, schema[:name][:type], "Name should be string type"
      assert_equal :string, schema[:email][:type], "Email should be string type"
      assert_equal :integer, schema[:age][:type], "Age should be integer type"
      assert_equal :boolean, schema[:active][:type], "Active should be boolean type"
      assert_equal :datetime, schema[:created_at][:type], "Created_at should be datetime type"

      # Verify primary key is identified
      assert schema[:id][:primary_key], "ID should be identified as primary key"
      refute schema[:name][:primary_key], "Name should not be primary key"

      # Verify null constraints
      refute schema[:id][:allow_null], "ID should not allow null"
      refute schema[:name][:allow_null], "Name should not allow null"
      assert schema[:email][:allow_null], "Email should allow null"

      # Verify default values
      assert_equal true, schema[:active][:default], "Active should have default value true"
    end

    it "should work with models that have foreign key relationships" do
      # Define model classes with associations
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        one_to_many :posts
      end

      post_class = Class.new(Sequel::Model(@db[:posts])) do
        def self.name
          "Post"
        end
        many_to_one :user
      end

      # Verify schema introspection for table with foreign key
      post_schema = post_class.db_schema

      assert post_schema.key?(:user_id), "Post schema should include user_id foreign key"
      assert_equal :integer, post_schema[:user_id][:type], "user_id should be integer type"
      refute post_schema[:user_id][:allow_null], "user_id should not allow null"
    end

    it "should handle models with complex column types" do
      # Create table with various DuckDB-specific types
      @db.create_table(:complex_table) do
        Integer :id, primary_key: true
        BigDecimal :price, size: [10, 2]
        Date :birth_date
        Time :wake_time
        column :data, :blob
      end

      complex_class = Class.new(Sequel::Model(@db[:complex_table])) do
        def self.name
          "ComplexModel"
        end
      end

      schema = complex_class.db_schema

      # Verify complex types are properly mapped
      assert_equal :decimal, schema[:price][:type], "Price should be decimal type"
      assert_equal :date, schema[:birth_date][:type], "Birth_date should be date type"
      assert_equal :time, schema[:wake_time][:type], "Wake_time should be time type"
      assert_equal :blob, schema[:data][:type], "Data should be blob type"
    end
  end

  describe "INSERT statement generation for model creation" do
    it "should generate proper INSERT statements when creating model instances" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
      end

      # Test INSERT statement generation by creating a model instance
      user = user_class.new(
        id: next_user_id,
        name: "John Doe",
        email: "john@example.com",
        age: 30,
        active: true,
        created_at: Time.new(2023, 1, 1, 12, 0, 0)
      )

      # Capture the SQL that will be generated
      insert_sql = nil
      logger = Object.new
      def logger.info(sql)
        @captured_sql = sql if sql.include?("INSERT")
      end
      def logger.captured_sql
        @captured_sql
      end
      @db.loggers << logger

      # Save the model instance
      user.save

      # Verify INSERT SQL was generated correctly
      insert_sql = logger.captured_sql
      assert insert_sql, "INSERT SQL should have been generated"
      assert insert_sql.include?("INSERT INTO users"), "Should insert into users table"
      assert insert_sql.include?("name"), "Should include name column"
      assert insert_sql.include?("email"), "Should include email column"
      assert insert_sql.include?("age"), "Should include age column"
      assert insert_sql.include?("active"), "Should include active column"
      assert insert_sql.include?("created_at"), "Should include created_at column"

      # Verify the record was actually inserted
      assert user.id, "User should have an ID after saving"

      # Verify data can be retrieved
      retrieved_user = user_class[user.id]
      assert_equal "John Doe", retrieved_user.name
      assert_equal "john@example.com", retrieved_user.email
      assert_equal 30, retrieved_user.age
      assert_equal true, retrieved_user.active
    end

    it "should handle INSERT with only required fields" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
      end

      # Create user with only required field
      user = user_class.new(id: next_user_id, name: "Jane Doe")

      logger = create_sql_logger("INSERT")
      @db.loggers << logger

      user.save

      # Verify INSERT SQL handles optional fields correctly
      insert_sql = logger.captured_sql
      assert insert_sql, "INSERT SQL should have been generated"
      assert insert_sql.include?("INSERT INTO users"), "Should insert into users table"
      assert insert_sql.include?("name"), "Should include name column"

      # Verify the record was inserted with defaults
      assert user.id, "User should have an ID after saving"
      retrieved_user = user_class[user.id]
      assert_equal "Jane Doe", retrieved_user.name
      assert_equal true, retrieved_user.active, "Should use default value for active"
    end

    it "should handle INSERT with foreign key relationships" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
        one_to_many :posts
      end

      post_class = Class.new(Sequel::Model(@db[:posts])) do
        def self.name
          "Post"
        end
        unrestrict_primary_key
        many_to_one :user
      end

      # Create user first
      user = user_class.create(id: next_user_id, name: "Author", email: "author@example.com")

      # Create post with foreign key
      post = post_class.new(
        id: next_post_id,
        user_id: user.id,
        title: "Test Post",
        content: "This is a test post",
        created_at: Time.now
      )

      logger = create_sql_logger("INSERT INTO posts")
      @db.loggers << logger

      post.save

      # Verify INSERT SQL includes foreign key
      insert_sql = logger.captured_sql
      assert insert_sql, "INSERT SQL should have been generated"
      assert insert_sql.include?("INSERT INTO posts"), "Should insert into posts table"
      assert insert_sql.include?("user_id"), "Should include user_id foreign key"
      assert insert_sql.include?("title"), "Should include title column"

      # Verify the relationship works
      assert post.id, "Post should have an ID after saving"
      retrieved_post = post_class[post.id]
      assert_equal user.id, retrieved_post.user_id
      assert_equal "Test Post", retrieved_post.title
    end
  end

  describe "UPDATE statement generation for model updates" do
    it "should generate proper UPDATE statements when updating model instances" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
      end

      # Create initial user
      user = user_class.create(
        id: next_user_id,
        name: "John Doe",
        email: "john@example.com",
        age: 30,
        active: true
      )

      # Update the user
      user.name = "John Smith"
      user.email = "johnsmith@example.com"
      user.age = 31

      logger = create_sql_logger("UPDATE")
      @db.loggers << logger

      user.save

      # Verify UPDATE SQL was generated correctly
      update_sql = logger.captured_sql
      assert update_sql, "UPDATE SQL should have been generated"
      assert update_sql.include?("UPDATE users"), "Should update users table"
      assert update_sql.include?("name"), "Should update name column"
      assert update_sql.include?("email"), "Should update email column"
      assert update_sql.include?("age"), "Should update age column"
      assert update_sql.include?("WHERE"), "Should include WHERE clause"
      assert update_sql.include?("id"), "Should include ID in WHERE clause"

      # Verify the record was actually updated
      retrieved_user = user_class[user.id]
      assert_equal "John Smith", retrieved_user.name
      assert_equal "johnsmith@example.com", retrieved_user.email
      assert_equal 31, retrieved_user.age
      assert_equal true, retrieved_user.active, "Unchanged field should remain the same"
    end

    it "should handle partial updates efficiently" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
      end

      # Create initial user
      user = user_class.create(
        id: next_user_id,
        name: "Jane Doe",
        email: "jane@example.com",
        age: 25,
        active: true
      )

      # Update only one field
      user.age = 26

      logger = create_sql_logger("UPDATE")
      @db.loggers << logger

      user.save

      # Verify UPDATE SQL only includes changed fields
      update_sql = logger.captured_sql
      assert update_sql, "UPDATE SQL should have been generated"
      assert update_sql.include?("UPDATE users"), "Should update users table"
      assert update_sql.include?("age"), "Should update age column"
      refute update_sql.include?("name ="), "Should not update unchanged name"
      refute update_sql.include?("email ="), "Should not update unchanged email"
      refute update_sql.include?("active ="), "Should not update unchanged active"

      # Verify the record was updated correctly
      retrieved_user = user_class[user.id]
      assert_equal "Jane Doe", retrieved_user.name, "Name should be unchanged"
      assert_equal "jane@example.com", retrieved_user.email, "Email should be unchanged"
      assert_equal 26, retrieved_user.age, "Age should be updated"
      assert_equal true, retrieved_user.active, "Active should be unchanged"
    end

    it "should handle UPDATE with foreign key relationships" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
        one_to_many :posts
      end

      post_class = Class.new(Sequel::Model(@db[:posts])) do
        def self.name
          "Post"
        end
        unrestrict_primary_key
        many_to_one :user
      end

      # Create users and post
      user1 = user_class.create(id: next_user_id, name: "Author 1", email: "author1@example.com")
      user2 = user_class.create(id: next_user_id, name: "Author 2", email: "author2@example.com")
      post = post_class.create(
        id: next_post_id,
        user_id: user1.id,
        title: "Test Post",
        content: "Original content"
      )

      # Update post to different user and change content
      post.user_id = user2.id
      post.content = "Updated content"

      logger = create_sql_logger("UPDATE posts")
      @db.loggers << logger

      post.save

      # Verify UPDATE SQL includes foreign key update
      update_sql = logger.captured_sql
      assert update_sql, "UPDATE SQL should have been generated"
      assert update_sql.include?("UPDATE posts"), "Should update posts table"
      assert update_sql.include?("user_id"), "Should update user_id foreign key"
      assert update_sql.include?("content"), "Should update content column"

      # Verify the relationship was updated
      retrieved_post = post_class[post.id]
      assert_equal user2.id, retrieved_post.user_id
      assert_equal "Updated content", retrieved_post.content
      assert_equal "Test Post", retrieved_post.title, "Title should be unchanged"
    end
  end

  describe "DELETE statement generation for model deletion" do
    it "should generate proper DELETE statements when deleting model instances" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
      end

      # Create user to delete
      user = user_class.create(
        id: next_user_id,
        name: "John Doe",
        email: "john@example.com",
        age: 30
      )
      user_id = user.id

      logger = create_sql_logger("DELETE")
      @db.loggers << logger

      # Delete the user
      user.delete

      # Verify DELETE SQL was generated correctly
      delete_sql = logger.captured_sql
      assert delete_sql, "DELETE SQL should have been generated"
      assert delete_sql.include?("DELETE FROM users"), "Should delete from users table"
      assert delete_sql.include?("WHERE"), "Should include WHERE clause"
      assert delete_sql.include?("id"), "Should include ID in WHERE clause"

      # Verify the record was actually deleted
      deleted_user = user_class[user_id]
      assert_nil deleted_user, "User should be deleted from database"
    end

    it "should handle DELETE with destroy method" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
      end

      # Create user to destroy
      user = user_class.create(
        id: next_user_id,
        name: "Jane Doe",
        email: "jane@example.com",
        age: 25
      )
      user_id = user.id

      logger = create_sql_logger("DELETE")
      @db.loggers << logger

      # Destroy the user (should also generate DELETE)
      user.destroy

      # Verify DELETE SQL was generated correctly
      delete_sql = logger.captured_sql
      assert delete_sql, "DELETE SQL should have been generated"
      assert delete_sql.include?("DELETE FROM users"), "Should delete from users table"
      assert delete_sql.include?("WHERE"), "Should include WHERE clause"
      assert delete_sql.include?("id"), "Should include ID in WHERE clause"

      # Verify the record was actually deleted
      destroyed_user = user_class[user_id]
      assert_nil destroyed_user, "User should be destroyed from database"
    end

    it "should handle bulk DELETE operations" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
      end

      # Create multiple users
      user1 = user_class.create(id: next_user_id, name: "User 1", email: "user1@example.com", active: true)
      user2 = user_class.create(id: next_user_id, name: "User 2", email: "user2@example.com", active: false)
      user3 = user_class.create(id: next_user_id, name: "User 3", email: "user3@example.com", active: false)

      logger = create_sql_logger("DELETE")
      @db.loggers << logger

      # Delete all inactive users
      deleted_count = user_class.where(active: false).delete

      # Verify DELETE SQL was generated correctly
      delete_sql = logger.captured_sql
      assert delete_sql, "DELETE SQL should have been generated"
      assert delete_sql.include?("DELETE FROM users"), "Should delete from users table"
      assert delete_sql.include?("WHERE"), "Should include WHERE clause"
      assert delete_sql.include?("active"), "Should include active condition"

      # Verify correct number of records were deleted
      assert_equal 2, deleted_count, "Should delete 2 inactive users"

      # Verify only active user remains
      remaining_users = user_class.all
      assert_equal 1, remaining_users.length, "Should have 1 user remaining"
      assert_equal user1.id, remaining_users.first.id, "Active user should remain"
    end

    it "should handle DELETE with foreign key constraints" do
      user_class = Class.new(Sequel::Model(@db[:users])) do
        def self.name
          "User"
        end
        unrestrict_primary_key
        one_to_many :posts
      end

      post_class = Class.new(Sequel::Model(@db[:posts])) do
        def self.name
          "Post"
        end
        unrestrict_primary_key
        many_to_one :user
      end

      # Create user and post
      user = user_class.create(id: next_user_id, name: "Author", email: "author@example.com")
      post = post_class.create(
        id: next_post_id,
        user_id: user.id,
        title: "Test Post",
        content: "Test content"
      )

      # Delete the post first (to avoid foreign key constraint issues)
      logger = create_sql_logger("DELETE FROM posts")
      @db.loggers << logger

      post.delete

      # Verify DELETE SQL was generated correctly
      delete_sql = logger.captured_sql
      assert delete_sql, "DELETE SQL should have been generated"
      assert delete_sql.include?("DELETE FROM posts"), "Should delete from posts table"
      assert delete_sql.include?("WHERE"), "Should include WHERE clause"
      assert delete_sql.include?("id"), "Should include ID in WHERE clause"

      # Verify the post was deleted
      deleted_post = post_class[post.id]
      assert_nil deleted_post, "Post should be deleted from database"

      # Now delete the user
      user_logger = create_sql_logger("DELETE FROM users")
      @db.loggers << user_logger

      user.delete

      # Verify user DELETE SQL was generated correctly
      user_delete_sql = user_logger.captured_sql
      assert user_delete_sql, "User DELETE SQL should have been generated"
      assert user_delete_sql.include?("DELETE FROM users"), "Should delete from users table"

      # Verify the user was deleted
      deleted_user = user_class[user.id]
      assert_nil deleted_user, "User should be deleted from database"
    end
  end
end