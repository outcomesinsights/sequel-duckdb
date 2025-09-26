# frozen_string_literal: true

require "bundler/gem_tasks"
require "rubocop/rake_task"
require "rake/testtask"

RuboCop::RakeTask.new

Rake::TestTask.new do |t|
  t.libs << "test"
  # Exclude performance tests by default
  t.test_files = FileList["test/**/*_test.rb"].exclude("test/performance*_test.rb")
end

# Create a separate task for performance tests
Rake::TestTask.new(:test_performance) do |t|
  t.libs << "test"
  t.test_files = FileList["test/performance*_test.rb"]
end

# Task to run all tests including performance
Rake::TestTask.new(:test_all) do |t|
  t.libs << "test"
  t.test_files = FileList["test/**/*_test.rb"]
end

task default: %i[test rubocop]
