# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

begin
  require "rubocop/rake_task"

  RuboCop::RakeTask.new(:lint) do |task|
    task.options = ["--display-cop-names"]
  end

  RuboCop::RakeTask.new(:format) do |task|
    task.options = ["--auto-correct-all"]
  end

  desc "Run RuboCop with safe autocorrect"
  task :lint_fix do
    system("bundle exec rubocop --autocorrect")
  end

  # Keep 'rubocop' task for backwards compat with default task
  RuboCop::RakeTask.new(:rubocop)
rescue LoadError
  # RuboCop not available
end

Rake::TestTask.new do |t|
  t.libs << "test"
  t.test_files = FileList["test/**/*_test.rb"].exclude("test/performance*_test.rb")
end

Rake::TestTask.new(:test_performance) do |t|
  t.libs << "test"
  t.test_files = FileList["test/performance*_test.rb"]
end

Rake::TestTask.new(:test_all) do |t|
  t.libs << "test"
  t.test_files = FileList["test/**/*_test.rb"]
end

task default: %i[test rubocop]
