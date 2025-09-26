#!/usr/bin/env ruby
# frozen_string_literal: true

# Test runner for sequel-duckdb adapter
# This file loads all test files and runs the complete test suite
# Following sequel-hexspace pattern for test organization

require_relative "spec_helper"

# Load all test files
test_files = Dir[File.join(__dir__, "*_test.rb")]

# Exclude performance tests by default unless PERFORMANCE_TESTS environment variable is set
test_files = test_files.reject { |file| file.include?("performance") } unless ENV["PERFORMANCE_TESTS"]

test_files.each do |file|
  require file
end

puts "Running sequel-duckdb test suite..."
puts "Loaded #{test_files.length} test files"
if ENV["PERFORMANCE_TESTS"]
  puts "Performance tests included (PERFORMANCE_TESTS=1)"
else
  puts "Performance tests excluded (set PERFORMANCE_TESTS=1 to include)"
end
