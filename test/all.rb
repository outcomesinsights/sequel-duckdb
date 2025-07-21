#!/usr/bin/env ruby
# frozen_string_literal: true

# Test runner for sequel-duckdb adapter
# This file loads all test files and runs the complete test suite
# Following sequel-hexspace pattern for test organization

require_relative "spec_helper"

# Load all test files
test_files = Dir[File.join(__dir__, "*_test.rb")].sort

test_files.each do |file|
  require file
end

puts "Running sequel-duckdb test suite..."
puts "Loaded #{test_files.length} test files"