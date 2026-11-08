# frozen_string_literal: true

# Minitest calls record only once the test's teardown has ended.
require 'minitest'

module RubySuite
  class Results < Minitest::AbstractReporter
    def prerecord(klass, name)
      puts "Starting #{klass}##{name}"
      $stdout.flush
    end

    def record(result)
      outcome = if result.skipped?
                  'skipped'
                elsif result.passed?
                  'passed'
                else
                  'failed'
                end
      # Keep diagnostics beside results so a scenario timeout retains completed failures too.
      result.failures.each { |failure| puts "#{result.klass}##{result.name}: #{failure.message}\n#{failure.backtrace&.join("\n")}" }
      puts "NERVIX-SUITE #{outcome} #{result.klass}##{result.name}"
      $stdout.flush
    end
  end
end

Minitest.extensions << 'nervix_results'
def Minitest.plugin_nervix_results_init(_options)
  # ProgressReporter's un-terminated dots must not prefix the result/action protocol's lines.
  reporter.reporters.reject! { |part| part.is_a?(Minitest::ProgressReporter) }
  reporter << RubySuite::Results.new
end
